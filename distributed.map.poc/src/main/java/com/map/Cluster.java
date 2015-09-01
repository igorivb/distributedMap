package com.map;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Cluster {

	private static final Logger logger = LoggerFactory.getLogger(Cluster.class);

	//table of partitions
	private PartitionTable pt;		
	
	/*
	 * Node comparators. Default sorting: desc
	 */
	static class NodesComparator implements Comparator<NodeEntry> {
		NodeSection nodeSection;
		public NodesComparator(NodeSection nodeSection) {
			this.nodeSection = nodeSection;
		}
		@Override
		public int compare(NodeEntry o1, NodeEntry o2) {
			int n1 = o1.getPartitionsCount(nodeSection);
			int n2 = o2.getPartitionsCount(nodeSection);
			return n1 == n2 ? 0 : (n1 < n2 ? 1 : -1);
		}		
	}	
	static class ReversedNodesComparator implements Comparator<NodeEntry> {
		Comparator<NodeEntry> cmp;
		public ReversedNodesComparator(Comparator<NodeEntry> cmp) {
			this.cmp = cmp;
		}
		@Override
		public int compare(NodeEntry o1, NodeEntry o2) {
			return cmp.compare(o2, o1);
		}		
	}
	//desc
	private Comparator<NodeEntry> primaryNodesDescCmp = new NodesComparator(NodeSection.PRIMARY);
	private Comparator<NodeEntry> secondaryNodesDescCmp = new NodesComparator(NodeSection.SECONDARY);
	//asc
	private Comparator<NodeEntry> primaryNodesAscCmp = new ReversedNodesComparator(primaryNodesDescCmp);
	private Comparator<NodeEntry> secondaryNodesAscCmp = new ReversedNodesComparator(secondaryNodesDescCmp);
	
	
	public Cluster(int replicationFactor, int partitionsCount) {
		pt = new PartitionTable(replicationFactor, partitionsCount);
		
		logger.info("Creating cluster, replicationFactor: {}, partitionsCount: {}", replicationFactor, partitionsCount);
	}
	
	/**
	 * TODO: 
	 * 1. allows to add 1 node at a time. 
	 * If needed improve later to allow process several nodes at a time.
	 * 
	 * 2. make changes, if 'remove' occurred during 'add' operation
	 * e.g. some partitions may be available only in secondary or lost at all. 
	 * Is seems it should be handled by 'remove' and 'add' operation should be stopped.		
	 */
	public void addNode(NodeEntry newNode) {
		logger.info("Adding node: {}", newNode);
		
		boolean isClusterFull = pt.getNodesSize() == pt.getPartitionsSize();		
		boolean isClusterEmpty = pt.getNodesSize() == 0;
		boolean isBalanced = pt.getNodesSize() >= pt.getReplicationFactor() + 1; 
		
		if (!isClusterFull) { //TODO: remove condition when we can expand cluster if it is full
			
			Node node;
			pt.getNodes().put(newNode.getId(), node = new Node(newNode.getId(), newNode.getAddress())); //create node
			node.setPartitionTable(pt);		
			
			pt.addNodeEntry(newNode); //update PartitionTable						
		}
		
		boolean added = true;
		if (isClusterEmpty) { //there are no nodes: cluster is empty
			initCluster(newNode);			
		} else if (isClusterFull) { //cluster is full
			added = clusterFull(newNode);
		} else {			
			logger.info("Is balanced: {}", isBalanced);
			
			List<PartitionTableEntry> primaryPartitions = processAddPrimary(newNode, isBalanced);
			
			if (pt.hasReplica()) {
				processAddSecondary(newNode, isBalanced, primaryPartitions);
			}
		}	
				
		if (added) {			
			logger.info("Added new node to nodes list: {}", newNode.getId());
		}	
		
		checkClusterPartitions(true);
	}
	
	/**
	 * Allows to delete multiple nodes at a time.
	 * 
	 * TODO: issues:    
	 *   1. handle if we have previous 'remove' in progress: stop it and start this one
	 *   2. implement 'balanceRemoveNodes': 
	 *   e.g. partitions may be not balanced if 'remove' occurred in the middle of 'add' 
	 *   where only part of partitions were re-distributed. 
	 */
	public void removeNodes(List<NodeEntry> deletedNodes) {
		logger.info("Removing nodes: {}", deletedNodes);
		
		//check if we have nodes to delete
		for (NodeEntry deletedNode : deletedNodes) {
			NodeEntry n = pt.getNodeEntry(deletedNode.getId());
			if (n == null) {
				throw new RuntimeException(String.format("Failed to delete nodes, node: %s doesn't exist", deletedNode));
			}
		}
				
		boolean deleteAllNodes = deletedNodes.size() == pt.getNodesSize();
		
		if (!deleteAllNodes) {
			for (NodeEntry node : deletedNodes) { //mark nodes as deleted
				pt.markNodeAsDeleted(node);
			}				
					
			processRemovePrimary(deletedNodes);
			
			if (pt.hasReplica()) {
				processRemoveSecondary(deletedNodes);
			}
			
			balanceRemoveNodes(deletedNodes);
					
			for (NodeEntry deleted : deletedNodes) { //delete nodes
				pt.getNodes().remove(deleted);
			}
			
			pt.deleteNodes(deletedNodes);
						
			checkClusterPartitions(true);
		} else {
			logger.info("Deleting all nodes");			
			
			pt.deleteAll();
		}						
	}
	
	/*
	 * Restore from primary if needed.
	 * If needed, remove extra secondary nodes.
	 */
	private void processRemoveSecondary(List<NodeEntry> deletedNodes) {
		//potential target nodes (sort them before usage), exclude deleted nodes 
		List<NodeEntry> targetNodes = getNodes(deletedNodes);
		
		int currentRF = Math.min(pt.getReplicationFactor(), targetNodes.size() - 1);
		
		logger.debug("Processing remove secondary, current replication factor: {}", currentRF);
		
		//delete extra nodes if needed
		if (currentRF < pt.getReplicationFactor()) {						
			for (PartitionTableEntry entry : pt.getPartitionEntries().values()) {				
				int partId = entry.getPartitionId();
				
				int diff = excludeDeletedNodes(entry.getSecondaryNodes()).size() - currentRF;
				if (diff > 0) {
					logger.debug("Deleting extra nodes for partition: {}, num: {}", partId, diff);
					
					//delete from max secondary.len
					Collections.sort(targetNodes, secondaryNodesDescCmp);
					
					for (int i = 0; i < diff; i ++) {						
						NodeEntry targetNode = targetNodes.get(i);
						logger.debug("Delete secondary partition from node, node: {}, partition: {}", targetNode, partId);						
						deleteSecondaryPartition(targetNode, partId);																		
					}
				}
			}
		}
		
		//restore secondary if needed
		for (NodeEntry deletedNode : deletedNodes) { //iterate nodes			
			List<PartitionTableEntry> deletedSecParts = deletedNode.getSecondaryPartitions();
			logger.debug("Restoring secondary partitions deleted on node: {}, partitions: {}", deletedNode.getId(), deletedSecParts);
			
			for (PartitionTableEntry deletedSecPart : deletedSecParts) { //iterate partitions								
				int diff = currentRF - excludeDeletedNodes(deletedSecPart.getSecondaryNodes()).size();				
				
				logger.debug("Handle deleted secondary partition: {}, diff: {}", deletedSecPart, diff);
				
				if (diff == 0) { //need to restore ?
					logger.debug("No need to restore secondary partition: {}", deletedSecPart);
				} else {
					NodeEntry primaryNode = pt.getPrimaryNodeForPartition(deletedSecPart.getPartitionId());
					if (primaryNode != null) { //restore						
						//find target node
						Collections.sort(targetNodes, secondaryNodesAscCmp);							
						NodeEntry targetNode = findTargetNode(targetNodes, deletedSecPart, NodeSection.SECONDARY);							
																											
						/*
						 * It may copy empty partition if there was data loss.
						 * But it is ok, because there may be some delay between handling primary and secondary partitions;
						 * and during that delay some data may be added to primary partition by clients, 
						 * so partition will not be empty. 
						 */
						copyPartition(primaryNode, deletedSecPart, NodeSection.PRIMARY, targetNode, NodeSection.SECONDARY);											
					} else {
						/*
						 * Data loss.
						 * Should not happen because primary should be restored on previous step. Delete this part ? 
						 */						
//						//create new secondary partition and register it in PartitionTable				
//						for (int i = 0; i < diff; i ++) {
//							Node targetNode = targetNodes.get(i);
//							logger.warn("Data are lost for secondary partition, create a new one, partition id: {}, target node id: {}", deletedSecPart, targetNode);						
//							targetNode.createPartition(NodeSection.SECONDARY, deletedSecPart);												
//						}
						throw new RuntimeException("Should not happen because primary should be restored on previous step.");
					}
				}
				
				//remove previous entry
				deletedSecPart.removeSecondaryNode(deletedNode.getId());
			}		
		}			
	}		
	
	//Try to avoid collisions between primary and secondary.
	private NodeEntry findTargetNode(List<NodeEntry> targetNodes, PartitionTableEntry partitionId, NodeSection section) {
		NodeEntry withCollision = null;
		
		for (NodeEntry targetNode : targetNodes) {
			if (!targetNode.hasPartition(partitionId.getPartitionId(), section)) { //check if contains in section
				if (!targetNode.hasPartition(partitionId.getPartitionId(), NodeSection.reverse(section))) { //check if there is collision
					return targetNode;
				} else if (withCollision == null) {
					withCollision = targetNode;
				}				
			}
		}		
		
		if (withCollision != null) {
			return withCollision;
		}
		throw new RuntimeException(String.format("Failed to find target node. Partition: %s, section: %s", partitionId, section));		
	}

	/*
	 * Restore deleted primary partitions from secondary nodes, if there are any, 
	 * otherwise create empty partitions. 
	 */
	private void processRemovePrimary(List<NodeEntry> deletedNodes) {						
		//sort nodes by primaryData.len asc		
		List<NodeEntry> targetNodes = getNodes(deletedNodes);
		
		List<PartitionTableEntry> deletedPrimaryParts = this.getDeletedPrimaryPartitions(deletedNodes);
		
		logger.info("Processing remove primary, deleted partitions: {}", deletedPrimaryParts);
		
		for (PartitionTableEntry deletedPrimaryPart : deletedPrimaryParts) {			
			logger.debug("Handle deleted primary partition: {}", deletedPrimaryPart);
			
			Collections.sort(targetNodes, primaryNodesAscCmp);
			
			//find nodes which contain deleted partition
			List<NodeEntry> secNodes = excludeDeletedNodes(deletedPrimaryPart.getSecondaryNodes());
			
			logger.debug("Secondary nodes which contain deleted partition: {}, nodes: {}", deletedPrimaryPart, secNodes);
			if (!secNodes.isEmpty()) {
				NodeEntry replicaNode = secNodes.get(0); //take first one
				
				//get node with minimum primary
				NodeEntry targetNode = findTargetNode(targetNodes, deletedPrimaryPart, NodeSection.PRIMARY);
				
				copyPartition(replicaNode, deletedPrimaryPart, NodeSection.SECONDARY, targetNode, NodeSection.PRIMARY);				
			} else { //data loss
				//create new partition and register it in PartitionTable
				
				NodeEntry targetNode = targetNodes.get(0);
				
				logger.warn("Data are lost for partition, create a new one, partition id: {}, node: {}", deletedPrimaryPart, targetNode);						
				
				createPartition(targetNode, NodeSection.PRIMARY, deletedPrimaryPart.getPartitionId());
			}
		}
	}		
	
	private List<NodeEntry> excludeDeletedNodes(List<NodeEntry> nodes) {
		List<NodeEntry> res = new ArrayList<>();
		for (NodeEntry node : nodes) {
			if (!node.isDeleted()) {
				res.add(node);
			}
		}
		return res;
	}
	
	private List<PartitionTableEntry> getDeletedPrimaryPartitions(List<NodeEntry> deletedNodes) {
		List<Integer> nodes = new ArrayList<>();
		for (NodeEntry deletedNode : deletedNodes) {
			nodes.add(deletedNode.getId());
		}
		return pt.getNodesPrimaryPartitions(nodes);
	}

	private void balanceRemoveNodes(List<NodeEntry> nodesToDelete) {
		//TODO: implement		
	}
	
	private void processAddSecondary(NodeEntry newNode, boolean isBalanced, List<PartitionTableEntry> primaryPartitions) {				
		if (isBalanced) {									
			int num = (pt.getPartitionsSize() * pt.getReplicationFactor()) / pt.getNodesSize(); //ignore remaining part of division
			logger.debug("Processing add secondary: balanced, num: {}", num);
			
			Set<PartitionTableEntry> addedParts = new HashSet<>(primaryPartitions); 
			
			//sort nodes by secondaryData.len desc
			List<NodeEntry> sortedNodes = getNodes(Arrays.asList(newNode));			
									
			for (int i = 0; i < num;) {	
				if (sortedNodes.isEmpty()) {
					logger.debug("Didn't all needed nodes to secondary, left: {}", num);
					break;
				}
				Collections.sort(sortedNodes, secondaryNodesDescCmp);
				
				NodeEntry node = sortedNodes.get(0);
				
				//find first partition which is not already present in primary and secondary
				PartitionTableEntry part = null;
				for (PartitionTableEntry p : node.getSecondaryPartitions()) {
					if (!addedParts.contains(p)) {
						part = p;
						addedParts.add(part);
						break;
					} else {
						logger.debug("Don't add secondary partition: '{}' in node {}, because it already exists", p, node);
					}
				}
				if (part == null) { //don't process this node any more
					sortedNodes.remove(node);
					continue;
				}
												
				movePartition(node, part, NodeSection.SECONDARY, newNode, NodeSection.SECONDARY);
										
				i ++;
			}		
		} else {
			logger.debug("Processing secondary: not balanced");
			
			//copy all primary to new node's secondary
			for (NodeEntry node : getNodes(Arrays.asList(newNode))) {
				for (PartitionTableEntry part : node.getPrimaryPartitions()) {
					copyPartition(node, part, NodeSection.PRIMARY, newNode, NodeSection.SECONDARY);
				}
			}
		}				
	}	
	
	private List<PartitionTableEntry> processAddPrimary(NodeEntry newNode, boolean isBalanced) {								
		int num = pt.getPartitionsSize() / pt.getNodesSize(); //ignore remaining part of division
		logger.debug("Processing add primary, num: {}", num);
		
		List<PartitionTableEntry> primaryPartitions = new ArrayList<>();
		
		//sort nodes by primaryData.len desc		
		List<NodeEntry> sortedNodes = getNodes(Arrays.asList(newNode));
		
		for (int i = 0; i < num; i ++) {
			Collections.sort(sortedNodes, primaryNodesDescCmp);
			
			NodeEntry node = sortedNodes.get(0);
			PartitionTableEntry p1 = node.getPrimaryPartitions().get(0);
			
			if (isBalanced) {
				movePartition(node, p1, NodeSection.PRIMARY, newNode, NodeSection.PRIMARY);
			} else {
				copyPartition(node, p1, NodeSection.PRIMARY, newNode, NodeSection.PRIMARY);
				
				//TODO: don't need extra 'copy' here: move right away
				movePartition(node, p1, NodeSection.PRIMARY, node, NodeSection.SECONDARY);	//move partition to the same node's secondary section
			}												
			
			primaryPartitions.add(p1);
		}
		return primaryPartitions;
	}	
	
	private List<NodeEntry> getNodes(List<NodeEntry> exclude) {
		List<NodeEntry> nodes = new ArrayList<>();
		for (NodeEntry n : pt.getNodeEntries()) {
			if (!exclude.contains(n)) {
				nodes.add(n);
			}
		}
		return nodes;
	}

	private void copyPartition(
		NodeEntry src, PartitionTableEntry srcPartId, NodeSection srcSection,
		NodeEntry dest, NodeSection destSection) /*throws IOException*/ {
		
		transferPartition(false, src, srcPartId, srcSection, dest, destSection);		
	}
	
	private void movePartition(
		NodeEntry src, PartitionTableEntry srcPartId, NodeSection srcSection, 
		NodeEntry dest, NodeSection destSection) /*throws IOException*/ {
		
		transferPartition(true, src, srcPartId, srcSection, dest, destSection);		
	}
	
	/*
	 * TODO: There are possible local copies and removes - handle appropriately.
	 */
	private void transferPartition(boolean isMove, 
		NodeEntry srcEntry, PartitionTableEntry srcPartEntry, NodeSection srcSection, 
		NodeEntry destEntry, NodeSection destSection) /*throws IOException*/ {
		
		logger.debug((!isMove ? "Copying" : "Moving ") + 
			" partition '{}' from '{}'.{} to '{}'.{}", 
			srcPartEntry, srcEntry.getId(), srcSection, destEntry.getId(), destSection);			
		
		Node src = pt.getNodes().get(srcEntry.getId());
		Node dest = pt.getNodes().get(destEntry.getId());
		
		Partition srcPart = null;
		for (Partition part : src.getData(srcSection)) {
			if (part.getId() == srcPartEntry.getPartitionId()) {
				srcPart = part;
				break;
			}
		}
		if (srcPart == null) {
			throw new RuntimeException(
				String.format("Failed to find partition in node. Node: %s, partition: %s", srcEntry, srcPart));
		}
		
		//make actual copy
		Partition copy = srcPart.copy();
		dest.addPartition(destSection, copy);
		
		if (isMove) { //delete from src			
			//logger.debug("Deleting partition '{}' from {} node, isPrimary: {}", srcPart.getId(), src.getId(), isSrcPrimary);			
			src.removePartition(srcSection, srcPart);			
		}	
		
		//update partition table
		if (isMove) {			
			if (srcSection == NodeSection.PRIMARY) {		
				srcPartEntry.removePrimaryNode(srcEntry.getId());
			} else {
				srcPartEntry.removeSecondaryNode(srcEntry.getId());
			}
		}				
		
		if (destSection == NodeSection.PRIMARY) {
			srcPartEntry.setPrimaryNode(destEntry.getId());
		} else {
			srcPartEntry.addSecondaryNode(destEntry.getId());
		}		
	}
	
	public void createPartition(NodeEntry nodeEntry, NodeSection section, int partitionId) {
		Node node = pt.getNodes().get(nodeEntry.getId());		
		node.createPartition(section, partitionId);		
	}
	
	public void deleteSecondaryPartition(NodeEntry nodeEntry, int partitionId) {
		Node node = pt.getNodes().get(nodeEntry.getId());
		node.deleteSecondaryPartition(partitionId);		
	}
	

	/*
	 * TODO: Possible strategies:
	 * 
	 * 1. Do nothing
	 * 
	 * OR
	 * 
	 * 2. We can move secondary parts to new node
	 * 
	 * OR 
	 * 
	 * 3. Increase partitionsCount, e.g. double, and re-distribute nodes
	 * Probably it is best option, but do it later.
	 */
	private boolean clusterFull(NodeEntry newNode) {
		logger.warn("Cluster is full, do nothing. partitionsCount: {}, nodesCount: {}", pt.getPartitionsSize(), pt.getNodesSize());
		return false;
	}

	private void initCluster(NodeEntry newNode) {
		logger.info("Cluster is empty, init first node");								
		
		for (int i = 0; i < pt.getPartitionsSize(); i ++) { //create primary partitions							
			createPartition(newNode, NodeSection.PRIMARY, i);			
		}					
	}
	
	public PartitionTable getPartitionTable() {
		return pt;
	}
	
	public void checkClusterPartitions(boolean checkCollisions) {
		Set<PartitionTableEntry> primaryTotal = new HashSet<>();
		int secondaryTotal = 0;
		
		List<String> errors = new ArrayList<>();
				
		for (NodeEntry n : pt.getNodeEntries()) {
			primaryTotal.addAll(n.getPrimaryPartitions());
			secondaryTotal += n.getSecondaryPartitions().size();			
			
			//check collisions
			if (checkCollisions) {
				for (PartitionTableEntry p : n.getSecondaryPartitions()) {
					if (n.getPrimaryPartitions().contains(p)) {
						errors.add(String.format("Secondary and primary collide for node: %s", n.getId()));
					}
				}		
			}
			
			//duplicates
			if (n.getPrimaryPartitionsCount() != new HashSet<>(n.getPrimaryPartitions()).size()) {
				errors.add("There are duplicates in primary: " + n.getId());
			}
			if (n.getSecondaryPartitionsCount() != new HashSet<>(n.getSecondaryPartitions()).size()) {
				errors.add("There are duplicates in secondary: " + n.getId());
			}
		}		
				
		if (primaryTotal.size() != pt.getPartitionsSize()) {
			errors.add(String.format("Primary total is not correct. Expected: %d, was: %s", pt.getPartitionsSize(), primaryTotal));
		}
		
		if (pt.hasReplica()) {
			int secExpected = (pt.getNodesSize() > pt.getReplicationFactor() ? pt.getReplicationFactor() : pt.getNodesSize() - 1) * pt.getPartitionsSize();
			if (secondaryTotal != secExpected) {
				errors.add(String.format("Secondary total is not correct. Expected: %d, was: %d", secExpected, secondaryTotal));
			}							
		} else if (secondaryTotal > 0) {
			errors.add("Secondary total is not empty: " + secondaryTotal);
		}				
		
		if (!errors.isEmpty()) {
			throw new RuntimeException("Found errors: " + errors);
		}
	}
	
	public boolean isEmpty() {
		return this.pt.getNodesSize() == 0;
	}
}
