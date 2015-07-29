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

import com.map.Node.NodeSection;

public class Cluster {

	private static final Logger logger = LoggerFactory.getLogger(Cluster.class);

	//table of partitions
	private PartitionTable pt;
	
	
	/*
	 * Node comparators. Default sorting: desc
	 */
	static class NodesComparator implements Comparator<Node> {
		NodeSection nodeSection;
		public NodesComparator(NodeSection nodeSection) {
			this.nodeSection = nodeSection;
		}
		@Override
		public int compare(Node o1, Node o2) {
			int n1 = o1.getData(nodeSection).size();
			int n2 = o2.getData(nodeSection).size();
			return n1 == n2 ? 0 : (n1 < n2 ? 1 : -1);
		}		
	}	
	static class ReversedNodesComparator implements Comparator<Node> {
		Comparator<Node> cmp;
		public ReversedNodesComparator(Comparator<Node> cmp) {
			this.cmp = cmp;
		}
		@Override
		public int compare(Node o1, Node o2) {
			return cmp.compare(o2, o1);
		}		
	}
	//desc
	Comparator<Node> primaryNodesDescCmp = new NodesComparator(NodeSection.PRIMARY);
	Comparator<Node> secondaryNodesDescCmp = new NodesComparator(NodeSection.SECONDARY);
	//asc
	Comparator<Node> primaryNodesAscCmp = new ReversedNodesComparator(primaryNodesDescCmp);
	Comparator<Node> secondaryNodesAscCmp = new ReversedNodesComparator(secondaryNodesDescCmp);
	
	
	public Cluster(int replicationFactor, int partitionsCount) {
		pt = new PartitionTable(replicationFactor, partitionsCount);
		
		logger.info("Creating cluster, replicationFactor: {}, partitionsCount: {}", replicationFactor, partitionsCount);
	}
	
	/*
	 * TODO: 
	 * 1. allows to add 1 node at a time. 
	 * If needed improve later to allow process several nodes at a time.
	 * 
	 * 2. make changes, if 'remove' occurred during 'add' operation
	 * e.g. some partitions may be available only in secondary or lost at all. 
	 * Is seems it should be handled by 'remove' and 'add' operation should be stopped.		
	 */
	public void addNode(Node newNode) {
		logger.info("Adding node: {}", newNode.getId());
		
		boolean isClusterFull = pt.getNodesSize() == pt.getPartitionsSize();		
		boolean isClusterEmpty = pt.getNodesSize() == 0;
		boolean isBalanced = pt.getNodesSize() >= pt.getReplicationFactor() + 1; 
		
		if (!isClusterFull) { //TODO: remove condition when we can expand cluster if it is full
			pt.addNode(newNode);
		}
		
		boolean added = true;
		if (isClusterEmpty) { //there are no nodes: cluster is empty
			initCluster(newNode);			
		} else if (isClusterFull) { //cluster is full
			added = clusterFull(newNode);
		} else {			
			logger.info("Is balanced: {}", isBalanced);
			
			List<Partition> primaryPartitions = processAddPrimary(newNode, isBalanced);
			
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
	 *   0. processRemovePrimary and processRemoveSecondary create collisions between primary and secondary. Try to avoid ? 
	 *   	There are possible local copies here.
	 *   	Think about it when implementing 'balanceRemoveNodes'.
	 *   
	 *   1. junit test failures 
	 */
	public void removeNodes(List<Node> deletedNodes) {
		logger.info("Removing nodes: {}", deletedNodes);
				
		for (Node node : deletedNodes) { //mark nodes as deleted
			pt.markNodeAsDeleted(node);
		}
		
		//TODO: handle if all nodes are deleted		
		
		//TODO: handle if we have previous remove in progress: stop it and start this one
		
		processRemovePrimary(deletedNodes);
		
		if (pt.hasReplica()) {
			processRemoveSecondary(deletedNodes);
		}
		
		balanceRemoveNodes(deletedNodes);
				
		pt.deleteNodes(deletedNodes);
		
		//TODO: check also that there are no checkCollisions after implementing 'balanceRemoveNodes'
		checkClusterPartitions(false);
	}
	
	/*
	 * Restore from primary if needed.
	 * If needed, remove extra secondary nodes.
	 */
	private void processRemoveSecondary(List<Node> deletedNodes) {
		//potential target nodes (sort them before usage), exclude deleted nodes 
		List<Node> targetNodes = getNodes(deletedNodes);
		
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
						Node targetNode = targetNodes.get(i);
						logger.debug("Delete secondary partition from node, node: {}, partition: {}", targetNode, partId);						
						targetNode.deleteSecondaryPartition(partId);																		
					}
				}
			}
		}
		
		//restore secondary if needed
		for (Node deletedNode : deletedNodes) { //iterate nodes
			Set<Integer> deletedSecParts = pt.getNodesSecondaryPartitions(Arrays.asList(deletedNode.getId()));
			logger.debug("Restoring secondary partitions deleted on node: {}, partitions: {}", deletedNode.getId(), deletedSecParts);
			
			for (Integer deletedSecPart : deletedSecParts) { //iterate partitions								
				PartitionTableEntry entry = pt.getEntryForPartition(deletedSecPart);
				int diff = currentRF - excludeDeletedNodes(entry.getSecondaryNodes()).size();
				Collections.sort(targetNodes, secondaryNodesAscCmp);
				
				logger.debug("Handle deleted secondary partition: {}, diff: {}", deletedSecPart, diff);
				
				if (diff == 0) { //need to restore ?
					logger.debug("No need to restore secondary partition: {}", deletedSecPart);
				} else {
					Node primaryNode = pt.getPrimaryNodeForPartition(deletedSecPart);
					if (primaryNode != null) { //restore
						for (int i = 0; i < diff; i ++) {
							Node targetNode = targetNodes.get(i);						
							/*
							 * It may copy empty partition if there was data loss.
							 * But it is ok, because there may be some delay between handling primary and secondary partitions;
							 * and during that delay some data may be added to primary partition by clients, 
							 * so partition will not be empty. 
							 */
							copyPartition(primaryNode, deletedSecPart, NodeSection.PRIMARY, targetNode, NodeSection.SECONDARY);
						}					
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
				entry.removeSecondaryNode(deletedNode);
			}		
		}			
	}		

	/*
	 * Restore deleted primary partitions from secondary nodes, if there are any, 
	 * otherwise create empty partitions. 
	 */
	private void processRemovePrimary(List<Node> deletedNodes) {						
		//sort nodes by primaryData.len asc		
		List<Node> targetNodes = getNodes(deletedNodes);
		
		List<Integer> deletedPrimaryParts = this.getDeletedPrimaryPartitions(deletedNodes);
		
		logger.info("Processing remove primary, deleted partitions: {}", deletedPrimaryParts);
		
		for (Integer deletedPrimaryPart : deletedPrimaryParts) {			
			logger.debug("Handle deleted primary partition: {}", deletedPrimaryPart);
			
			Collections.sort(targetNodes, primaryNodesAscCmp);
			
			//find nodes which contain deleted partition
			List<Node> secNodes = excludeDeletedNodes(pt.getSecondaryNodesForPartition(deletedPrimaryPart));
			
			logger.debug("Secondary nodes which contain deleted partition: {}, nodes: {}", deletedPrimaryPart, secNodes);
			if (!secNodes.isEmpty()) {
				Node replicaNode = secNodes.get(0); //take first one
				
				//get node with minimum primary
				Node targetNode = targetNodes.get(0);
				
				copyPartition(replicaNode, deletedPrimaryPart, NodeSection.SECONDARY, targetNode, NodeSection.PRIMARY);				
			} else { //data loss
				//create new partition and register it in PartitionTable
				
				Node targetNode = targetNodes.get(0);
				
				logger.warn("Data are lost for partition, create a new one, partition id: {}, node: {}", deletedPrimaryPart, targetNode);
																
				targetNode.createPartition(NodeSection.PRIMARY, deletedPrimaryPart);
			}
		}
	}
	
	private List<Node> excludeDeletedNodes(List<Node> nodes) {
		List<Node> res = new ArrayList<>();
		for (Node node : nodes) {
			if (!node.isDeleted()) {
				res.add(node);
			}
		}
		return res;
	}
	
	private List<Integer> getDeletedPrimaryPartitions(List<Node> deletedNodes) {
		List<Integer> nodes = new ArrayList<>();
		for (Node deletedNode : deletedNodes) {
			nodes.add(deletedNode.getId());
		}
		return pt.getNodesPrimaryPartitions(nodes);
	}

	private void balanceRemoveNodes(List<Node> nodesToDelete) {
		//TODO: implement		
	}
	
	private void processAddSecondary(Node newNode, boolean isBalanced, List<Partition> primaryPartitions) {				
		if (isBalanced) {									
			int num = (pt.getPartitionsSize() * pt.getReplicationFactor()) / pt.getNodesSize(); //ignore remaining part of division
			logger.debug("Processing add secondary: balanced, num: {}", num);
			
			Set<Partition> addedParts = new HashSet<>(primaryPartitions); 
			
			//sort nodes by secondaryData.len desc
			List<Node> sortedNodes = getNodes(Arrays.asList(newNode));			
									
			for (int i = 0; i < num;) {	
				if (sortedNodes.isEmpty()) {
					logger.debug("Didn't all needed nodes to secondary, left: {}", num);
					break;
				}
				Collections.sort(sortedNodes, secondaryNodesDescCmp);
				
				Node node = sortedNodes.get(0);
				
				//find first partition which is not already present in primary and secondary
				Partition part = null;
				for (Partition p : node.getSecondaryData()) {
					if (!addedParts.contains(p)) {
						part = p;
						addedParts.add(part);
						break;
					} else {
						logger.debug("Don't add secondary partition: '{}' in node {}, because it already exists", p.getId(), node.getId());
					}
				}
				if (part == null) { //don't process this node any more
					sortedNodes.remove(node);
					continue;
				}
												
				movePartition(node, part.getId(), NodeSection.SECONDARY, newNode, NodeSection.SECONDARY);
										
				i ++;
			}		
		} else {
			logger.debug("Processing secondary: not balanced");
			
			//copy all primary to new node's secondary
			for (Node node : getNodes(Arrays.asList(newNode))) {
				for (Partition part : node.getPrimaryData()) {
					copyPartition(node, part.getId(), NodeSection.PRIMARY, newNode, NodeSection.SECONDARY);
				}
			}
		}				
	}	

	private List<Partition> processAddPrimary(Node newNode, boolean isBalanced) {								
		int num = pt.getPartitionsSize() / pt.getNodesSize(); //ignore remaining part of division
		logger.debug("Processing add primary, num: {}", num);
		
		List<Partition> primaryPartitions = new ArrayList<>();
		
		//sort nodes by primaryData.len desc		
		List<Node> sortedNodes = getNodes(Arrays.asList(newNode));
		
		for (int i = 0; i < num; i ++) {
			Collections.sort(sortedNodes, primaryNodesDescCmp);
			
			Node node = sortedNodes.get(0);
			Partition p1 = node.getPrimaryData().get(0);
			
			if (isBalanced) {
				movePartition(node, p1.getId(), NodeSection.PRIMARY, newNode, NodeSection.PRIMARY);
			} else {
				copyPartition(node, p1.getId(), NodeSection.PRIMARY, newNode, NodeSection.PRIMARY);
				
				//TODO: don't need extra 'copy' here: move right away
				movePartition(node, p1.getId(), NodeSection.PRIMARY, node, NodeSection.SECONDARY);	//move partition to the same node's secondary section
			}												
			
			primaryPartitions.add(p1);
		}
		return primaryPartitions;
	}	
	
	private List<Node> getNodes(List<Node> exclude) {
		List<Node> nodes = new ArrayList<>();
		for (Node n : pt.getNodes()) {
			if (!exclude.contains(n)) {
				nodes.add(n);
			}
		}
		return nodes;
	}

	private void copyPartition(Node src, int srcPartId, NodeSection srcSection, Node dest, NodeSection destSection) {
		transferPartition(false, src, srcPartId, srcSection, dest, destSection);		
	}
	
	private void movePartition(Node src, int srcPartId, NodeSection srcSection, Node dest, NodeSection destSection) {
		transferPartition(true, src, srcPartId, srcSection, dest, destSection);		
	}

	/*
	 * TODO: There are possible local copies and removes - handle appropriately.
	 */
	private void transferPartition(boolean isMove, Node src, int srcPartId, NodeSection srcSection, Node dest, NodeSection destSection) {
		logger.debug((!isMove ? "Copying" : "Moving ") + 
			" partition '{}' from '{}'.{} to '{}'.{}", 
			srcPartId, src.getId(), srcSection, dest.getId(), destSection);
			
		Partition srcPart = null;
		for (Partition part : src.getData(srcSection)) {
			if (part.getId() == srcPartId) {
				srcPart = part;
				break;
			}
		}
		if (srcPart == null) {
			throw new RuntimeException(
				String.format("Failed to find partition in node. Node: %s, partition: %s", src, srcPartId));
		}
		
		dest.addPartition(destSection, srcPart);
		
		if (isMove) { //delete from src			
			//logger.debug("Deleting partition '{}' from {} node, isPrimary: {}", srcPart.getId(), src.getId(), isSrcPrimary);			
			src.removePartition(srcSection, srcPart);			
		}	
		
		//update partition table
		PartitionTableEntry ptEntry = pt.getEntryForPartition(srcPartId);
		
		if (isMove) {			
			if (srcSection == NodeSection.PRIMARY) {		
				ptEntry.removePrimaryNode(src);
			} else {
				ptEntry.removeSecondaryNode(src);
			}
		}				
		
		if (destSection == NodeSection.PRIMARY) {
			ptEntry.setPrimaryNode(dest);
		} else {
			ptEntry.addSecondaryNode(dest);
		}		
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
	private boolean clusterFull(Node newNode) {
		logger.warn("Cluster is full, do nothing. partitionsCount: {}, nodesCount: {}", pt.getPartitionsSize(), pt.getNodesSize());
		return false;
	}

	private void initCluster(Node newNode) {
		logger.info("Cluster is empty, init first node");				
		
		for (int i = 0; i < pt.getPartitionsSize(); i ++) { //create primary partitions
			newNode.createPartition(NodeSection.PRIMARY, i);			
		}					
	}
	
	public PartitionTable getPartitionTable() {
		return pt;
	}
	
	public void checkClusterPartitions(boolean checkCollisions) {
		Set<Partition> primaryTotal = new HashSet<>();
		int secondaryTotal = 0;
		
		List<String> errors = new ArrayList<>();
				
		for (Node n : pt.getNodes()) {
			primaryTotal.addAll(n.getPrimaryData());
			secondaryTotal += n.getSecondaryData().size();			
			
			//check collisions
			if (checkCollisions) {
				for (Partition p : n.getSecondaryData()) {
					if (n.getPrimaryData().contains(p)) {
						errors.add(String.format("Secondary and primary collide for node: %s", n.getId()));
					}
				}		
			}
			
			//duplicates
			if (n.getPrimaryData().size() != new HashSet<>(n.getPrimaryData()).size()) {
				errors.add("There are duplicates in primary: " + n.getId());
			}
			if (n.getSecondaryData().size() != new HashSet<>(n.getSecondaryData()).size()) {
				errors.add("There are duplicates in secondary: " + n.getId());
			}
		}		
				
		if (primaryTotal.size() != pt.getPartitionsSize()) {
			errors.add(String.format("Primary total is not correct. Expected: %d, was: %d", pt.getPartitionsSize(), primaryTotal));
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
}
