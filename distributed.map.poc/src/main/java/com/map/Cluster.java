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
	NodesComparator primaryNodesCmp = new NodesComparator(NodeSection.PRIMARY);
	NodesComparator secondaryNodesCmp = new NodesComparator(NodeSection.SECONDARY);
	
	
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
	}
	
	/**
	 * Allows to delete multiple nodes at a time.
	 */
	public void removeNodes(List<Node> deletedNodes) {
		logger.info("Removing nodes: {}", deletedNodes);
				
		for (Node node : deletedNodes) { //mark nodes as deleted
			pt.markNodeAsDeleted(node);
		}
		
		//TODO: handle if all nodes are deleted		
		
		//TODO: handle if we have previous remove in progress: stop it and start this one
		
		processRemovePrimary(deletedNodes);
		
		processRemoveSecondary(deletedNodes);
		
		balanceRemoveNodes(deletedNodes);
				
		pt.deleteNodes(deletedNodes);
	}
	
	/*
	 * Restore deleted primary partitions from secondary nodes, if there are any, 
	 * otherwise create empty partitions. 
	 */
	private void processRemovePrimary(List<Node> deletedNodes) {
		logger.debug("Processing remove primary");				
		
		//sort nodes by primaryData.len asc		
		List<Node> sortedNodes = getNodes(deletedNodes);
		Comparator<Node> nodesCmp = new ReversedNodesComparator(primaryNodesCmp);
		
		List<Integer> deletedPrimaryParts = this.getDeletedPrimaryPartitions(deletedNodes);
		for (Integer deletedPrimaryPart : deletedPrimaryParts) {			
			logger.debug("Handle deleted primary partition: {}", deletedPrimaryPart);
			
			Collections.sort(sortedNodes, nodesCmp);
			
			//find nodes which contain deleted partition
			List<Node> secNodes = excludeDeletedNodes(pt.getSecondaryNodesForPartition(deletedPrimaryPart));
			
			logger.debug("Secondary nodes which contain deleted partition: {}, nodes: {}", deletedPrimaryPart, secNodes);
			if (!secNodes.isEmpty()) {
				Node replicaNode = secNodes.get(0); //take first one
				
				//get node with minimum primary
				Node targetNode = sortedNodes.get(0);
				
				copyPartition(replicaNode, deletedPrimaryPart, NodeSection.SECONDARY, targetNode, NodeSection.PRIMARY);				
			} else { //data loss
				logger.warn("Data are lost for partition, create a new one, partition id: {}", deletedPrimaryPart);
				
				//create new partition and register it in PartitionTable
				Node targetNode = sortedNodes.get(0);				
				Partition part = targetNode.createPartition(NodeSection.PRIMARY, deletedPrimaryPart);
				
				pt.getEntryForPartition(part.getId()).setPrimaryNode(targetNode);
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
		List<Integer> res = new ArrayList<>();		
		for (PartitionTableEntry entry : pt.getPartitionEntries().values()) {
			for (Node node : deletedNodes) {
				if (entry.getPrimaryNode().getId() == node.getId()) {
					res.add(entry.getPartitionId());
					break;
				}	
			}
		}			
		return res;
	}

	private void balanceRemoveNodes(List<Node> nodesToDelete) {
		//TODO: implement		
	}

	private void processRemoveSecondary(List<Node> nodesToDelete) {
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
				Collections.sort(sortedNodes, secondaryNodesCmp);
				
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
			Collections.sort(sortedNodes, primaryNodesCmp);
			
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
			Partition p = newNode.createPartition(NodeSection.PRIMARY, i);
			pt.getEntryForPartition(p.getId()).setPrimaryNode(newNode);
		}					
	}
	
	public PartitionTable getPartitionTable() {
		return pt;
	}
}
