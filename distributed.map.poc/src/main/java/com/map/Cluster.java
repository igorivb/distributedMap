package com.map;

import java.util.ArrayList;
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
	
	
	//Node comparators
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
			
			List<Partition> primaryPartitions = processPrimary(newNode, isBalanced);
			
			if (pt.hasReplica()) {
				processSecondary(newNode, isBalanced, primaryPartitions);
			}
		}	
				
		if (added) {			
			logger.info("Added new node to nodes list: {}", newNode.getId());
		}
				
		printAndCheckPartitions();
	}
	
	/**
	 * Allows to delete multiple nodes at a time.
	 */
	public void removeNodes(List<Node> nodes) {
		//TODO
	}
	
	private void printAndCheckPartitions() {
		System.out.println();
		System.out.printf("  id | primary | secondary%n");		
		
		int primaryTotal = 0, secondaryTotal = 0;
		int minPrimary = Integer.MAX_VALUE, maxPrimary = Integer.MIN_VALUE;
		int minSecond = Integer.MAX_VALUE, maxSecond = Integer.MIN_VALUE;
		
		List<String> errors = new ArrayList<>();
				
		for (Node n : pt.getNodes()) {
			System.out.printf("%4d | %7d | %9d%n", n.getId(), n.getPrimaryData().size(), n.getSecondaryData().size());
			primaryTotal += n.getPrimaryData().size();
			secondaryTotal += n.getSecondaryData().size();
			
			minPrimary = Math.min(minPrimary, n.getPrimaryData().size());
			maxPrimary = Math.max(maxPrimary, n.getPrimaryData().size());
			
			minSecond = Math.min(minSecond, n.getSecondaryData().size());
			maxSecond = Math.max(maxSecond, n.getSecondaryData().size());
			
			//check collisions
			for (Partition p : n.getSecondaryData()) {
				if (n.getPrimaryData().contains(p)) {
					errors.add(String.format("Secondary and primary collide for node: %s", n.getId()));
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
		System.out.println();
		
		System.out.println("Details:");
		for (Node n : pt.getNodes()) {
			System.out.printf("%4d. primary: %s, secondary: %s%n ", n.getId(), n.getPrimaryData(), n.getSecondaryData());
		}
		
		System.out.println();
		
		System.out.println("Partition Table:");
		pt.printDebug();
		
		System.out.println();
		
		if (primaryTotal != pt.getPartitionsSize()) {
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
		
		if (maxPrimary - minPrimary > 1) {
			errors.add("Big difference between primaries: " + (maxPrimary - minPrimary));
		}
		if (pt.hasReplica()) {
			if (maxSecond - minSecond > 1) {
				errors.add("Big difference between secondaries: " + (maxPrimary - minPrimary));
			}
		}
		
		
		if (!errors.isEmpty()) {
			throw new RuntimeException("Found errors: " + errors);
		}
	}
	
	private void processSecondary(Node newNode, boolean isBalanced, List<Partition> primaryPartitions) {				
		if (isBalanced) {									
			int num = (pt.getPartitionsSize() * pt.getReplicationFactor()) / pt.getNodesSize(); //ignore remaining part of division
			logger.debug("Processing secondary: balanced, num: {}", num);
			
			Set<Partition> addedParts = new HashSet<>(primaryPartitions); 
			
			//sort nodes by secondaryData.len desc
			List<Node> sortedNodes = new ArrayList<>(pt.getNodes());			
									
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
												
				movePartition(node, part, NodeSection.SECONDARY, newNode, NodeSection.SECONDARY);
										
				i ++;
			}		
		} else {
			logger.debug("Processing secondary: not balanced");
			
			//copy all primary to new node's secondary
			for (Node node : pt.getNodes()) {
				for (Partition part : node.getPrimaryData()) {
					copyPartition(node, part, NodeSection.PRIMARY, newNode, NodeSection.SECONDARY);
				}
			}
		}				
	}	

	private List<Partition> processPrimary(Node newNode, boolean isBalanced) {								
		int num = pt.getPartitionsSize() / pt.getNodesSize(); //ignore remaining part of division
		logger.debug("Processing primary, num: {}", num);
		
		List<Partition> primaryPartitions = new ArrayList<>();
		
		//sort nodes by primaryData.len desc
		List<Node> sortedNodes = new ArrayList<>(pt.getNodes());		
		
		for (int i = 0; i < num; i ++) {
			Collections.sort(sortedNodes, primaryNodesCmp);
			
			Node node = sortedNodes.get(0);
			Partition p1 = node.getPrimaryData().get(0);
			
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

	private void copyPartition(Node src, Partition srcPart, NodeSection srcSection, Node dest, NodeSection destSection) {
		transferPartition(true, src, srcPart, srcSection, dest, destSection);		
	}
	
	private void movePartition(Node src, Partition srcPart, NodeSection srcSection, Node dest, NodeSection destSection) {
		transferPartition(false, src, srcPart, srcSection, dest, destSection);		
	}

	private void transferPartition(boolean isCopy, Node src, Partition srcPart, NodeSection srcSection, Node dest, NodeSection destSection) {
		logger.debug((isCopy ? "Copying" : "Moving") + 
			" partition '{}' from '{}'.{} to '{}'.{}", 
			srcPart.getId(), src.getId(), srcSection, dest.getId(), destSection);
			
		dest.addPartition(destSection, srcPart);
		
		if (!isCopy) { //delete from src			
			//logger.debug("Deleting partition '{}' from {} node, isPrimary: {}", srcPart.getId(), src.getId(), isSrcPrimary);
			
			src.removePartition(srcSection, srcPart);			
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
			Partition p = new Partition(i);
			newNode.addPartition(NodeSection.PRIMARY, p);
		}					
	}		
}
