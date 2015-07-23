package com.map;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Cluster {

	private static final Logger logger = LoggerFactory.getLogger(Cluster.class);
	
	private int replicationFactor;
	
	private int partitionsCount;
	
	//nodes in cluster
	private List<Node> nodes = new ArrayList<>();

	
	//Node comparators
	static class NodesComparator implements Comparator<Node> {
		boolean isPrimary;
		public NodesComparator(boolean isPrimary) {
			this.isPrimary = isPrimary;
		}
		@Override
		public int compare(Node o1, Node o2) {
			int n1 = o1.getData(isPrimary).size();
			int n2 = o2.getData(isPrimary).size();
			return n1 == n2 ? 0 : (n1 < n2 ? 1 : -1);
		}		
	}	
	NodesComparator primaryNodesCmp = new NodesComparator(true);
	NodesComparator secondaryNodesCmp = new NodesComparator(false);
	
	
	public Cluster(int replicationFactor, int partitionsCount) {
		this.replicationFactor = replicationFactor;	
		this.partitionsCount = partitionsCount;			
		
		logger.info("Creating cluster, replicationFactor: {}, partitionsCount: {}", replicationFactor, partitionsCount);
	}
	
//	int getNextNodesLen() {
//		return this.nodes.size() + 1;
//	}
	
	public void addNode(Node newNode) {
		logger.info("Adding node: {}", newNode.getId());
		
		boolean added = true;
		if (nodes.size() == 0) { //there are no nodes	
			initCluster(newNode);			
		} else if (this.nodes.size() == this.partitionsCount) { //cluster is full
			clusterFull(newNode);
			added = false;
		} else {
			boolean isBalanced = nodes.size() >= replicationFactor + 1; 
			logger.info("Is balanced: {}", isBalanced);
			
			List<Partition> primaryPartitions = processPrimary(newNode, isBalanced);
			
			if (replicationFactor > 0) {
				processSecondary(newNode, isBalanced, primaryPartitions);
			}
		}	
				
		if (added) {
			this.nodes.add(newNode);		
			logger.info("Added new node to nodes list");
		}
				
		printAndCheckPartitions();
	}
	
	private void printAndCheckPartitions() {
		System.out.println();
		System.out.printf("  id | primary | secondary%n");		
		
		int primaryTotal = 0, secondaryTotal = 0;
		int minPrimary = Integer.MAX_VALUE, maxPrimary = Integer.MIN_VALUE;
		int minSecond = Integer.MAX_VALUE, maxSecond = Integer.MIN_VALUE;
		
		List<String> errors = new ArrayList<>();
				
		for (Node n : nodes) {
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
		for (Node n : nodes) {
			System.out.printf("%4d. primary: %s, secondary: %s%n ", n.getId(), n.getPrimaryData(), n.getSecondaryData());
		}
		
		System.out.println();
		
		if (primaryTotal != this.partitionsCount) {
			errors.add(String.format("Primary total is not correct. Expected: %d, was: %d", this.partitionsCount, primaryTotal));
		}
		
		if (replicationFactor > 0) {
			int secExpected = (nodes.size() > replicationFactor ? this.replicationFactor : nodes.size() - 1) * this.partitionsCount;
			if (secondaryTotal != secExpected) {
				errors.add(String.format("Secondary total is not correct. Expected: %d, was: %d", secExpected, secondaryTotal));
			}							
		} else if (secondaryTotal > 0) {
			errors.add("Secondary total is not empty: " + secondaryTotal);
		}				
		
		if (maxPrimary - minPrimary > 1) {
			errors.add("Big difference between primaries: " + (maxPrimary - minPrimary));
		}
		if (replicationFactor > 0) {
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
			int num = (partitionsCount * this.replicationFactor) / (nodes.size() + 1); //ignore remaining part of division
			logger.info("Processing secondary: balanced, num: {}", num);
			
			Set<Partition> addedParts = new HashSet<>(primaryPartitions); 
			
			//sort nodes by secondaryData.len desc
			List<Node> sortedNodes = new ArrayList<>(nodes);			
									
			for (int i = 0; i < num;) {	
				if (sortedNodes.isEmpty()) {
					logger.warn("Didn't all needed nodes to secondary, left: {}", num);
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
						logger.warn("Don't add secondary partition: '{}' in node {}, because it already exists", p.getId(), node.getId());
					}
				}
				if (part == null) { //don't process this node any more
					sortedNodes.remove(node);
					continue;
				}
												
				movePartition(node, part, false, newNode, false); //move partition from src.secondary to dest.secondary
										
				i ++;
			}		
		} else {
			logger.info("Processing secondary: not balanced");
			
			//copy all primary to new node's secondary
			for (Node node : this.nodes) {
				for (Partition part : node.getPrimaryData()) {
					copyPartition(node, part, true, newNode, false);
				}
			}
		}				
	}	

	private List<Partition> processPrimary(Node newNode, boolean isBalanced) {								
		int num = partitionsCount / (nodes.size() + 1); //ignore remaining part of division
		logger.info("Processing primary, num: {}", num);
		
		List<Partition> primaryPartitions = new ArrayList<>();
		
		//sort nodes by primaryData.len desc
		List<Node> sortedNodes = new ArrayList<>(nodes);		
		
		for (int i = 0; i < num; i ++) {
			Collections.sort(sortedNodes, primaryNodesCmp);
			
			Node node = sortedNodes.get(0);
			Partition p1 = node.getPrimaryData().get(0);
			
			if (isBalanced) {
				movePartition(node, p1, true, newNode, true); //move partition from src.primary to dest.primary
			} else {
				copyPartition(node, p1, true, newNode, true); //copy partition from src.primary to dest.primary
				
				movePartition(node, p1, true, node, false);	//move partition to the same node's secondary section
			}												
			
			primaryPartitions.add(p1);
		}
		return primaryPartitions;
	}	

	private void copyPartition(Node src, Partition srcPart, boolean isSrcPrimary, Node dest, boolean isDestPrimary) {
		transferPartition(true, src, srcPart, isSrcPrimary, dest, isDestPrimary);		
	}
	
	private void movePartition(Node src, Partition srcPart, boolean isSrcPrimary, Node dest, boolean isDestPrimary) {
		transferPartition(false, src, srcPart, isSrcPrimary, dest, isDestPrimary);		
	}

	private void transferPartition(boolean isCopy, Node src, Partition srcPart, boolean isSrcPrimary, Node dest, boolean isDestPrimary) {
		logger.info((isCopy ? "Copying" : "Moving") + 
			" partition '{}' from node '{}' to node '{}', isSrcPrimary: {}, isDestPrimary: {}", 
			srcPart.getId(), src.getId(), dest.getId(), isSrcPrimary, isDestPrimary);
			
		dest.getData(isDestPrimary).add(srcPart);
		
		if (!isCopy) { //delete from src			
			logger.info(
				"Deleting partition '{}' from {} node, isPrimary: {}", 
				srcPart.getId(), src.getId(), isSrcPrimary);
			
			src.getData(isSrcPrimary).remove(srcPart);			
		}		
	}

	/*
	 * Possible strategies:
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
	private void clusterFull(Node newNode) {
		logger.warn("Cluster is full, do nothing. partitionsCount: {}, nodesCount: {}", partitionsCount, nodes.size());		
	}

	private void initCluster(Node newNode) {
		logger.info("Cluster is empty, init first node");
		
		for (int i = 0; i < partitionsCount; i ++) { //create partitions				
			Partition p = new Partition(i);
			newNode.addPrimaryPartition(p);
		}		
	}
	
	public static void main(String[] args) {		
		int partitionsCount = 271;
						
		for (int replicationFactor = 0; replicationFactor < 7; replicationFactor ++) {			
			Cluster cluster = new Cluster(replicationFactor, partitionsCount);
			for (int i = 0; i < partitionsCount + 1; i ++) {
				cluster.addNode(new Node(i));
			}
			
			System.out.println();
			System.out.println();
			System.out.println("-------------------------------------------------------");
			System.out.println("-------------------------------------------------------");
			System.out.println("-------------------------------------------------------");
			System.out.println("-------------------------------------------------------");
			System.out.println();
			System.out.println();
		}
		
	}
}
