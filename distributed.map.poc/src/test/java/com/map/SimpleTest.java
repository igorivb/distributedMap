package com.map;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

public class SimpleTest {

	/*
	 * Test first version of adding node.
	 * Should work without exceptions. 
	 */
	@Test
	public void testAddNodes() {
		int partitionsCount = 13;
				
		for (int replicationFactor = 0; replicationFactor < 7; replicationFactor ++) {		
					
			Cluster cluster = new Cluster(replicationFactor, partitionsCount);
			for (int i = 0; i < partitionsCount + 1; i ++) {
				cluster.addNode(new Node(i, null));
				printAndCheckPartitions(cluster);
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
	
	private void printAndCheckPartitions(Cluster cluster) {
		PartitionTable pt = cluster.getPartitionTable();
		
		List<String> errors = new ArrayList<>();
		
		System.out.println();
		System.out.printf("  id | primary | secondary%n");		
				
		int minPrimary = Integer.MAX_VALUE, maxPrimary = Integer.MIN_VALUE;
		int minSecond = Integer.MAX_VALUE, maxSecond = Integer.MIN_VALUE;
				
		for (Node n : pt.getNodes()) {
			System.out.printf("%4d | %7d | %9d%n", n.getId(), n.getPrimaryData().size(), n.getSecondaryData().size());			
			
			minPrimary = Math.min(minPrimary, n.getPrimaryData().size());
			maxPrimary = Math.max(maxPrimary, n.getPrimaryData().size());
			
			minSecond = Math.min(minSecond, n.getSecondaryData().size());
			maxSecond = Math.max(maxSecond, n.getSecondaryData().size());									
		}		
		System.out.println();
		
		System.out.println("----- Details: -----");		
		for (Node n : pt.getNodes()) {
			System.out.printf("node: %4s, primary: %s, secondary: %s%n", n.getId(), n.getPrimaryData(), n.getSecondaryData());
		}
		
		System.out.println();
		
		System.out.println("----- Partition Table: -----");
		pt.printDebug();
		
		System.out.println();			
		
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
	
	/**
	 * TODO: test cases:
	 * 1. data loss
	 * 2. remove several nodes
	 * 3. check with various replication factors
	 * 4. all nodes are deleted
	 */
	@Test
	public void testRemoveNode() {
		int partitionsCount = 7;
		int replicationFactor = 1;
		
		List<Node> nodes = new ArrayList<>();
		
		//create cluster
		Cluster cluster = new Cluster(replicationFactor, partitionsCount);
		for (int i = 0; i < partitionsCount; i ++) {
			Node node = new Node(i, null);
			nodes.add(node);
			cluster.addNode(node);
		}
		
		//delete 2 nodes
		List<Node> deletedNodes = Arrays.asList(nodes.get(0), nodes.get(5));
		cluster.removeNodes(deletedNodes);
		
		//print
		cluster.getPartitionTable().printDebug();
				
		checkPartitionTableAfterRemoval(cluster, deletedNodes);		
	}

	//check that PartitionTable doesn't have any info about deleted nodes
	private void checkPartitionTableAfterRemoval(Cluster cluster, List<Node> deletedNodes) {
		PartitionTable pt = cluster.getPartitionTable();
		
		for (Node node : pt.getNodes()) {
			if (deletedNodes.contains(node)) {
				fail("Nodes contain deleted node: " + node);
			}
		}
		
		for (PartitionTableEntry entry : pt.getPartitionEntries().values()) {
			assertTrue("Primary node is null, partition: " + entry.getPartitionId(), entry.getPrimaryNode() != null);
			
			if (deletedNodes.contains(entry.getPrimaryNode())) {
				fail(String.format(
					"Contains deleted node entry in primary, partition: %s, node: %s", 
					entry.getPartitionId(), entry.getPrimaryNode()));
			}
			
			List<Node> secNodes = entry.getSecondaryNodes();
			for (Node secNode : secNodes) {
				if (deletedNodes.contains(secNode)) {
					fail(String.format(
						"Contains deleted node entry in secondary, partition: %s, node: %s", 
						entry.getPartitionId(), secNode));
				}
			}
		}
		
	}
}
