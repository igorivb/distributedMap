package com.map;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.junit.Test;

public class SimpleTest {

	@Test
	public void testSimpleOps() {						
		String key = "key";
		String value = "value";
		String mapId = "mapId";
		
		Cluster cluster = createTestCluster();
		Node node = cluster.getPartitionTable().getNodes().get(0);		
		@SuppressWarnings("unchecked")
		Map<String, String> map = (Map<String, String>) node.getMap(mapId);
		
		map.put(key, value);
		assertEquals(1, map.size());
		
		assertEquals(value, map.get(key));
		
		assertEquals(value, map.remove(key));
		assertEquals(0, map.size());
	}

	/*
	 * Check that 'put' adds data to secondary nodes.
	 */
	@Test
	public void testPut() {
		String key = "key";
		String value = "value";
		String mapId = "mapId";
		
		int partitionsCount = 7;
		int replicationFactor = 3;
		int nodesCount = partitionsCount; 
		
		Cluster cluster = createTestCluster(partitionsCount, replicationFactor, nodesCount);
		PartitionTable pt = cluster.getPartitionTable();
		
		Node node = pt.getNodes().get(0);	
		@SuppressWarnings("unchecked")
		Map<String, String> map = (Map<String, String>) node.getMap(mapId);
		
		map.put(key, value);
		assertEquals(1, map.size());
		
		int partitionId = pt.getPartitionForKey(key);
		
		assertEquals(1, getMapSizeInNodes(pt, mapId, Arrays.asList(pt.getPrimaryNodeForPartition(partitionId)), NodeSection.PRIMARY));
		assertEquals(replicationFactor, getMapSizeInNodes(pt, mapId, pt.getSecondaryNodesForPartition(partitionId), NodeSection.SECONDARY));		
	}
	
	private int getMapSizeInNodes(PartitionTable pt, String mapId, List<NodeEntry> nodes, NodeSection section) {
		int size = 0;
		for (NodeEntry nodeEntry : nodes) {	
			Node node = pt.getNodes().get(nodeEntry.getId());
			for (Partition part : node.getData(section)) {
				size += part.getMap(mapId).size();
			}	
		}		
		return size;
	}

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
				cluster.addNode(new NodeEntry(i, null));
				printPartitions(cluster);
				checkPartitions(cluster);
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
	
	/*
	 * Remove 2 nodes: replication factor is the same.
	 * No data loss.
	 */
	@Test
	public void testRemoveNode() {
		int partitionsCount = 7;
		int replicationFactor = 1;
		int nodesCount = partitionsCount;
		
		List<NodeEntry> nodes = new ArrayList<>();
		
		//create cluster
		Cluster cluster = new Cluster(replicationFactor, partitionsCount);
		for (int i = 0; i < nodesCount; i ++) {
			NodeEntry node = new NodeEntry(i, null);
			nodes.add(node);
			cluster.addNode(node);
		}
		
		printPartitions(cluster);
		
		System.out.println("-------------------------------------------------------");
		System.out.println("Remove nodes");
		System.out.println("-------------------------------------------------------");
		
		//delete 2 nodes
		List<NodeEntry> deletedNodes = Arrays.asList(nodes.get(0), nodes.get(5));
		cluster.removeNodes(deletedNodes);
		
		printPartitions(cluster);
				
		checkPartitionTableAfterRemoval(cluster, deletedNodes);					
	}
	
	/*
	 * Remove 4 nodes: data loss, interesting data re-distribution.
	 */
	@Test
	public void testRemoveNode_2() {
		int partitionsCount = 7;
		int replicationFactor = 2;
		int nodesCount = partitionsCount;
		
		List<NodeEntry> nodes = new ArrayList<>();
		
		//create cluster
		Cluster cluster = new Cluster(replicationFactor, partitionsCount);
		for (int i = 0; i < nodesCount; i ++) {
			NodeEntry node = new NodeEntry(i, null);
			nodes.add(node);
			cluster.addNode(node);
		}
		
		printPartitions(cluster);
		
		System.out.println("-------------------------------------------------------");
		System.out.println("Remove nodes");
		System.out.println("-------------------------------------------------------");
		
		//delete 2 nodes
		List<NodeEntry> deletedNodes = Arrays.asList(nodes.get(0), nodes.get(2), nodes.get(3), nodes.get(5));
		cluster.removeNodes(deletedNodes);
		
		printPartitions(cluster);
				
		checkPartitionTableAfterRemoval(cluster, deletedNodes);					
	}
	
	/*
	 * Remove 5 nodes: data loss, interesting data re-distribution.
	 */
	@Test
	public void testRemoveNode_3() {
		int partitionsCount = 7;
		int replicationFactor = 2;
		int nodesCount = partitionsCount;
		
		List<NodeEntry> nodes = new ArrayList<>();
		
		//create cluster
		Cluster cluster = new Cluster(replicationFactor, partitionsCount);
		for (int i = 0; i < nodesCount; i ++) {
			NodeEntry node = new NodeEntry(i, null);
			nodes.add(node);
			cluster.addNode(node);
		}
		
		printPartitions(cluster);
		
		System.out.println("-------------------------------------------------------");
		System.out.println("Remove nodes");
		System.out.println("-------------------------------------------------------");
		
		//delete 2 nodes
		List<NodeEntry> deletedNodes = Arrays.asList(nodes.get(0), nodes.get(2), nodes.get(3), nodes.get(4), nodes.get(5));
		cluster.removeNodes(deletedNodes);
		
		printPartitions(cluster);
				
		checkPartitionTableAfterRemoval(cluster, deletedNodes);					
	}
	
	/*
	 * Remove 2 nodes: replication factor is the same.
	 * Data loss.
	 */
	@Test
	public void testRemoveNode_DataLoss() {
		int partitionsCount = 7;
		int replicationFactor = 1;
		int nodesCount = partitionsCount;
		
		List<NodeEntry> nodes = new ArrayList<>();
		
		//create cluster
		Cluster cluster = new Cluster(replicationFactor, partitionsCount);
		for (int i = 0; i < nodesCount; i ++) {
			NodeEntry node = new NodeEntry(i, null);
			nodes.add(node);
			cluster.addNode(node);
		}
		
		printPartitions(cluster);
		
		System.out.println("-------------------------------------------------------");
		System.out.println("Remove nodes");
		System.out.println("-------------------------------------------------------");
		
		//delete 2 nodes
		List<NodeEntry> deletedNodes = Arrays.asList(nodes.get(0), nodes.get(2));
		cluster.removeNodes(deletedNodes);
		
		printPartitions(cluster);
				
		checkPartitionTableAfterRemoval(cluster, deletedNodes);		
	}
	
	/*
	 * Remove 2 nodes: no replicas.
	 * No data loss.
	 */
	@Test
	public void testRemoveNode_NoReplica() {
		int partitionsCount = 7;
		int replicationFactor = 0;
		int nodesCount = partitionsCount;
		
		List<NodeEntry> nodes = new ArrayList<>();
		
		//create cluster
		Cluster cluster = new Cluster(replicationFactor, partitionsCount);
		for (int i = 0; i < nodesCount; i ++) {
			NodeEntry node = new NodeEntry(i, null);
			nodes.add(node);
			cluster.addNode(node);
		}
		
		printPartitions(cluster);
		
		System.out.println("-------------------------------------------------------");
		System.out.println("Remove nodes");
		System.out.println("-------------------------------------------------------");
		
		//delete 2 nodes
		List<NodeEntry> deletedNodes = Arrays.asList(nodes.get(0), nodes.get(5));
		cluster.removeNodes(deletedNodes);
		
		printPartitions(cluster);
				
		checkPartitionTableAfterRemoval(cluster, deletedNodes);					
	}
	
	/*
	 * Remove nodes: replication factor will be changed.
	 */
	@Test
	public void testRemoveNode_ChangeReplicationFactor1() {
		int partitionsCount = 7;
		int replicationFactor = 2;
		int nodesCount = 3;
		
		List<NodeEntry> nodes = new ArrayList<>();
		
		//create cluster
		Cluster cluster = new Cluster(replicationFactor, partitionsCount);
		for (int i = 0; i < nodesCount; i ++) {
			NodeEntry node = new NodeEntry(i, null);
			nodes.add(node);
			cluster.addNode(node);
		}
		
		printPartitions(cluster);
		
		System.out.println("-------------------------------------------------------");
		System.out.println("Remove nodes");
		System.out.println("-------------------------------------------------------");
		
		//delete 2 nodes
		List<NodeEntry> deletedNodes = Arrays.asList(nodes.get(0));
		cluster.removeNodes(deletedNodes);
		
		printPartitions(cluster);
				
		checkPartitionTableAfterRemoval(cluster, deletedNodes);					
	}
	
	/*
	 * Remove nodes: replication factor will be changed.
	 */
	@Test
	public void testRemoveNode_ChangeReplicationFactor2() {
		int partitionsCount = 7;
		int replicationFactor = 2;
		int nodesCount = 2;
		
		List<NodeEntry> nodes = new ArrayList<>();
		
		//create cluster
		Cluster cluster = new Cluster(replicationFactor, partitionsCount);
		for (int i = 0; i < nodesCount; i ++) {
			NodeEntry node = new NodeEntry(i, null);
			nodes.add(node);
			cluster.addNode(node);
		}
		
		printPartitions(cluster);
		
		System.out.println("-------------------------------------------------------");
		System.out.println("Remove nodes");
		System.out.println("-------------------------------------------------------");
		
		//delete 2 nodes
		List<NodeEntry> deletedNodes = Arrays.asList(nodes.get(0));
		cluster.removeNodes(deletedNodes);
		
		printPartitions(cluster);
				
		checkPartitionTableAfterRemoval(cluster, deletedNodes);					
	}
	
	/*
	 * Remove nodes one by one.
	 */
	@Test
	public void testRemoveAll() {
		int partitionsCount = 7;		
		int nodesCount = partitionsCount;
				
		for (int replicationFactor = 0; replicationFactor < partitionsCount; replicationFactor ++) {
			List<NodeEntry> nodes = new ArrayList<>();
			
			//create cluster
			System.out.println("-------------------------------------------------------");
			System.out.println("Create cluster nodes, replicationFactor: " + replicationFactor);
			System.out.println("-------------------------------------------------------");
			
			Cluster cluster = new Cluster(replicationFactor, partitionsCount);
			for (int i = 0; i < nodesCount; i ++) {
				NodeEntry node = new NodeEntry(i, null);
				nodes.add(node);
				cluster.addNode(node);
			}
			
			printPartitions(cluster);		
			
			for (int i = 0; i < nodesCount; i ++) {
				System.out.println("-------------------------------------------------------");
				System.out.println("Remove nodes, node: " + i);
				System.out.println("-------------------------------------------------------");
				
				List<NodeEntry> deletedNodes = Arrays.asList(nodes.get(i));
				cluster.removeNodes(deletedNodes);
				
				printPartitions(cluster);
				
				checkPartitionTableAfterRemoval(cluster, deletedNodes);
			}			
		}											
	}

	//check that PartitionTable doesn't have any info about deleted nodes
	private void checkPartitionTableAfterRemoval(Cluster cluster, List<NodeEntry> deletedNodes) {
		if (cluster.isEmpty()) {
			return;
		}
		
		PartitionTable pt = cluster.getPartitionTable();
		
		for (NodeEntry node : pt.getNodeEntries()) {
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
			
			List<NodeEntry> secNodes = entry.getSecondaryNodes();
			for (NodeEntry secNode : secNodes) {
				if (deletedNodes.contains(secNode)) {
					fail(String.format(
						"Contains deleted node entry in secondary, partition: %s, node: %s", 
						entry.getPartitionId(), secNode));
				}
			}
		}
	}
	
	private void printPartitions(Cluster cluster) {
		PartitionTable pt = cluster.getPartitionTable();

		System.out.println("----- Partition Table: -----");
		pt.printDebug();
		
		System.out.println();
		
		System.out.println("----- Nodes sizes: -----");
		System.out.printf("  id | primary | secondary%n");		
			
		for (NodeEntry n : pt.getNodeEntries()) {
			System.out.printf("%4d | %7d | %9d%n", n.getId(), n.getPrimaryPartitionsCount(), n.getSecondaryPartitionsCount());												
		}		
		System.out.println();
		
		System.out.println("----- Nodes Details: -----");		
		for (NodeEntry n : pt.getNodeEntries()) {
			System.out.printf("node: %4s, primary: %s, secondary: %s%n", n.getId(), n.getPrimaryPartitionsCount(), n.getSecondaryPartitionsCount());
		}
		
		System.out.println();								
	}
	
	private void checkPartitions(Cluster cluster) {
		PartitionTable pt = cluster.getPartitionTable();
		
		List<String> errors = new ArrayList<>();
				
		int minPrimary = Integer.MAX_VALUE, maxPrimary = Integer.MIN_VALUE;
		int minSecond = Integer.MAX_VALUE, maxSecond = Integer.MIN_VALUE;
				
		for (NodeEntry n : pt.getNodeEntries()) {
			minPrimary = Math.min(minPrimary, n.getPrimaryPartitionsCount());
			maxPrimary = Math.max(maxPrimary, n.getPrimaryPartitionsCount());
			
			minSecond = Math.min(minSecond, n.getSecondaryPartitionsCount());
			maxSecond = Math.max(maxSecond, n.getSecondaryPartitionsCount());									
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
	
	private Cluster createTestCluster() {
		int partitionsCount = 7;
		int replicationFactor = 1;
		int nodesCount = partitionsCount; 
		
		return this.createTestCluster(partitionsCount, replicationFactor, nodesCount);
	}
	
	private Cluster createTestCluster(int partitionsCount, int replicationFactor, int nodesCount) {				
		Cluster cluster = new Cluster(replicationFactor, partitionsCount);
		for (int i = 0; i < nodesCount; i ++) {
			NodeEntry node = new NodeEntry(i, null);			
			cluster.addNode(node);
		}
		
		printPartitions(cluster);
		System.out.println("-------------------------------------------------------");
		System.out.println("-------------------------------------------------------");
		
		return cluster;
	}
}
