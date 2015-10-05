package com.map;

import static org.junit.Assert.*;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.junit.After;
import org.junit.Test;

public class LocalClusterTest {

	@After
	public void tearDown() {
		terminateCluster();
	}
	
	@Test
	public void testSimpleOps() {						
		String key = "key";
		String value = "value";
		String mapId = "mapId";
		
		createTestCluster();		
		Map<String, String> map = getRemoteMap(mapId);
						
		map.put(key, value);
		assertEquals(1, map.size());
		
		assertEquals(value, map.get(key));
		
		assertEquals(value, map.remove(key));
		assertEquals(0, map.size());
	}

	/*
	 * Check that 'put' adds data to secondary nodes.
	 * TODO: we can't check it with current API. If it is possible, uncomment.
	 */
//	@Test
//	public void testPut() {
//		String key = "key";
//		String value = "value";
//		String mapId = "mapId";
//		
//		int partitionsCount = 7;
//		int replicationFactor = 3;
//		int nodesCount = partitionsCount; 
//		
//		createTestCluster(partitionsCount, replicationFactor, nodesCount);
//		Map<String, String> map = getRemoteMap(mapId);
//								
//		map.put(key, value);
//		assertEquals(1, map.size());
//		
//		PartitionTable pt = getFirstNodeInCluster().getPartitionTable();
//		int partitionId = pt.getPartitionForKey(key);
//		
//		assertEquals(1, getMapSizeInNodes(pt, mapId, Arrays.asList(pt.getPrimaryNodeForPartition(partitionId)), NodeSection.PRIMARY));
//		assertEquals(replicationFactor, getMapSizeInNodes(pt, mapId, pt.getSecondaryNodesForPartition(partitionId), NodeSection.SECONDARY));		
//	}
	
	/*
	 * Test creating clusters with different replicationFactors: [0, 6].
	 * Should work without exceptions. 
	 */
	@Test
	public void testAddNodes() {
		int partitionsCount = 13;
				
		for (int replicationFactor = 0; replicationFactor < 7; replicationFactor ++) {			
			Configuration config = this.createTestConfig(partitionsCount, replicationFactor);			
			
			System.out.println("------------------------------------------------------- Creating cluster: partitionsCount: " + partitionsCount + ", replicationFactor: " + replicationFactor);
			
			for (int i = 0; i < partitionsCount; i ++) {
				//cluster.addNode(new NodeEntry(i, null));
				
				InetAddress address = null;
				Node node = new Node(i, address, config);
				node.init();	
								
				printPartitions();
				checkPartitions();
			}
			
			this.terminateCluster();
			
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
	 * Remove 2 nodes: replication factor is not changed during removal.
	 * No data loss.
	 */
	@Test
	public void testRemoveNode() {
		int partitionsCount = 7;
		int replicationFactor = 1;
		int nodesCount = partitionsCount;
						
		//create cluster
		Configuration config = this.createTestConfig(partitionsCount, replicationFactor);
		for (int i = 0; i < nodesCount; i ++) {
			InetAddress address = null;
			Node node = new Node(i, address, config);
			node.init();		
		}
		
		printPartitions();
		
		System.out.println("-------------------------------------------------------");
		System.out.println("Remove nodes");
		System.out.println("-------------------------------------------------------");
		
		//delete 2 nodes
		PartitionTable pt = getFirstNodeInCluster().getPartitionTable();		
		List<NodeEntry> deletedNodes = Arrays.asList(pt.getNodeEntry(1, false), pt.getNodeEntry(5, false));
		//cluster.removeNodes(deletedNodes);
		deleteNodes(deletedNodes);
		
		printPartitions();
				
		checkPartitionTableAfterRemoval(deletedNodes);					
	}
		
	/*
	 * Remove 4 nodes: data loss, interesting data re-distribution.
	 */
	@Test
	public void testRemoveNode_2() {
		int partitionsCount = 7;
		int replicationFactor = 2;
		int nodesCount = partitionsCount;
						
		//create cluster
		Configuration config = createTestConfig(partitionsCount, replicationFactor);
		for (int i = 0; i < nodesCount; i ++) {
			InetAddress address = null;
			Node node = new Node(i, address, config);
			node.init();
		}
		
		printPartitions();
		
		System.out.println("-------------------------------------------------------");
		System.out.println("Remove nodes");
		System.out.println("-------------------------------------------------------");
		
		//delete 2 nodes
		PartitionTable pt = getFirstNodeInCluster().getPartitionTable();		
		//cluster.removeNodes(deletedNodes);
		List<NodeEntry> deletedNodes = Arrays.asList(
			pt.getNodeEntry(1, false), 
			pt.getNodeEntry(2, false), 
			pt.getNodeEntry(3, false), 
			pt.getNodeEntry(5, false));
		deleteNodes(deletedNodes);
						
		printPartitions();
				
		checkPartitionTableAfterRemoval(deletedNodes);					
	}
	
	/*
	 * Remove 5 nodes: data loss, interesting data re-distribution.
	 */
	@Test
	public void testRemoveNode_3() {
		int partitionsCount = 7;
		int replicationFactor = 2;
		int nodesCount = partitionsCount;
						
		//create cluster
		Configuration config = createTestConfig(partitionsCount, replicationFactor);
		for (int i = 0; i < nodesCount; i ++) {
			InetAddress address = null;
			Node node = new Node(i, address, config);
			node.init();
		}
		
		printPartitions();
		
		System.out.println("-------------------------------------------------------");
		System.out.println("Remove nodes");
		System.out.println("-------------------------------------------------------");
		
		//delete nodes
		PartitionTable pt = getFirstNodeInCluster().getPartitionTable();
		List<NodeEntry> deletedNodes = Arrays.asList(
			pt.getNodeEntry(1, false), 
			pt.getNodeEntry(2, false), 
			pt.getNodeEntry(3, false), 
			pt.getNodeEntry(4, false), 
			pt.getNodeEntry(5, false));
		//cluster.removeNodes(deletedNodes);
		deleteNodes(deletedNodes);
		
		printPartitions();
				
		checkPartitionTableAfterRemoval(deletedNodes);					
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
		
		//create cluster
		Configuration config = createTestConfig(partitionsCount, replicationFactor);
		for (int i = 0; i < nodesCount; i ++) {
			InetAddress address = null;
			Node node = new Node(i, address, config);
			node.init();
		}
		
		printPartitions();
		
		System.out.println("-------------------------------------------------------");
		System.out.println("Remove nodes");
		System.out.println("-------------------------------------------------------");
		
		//delete 2 nodes
		PartitionTable pt = getFirstNodeInCluster().getPartitionTable();
		List<NodeEntry> deletedNodes = Arrays.asList(pt.getNodeEntry(1, false), pt.getNodeEntry(2, false));
		//cluster.removeNodes(deletedNodes);
		deleteNodes(deletedNodes);
		
		printPartitions();
				
		checkPartitionTableAfterRemoval(deletedNodes);		
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
		
		//create cluster
		Configuration config = createTestConfig(partitionsCount, replicationFactor);
		for (int i = 0; i < nodesCount; i ++) {
			InetAddress address = null;
			Node node = new Node(i, address, config);
			node.init();
		}
		
		printPartitions();
		
		System.out.println("-------------------------------------------------------");
		System.out.println("Remove nodes");
		System.out.println("-------------------------------------------------------");
		
		//delete 2 nodes
		PartitionTable pt = getFirstNodeInCluster().getPartitionTable();
		List<NodeEntry> deletedNodes = Arrays.asList(pt.getNodeEntry(1, false), pt.getNodeEntry(5, false));
		//cluster.removeNodes(deletedNodes);
		deleteNodes(deletedNodes);
		
		printPartitions();
				
		checkPartitionTableAfterRemoval(deletedNodes);					
	}
	
	/*
	 * Remove nodes: replication factor will be changed.
	 */
	@Test
	public void testRemoveNode_ChangeReplicationFactor1() {
		int partitionsCount = 7;
		int replicationFactor = 2;
		int nodesCount = 3;
		
		//create cluster
		Configuration config = createTestConfig(partitionsCount, replicationFactor);
		for (int i = 0; i < nodesCount; i ++) {
			InetAddress address = null;
			Node node = new Node(i, address, config);
			node.init();
		}
		
		printPartitions();
		
		System.out.println("-------------------------------------------------------");
		System.out.println("Remove nodes");
		System.out.println("-------------------------------------------------------");
		
		//delete nodes
		PartitionTable pt = getFirstNodeInCluster().getPartitionTable();
		List<NodeEntry> deletedNodes = Arrays.asList(pt.getNodeEntry(1, false));
		//cluster.removeNodes(deletedNodes);
		deleteNodes(deletedNodes);
		
		printPartitions();
				
		checkPartitionTableAfterRemoval(deletedNodes);					
	}
	
	/*
	 * Remove nodes: replication factor will be changed.
	 */
	@Test
	public void testRemoveNode_ChangeReplicationFactor2() {
		int partitionsCount = 7;
		int replicationFactor = 2;
		int nodesCount = 2;				
		
		//create cluster
		Configuration config = createTestConfig(partitionsCount, replicationFactor);
		for (int i = 0; i < nodesCount; i ++) {
			InetAddress address = null;
			Node node = new Node(i, address, config);
			node.init();
		}
		
		printPartitions();
		
		System.out.println("-------------------------------------------------------");
		System.out.println("Remove nodes");
		System.out.println("-------------------------------------------------------");
		
		//delete 2 nodes
		PartitionTable pt = getFirstNodeInCluster().getPartitionTable();
		List<NodeEntry> deletedNodes = Arrays.asList(pt.getNodeEntry(0, false));
		//cluster.removeNodes(deletedNodes);
		deleteNodes(deletedNodes);
		
		printPartitions();
				
		checkPartitionTableAfterRemoval(deletedNodes);					
	}
	
	/*
	 * Remove nodes one by one.
	 */
	@Test
	public void testRemoveAll() {
		int partitionsCount = 7;		
		int nodesCount = partitionsCount;
				
		for (int replicationFactor = 0; replicationFactor < partitionsCount; replicationFactor ++) {						
			//create cluster
			System.out.println("-------------------------------------------------------");
			System.out.println("Create cluster nodes, replicationFactor: " + replicationFactor);
			System.out.println("-------------------------------------------------------");
			
			Configuration config = createTestConfig(partitionsCount, replicationFactor);
			for (int i = 0; i < nodesCount; i ++) {
				InetAddress address = null;
				Node node = new Node(i, address, config);
				node.init();
			}
			
			printPartitions();		
			
			for (int i = 0; i < nodesCount; i ++) {
				System.out.println("-------------------------------------------------------");
				System.out.println("Remove nodes, node: " + i);
				System.out.println("-------------------------------------------------------");
				
				PartitionTable pt = getFirstNodeInCluster().getPartitionTable();
				List<NodeEntry> deletedNodes = Arrays.asList(pt.getNodeEntry(i, false));
				//cluster.removeNodes(deletedNodes);
				deleteNodes(deletedNodes);
				
				printPartitions();
				
				checkPartitionTableAfterRemoval(deletedNodes);
			}	
			
			terminateCluster();
		}											
	}

	//check that PartitionTable doesn't have any info about deleted nodes
	private void checkPartitionTableAfterRemoval(List<NodeEntry> deletedNodes) {
		if (isClusterEmpty()) {
			return;
		}
		
		PartitionTable pt = getFirstNodeInCluster().getPartitionTable();
		
		for (NodeEntry node : pt.getNodeEntries(true)) {
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
	
	private void printPartitions() {
		RemoteNode firstNode = getFirstNodeInCluster();
		if (firstNode != null) {
			PartitionTable pt = firstNode.getPartitionTable();

			System.out.println("----- Partition Table: -----");
			pt.printDebug();
			
			System.out.println();
			
			System.out.println("----- Nodes sizes: -----");
			System.out.printf("  id | primary | secondary%n");		
				
			for (NodeEntry n : pt.getNodeEntries(true)) {
				System.out.printf("%4d | %7d | %9d%n", n.getNodeId(), n.getPrimaryPartitionsCount(), n.getSecondaryPartitionsCount());												
			}		
			System.out.println();
			
			System.out.println("----- Nodes Details: -----");		
			for (NodeEntry n : pt.getNodeEntries(true)) {
				System.out.printf("node: %4s, primary: %s, secondary: %s%n", n.getNodeId(), n.getPrimaryPartitions(), n.getSecondaryPartitions());
			}
		} else {
			System.out.println("Cluster is empty");
		}

		
		System.out.println();								
	}
	
	private void checkPartitions() {
		PartitionTable pt = getFirstNodeInCluster().getPartitionTable();
		
		List<String> errors = new ArrayList<>();
				
		int minPrimary = Integer.MAX_VALUE, maxPrimary = Integer.MIN_VALUE;
		int minSecond = Integer.MAX_VALUE, maxSecond = Integer.MIN_VALUE;
				
		for (NodeEntry n : pt.getNodeEntries(true)) {
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
	
	private void terminateCluster() {
		LocalNodes.getInstance().getNodes().clear();	
	}
	
	private void createTestCluster() {
		int partitionsCount = 7;
		int replicationFactor = 1;
		int nodesCount = partitionsCount; 
		
		this.createTestCluster(partitionsCount, replicationFactor, nodesCount);
	}		
	
	private void createTestCluster(int partitionsCount, int replicationFactor, int nodesCount) {		
		Configuration config = createTestConfig(partitionsCount, replicationFactor);
					
		for (int i = 0; i < nodesCount; i ++) {
			InetAddress address = null;
			Node node = new Node(i, address, config);
			node.init();						
		}
		
		printPartitions();
		System.out.println("-------------------------------------------------------");
		System.out.println("-------------------------------------------------------");
	}
	
	private Configuration createTestConfig(int partitionsCount, int replicationFactor) {
		Configuration config = new BaseConfiguration();
		config.addProperty("replicationFactor", replicationFactor);
		config.addProperty("partitionsCount", partitionsCount);
		return config;
	}
	
	@SuppressWarnings("unchecked")
	private RemoteNodeMap<String, String> getRemoteMap(String mapId) {
		InetAddress address = null;
		RemoteNode remoteNode = Node.createRemoteNode(0, address);
		return (RemoteNodeMap<String, String>) remoteNode.getMap(mapId);
	}
	
	//return null if cluster is empty
	private RemoteNode getFirstNodeInCluster() {				
		Map<Integer, Node> nodes = LocalNodes.getInstance().getNodes();
		if (nodes.isEmpty()) {
			return null;
		}
		Integer nodeId = nodes.keySet().iterator().next();
		
		InetAddress address = null;
		return Node.createRemoteNode(nodeId, address);
	}
	
	/*
	 * TODO: call 'deleteNodes' directly on server Node - it is ok for now,
	 * but for real implementation use other mechanisms, e.g. interact with heartbeats. 
	 */
	private void deleteNodes(List<NodeEntry> deletedNodes) {
		Map<Integer, Node> nodes = LocalNodes.getInstance().getNodes();
		Integer nodeId = nodes.keySet().iterator().next();
		/*
		 * It is possible that node deletes itself. TODO: check that it works.
		 */
		nodes.get(nodeId).removeNodes(deletedNodes); 								
	}
	
	//TODO: use only in local mode
	private boolean isClusterEmpty() {
		Map<Integer, Node> nodes = LocalNodes.getInstance().getNodes();
		return nodes.isEmpty();
	}
}
