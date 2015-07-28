package com.map;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PartitionTable {

	private static final Logger logger = LoggerFactory.getLogger(PartitionTable.class);
	
	private int replicationFactor;
	private int partitionsCount;
	
	private Map<Integer, PartitionTableEntry> partitionEntries;
	
	//nodes in cluster: all nodes, including removed
	private Map<Integer, Node> nodes;
	
	public PartitionTable(int replicationFactor, int partitionsCount) {
		this.replicationFactor = replicationFactor;
		this.partitionsCount = partitionsCount;
		
		partitionEntries = new HashMap<>(partitionsCount);
				
		for (int i = 0; i < partitionsCount; i ++) { //create empty entries
			partitionEntries.put(i, new PartitionTableEntry(i));
		}
		
		nodes = new HashMap<>();
	}
	
	public int getReplicationFactor() {
		return replicationFactor;
	}
	
	public int getPartitionsSize() {
		return partitionsCount;
	}
	
	/**
	 * Return all nodes, including removed.
	 */
	public Collection<Node> getNodes() {
		return this.nodes.values();
	}
	
	public int getNodesSize() {
		return this.nodes.size();		
	}
	
	public void addNode(Node newNode) {
		this.nodes.put(newNode.getId(), newNode);
		newNode.setPartitionTable(this);
	}
	
	public PartitionTableEntry getEntryForPartition(int partitionId) {
		return this.partitionEntries.get(partitionId);
	}
	
	public Map<Integer, PartitionTableEntry> getPartitionEntries() {
		return partitionEntries;
	}
	
	public boolean hasReplica() {
		return this.replicationFactor > 0;
	}
	
	public Partition getPartitionForKey(Object key) {
		//TODO
		return null;
	}
	
	public Node getPrimaryNodeForPartition(int partitionId) {
		//TODO
		return null;
	}
	
	public List<Node> getSecondaryNodesForPartition(int partitionId) {
		//TODO
		return null;
	}
	
	@Override
	public String toString() {	
		return String.format("Nodes: %d, entries: %d", this.getNodesSize(), this.getPartitionsSize());
	}
	
	//for debug
	public void printDebug() {
		System.out.printf("partition | primary | secondary%n");
		
		for (int i = 0; i < this.getPartitionsSize(); i ++) {
			PartitionTableEntry entry = this.partitionEntries.get(i);
			
			List<Integer> sec = new ArrayList<>();
			List<Node> secNodes = entry.getSecondaryNodes();
			for (Node secNode : secNodes) {
				sec.add(secNode.getId());
			} 
				
			System.out.printf("%9s | %7s | %s%n", entry.getPartitionId(), entry.getPrimaryNode().getId(), sec);
		}
		
		System.out.println();		
	}

	public void markNodeAsDeleted(Node node) {
		node.markDeleted();		
	}
}
