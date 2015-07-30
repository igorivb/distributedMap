package com.map;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
	public List<Node> getNodes() {
		return new ArrayList<>(this.nodes.values());
	}
	
	public Node getNode(int nodeId) {
		return this.nodes.get(nodeId);
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
		return this.partitionEntries.get(partitionId).getPrimaryNode();
	}
	
	public List<Node> getSecondaryNodesForPartition(int partitionId) {
		PartitionTableEntry entry = this.partitionEntries.get(partitionId);
		return new ArrayList<>(entry.getSecondaryNodes());
	}
	
	public List<Integer> getNodesPrimaryPartitions(List<Integer> nodes) {
		List<Integer> res = new ArrayList<>();		
		for (PartitionTableEntry entry : this.getPartitionEntries().values()) {
			for (Integer nodeId : nodes) {
				if (entry.getPrimaryNode().getId() == nodeId) {
					res.add(entry.getPartitionId());
					break;
				}	
			}
		}			
		return res;
	}
	
	public Set<Integer> getNodesSecondaryPartitions(List<Integer> nodes) {
		Set<Integer> res = new HashSet<>();		
		for (PartitionTableEntry entry : this.getPartitionEntries().values()) {
			for (Node secNode : entry.getSecondaryNodes()) {
				boolean found = false;
				for (Integer nodeId : nodes) {
					if (secNode.getId() == nodeId) {
						res.add(entry.getPartitionId());
						found = true;
						break;
					}	
				}	
				if (found) {
					break;
				}
			}
		}			
		return res;
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
				
			System.out.printf("%9s | %7s | %s%n", entry.getPartitionId(), entry.getPrimaryNode(), sec);
		}
		
		System.out.println();		
	}

	public void markNodeAsDeleted(Node node) {
		node.markDeleted();		
	}

	/**
	 * Delete nodes from all tables and lists.
	 */
	public void deleteNodes(List<Node> deletedNodes) {		
		for (Node deletedNode : deletedNodes) {
			nodes.remove(deletedNode.getId()); 
		}		
	}
	
	public void deleteAll() {
		this.nodes.clear();
		
		for (PartitionTableEntry entry : partitionEntries.values()) {
			entry.removePrimaryNode(entry.getPrimaryNode());
			for (Node n : entry.getSecondaryNodes()) {
				entry.removeSecondaryNode(n);
			}
		}		
	}
}
