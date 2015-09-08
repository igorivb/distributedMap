package com.map;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PartitionTable implements Serializable {

	//private static final Logger logger = LoggerFactory.getLogger(PartitionTable.class);
	
	private static final long serialVersionUID = -1799955999038909614L;
	
	
	private int replicationFactor;
	private int partitionsCount;
	
	private Map<Integer, PartitionTableEntry> partitionEntries;
	
	//nodes in cluster: all nodes, including removed
	private Map<Integer, NodeEntry> nodeEntries;
	
	//one of the nodes is coordinator
	private NodeEntry coordinator;
	
	public PartitionTable(int replicationFactor, int partitionsCount) {
		this.replicationFactor = replicationFactor;
		this.partitionsCount = partitionsCount;
		
		partitionEntries = new HashMap<>(partitionsCount);
				
		for (int i = 0; i < partitionsCount; i ++) { //create empty entries
			partitionEntries.put(i, new PartitionTableEntry(i, this));
		}
		
		nodeEntries = new HashMap<>();
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
	public List<NodeEntry> getNodeEntries() {
		return new ArrayList<>(this.nodeEntries.values());
	}
	
	public NodeEntry getNodeEntry(int nodeId) {
		return this.nodeEntries.get(nodeId);
	}
	
	public int getNodesSize() {
		return this.nodeEntries.size();		
	}
	
	public void addNodeEntry(NodeEntry newNode) {
		this.nodeEntries.put(newNode.getNodeId(), newNode);
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
	
	public int getPartitionForKey(Object key) {
		return key.hashCode() % this.getPartitionsSize();
	}
		
	public NodeEntry getPrimaryNodeForPartition(int partitionId) {
		return this.partitionEntries.get(partitionId).getPrimaryNode();				
	}
		
	public List<NodeEntry> getSecondaryNodesForPartition(int partitionId) {
		PartitionTableEntry entry = this.partitionEntries.get(partitionId);
		return entry.getSecondaryNodes();
	}
	
	public List<PartitionTableEntry> getNodesPrimaryPartitions(List<Integer> nodes) {
		List<PartitionTableEntry> res = new ArrayList<>();		
		for (PartitionTableEntry entry : this.getPartitionEntries().values()) {
			for (Integer nodeId : nodes) {
				if (entry.getPrimaryNode().getNodeId() == nodeId) {
					res.add(entry);
					break;
				}	
			}
		}			
		return res;
	}
	
	/*
	 * May return duplicates, because the same partition can be in multiple secondary nodes.
	 */
	public List<PartitionTableEntry> getNodesSecondaryPartitions(List<Integer> nodes) {
		List<PartitionTableEntry> res = new ArrayList<>();		
		for (PartitionTableEntry entry : this.getPartitionEntries().values()) {
			for (NodeEntry secNode : entry.getSecondaryNodes()) {
				boolean found = false;
				for (Integer nodeId : nodes) {
					if (secNode.getNodeId() == nodeId) {
						res.add(entry);
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

	public void markNodeAsDeleted(NodeEntry node) {
		node.markDeleted();		
	}
	
	/**
	 * Delete nodes from all tables and lists.
	 */
	public void deleteNodes(List<NodeEntry> deletedNodes) {		
		for (NodeEntry deletedNode : deletedNodes) {
			nodeEntries.remove(deletedNode.getNodeId()); 
		}		
	}
	
	public void deleteAll() {		
		LocalNodes.getInstance().getNodes().clear(); //TODO: delete nodes locally. Remove later.
		
		for (PartitionTableEntry entry : partitionEntries.values()) {
			entry.removePrimaryNode(entry.getPrimaryNode().getNodeId());
			
			for (NodeEntry n : entry.getSecondaryNodes()) {
				entry.removeSecondaryNode(n.getNodeId());
			}
		}		
		
		this.nodeEntries.clear();
	}
			
	public NodeEntry getCoordinator() {
		return coordinator;
	}

	public void setCoordinator(NodeEntry coordinator) {
		this.coordinator = coordinator;
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
			System.out.printf("%9s | %7s | %s%n", entry.getPartitionId(), entry.getPrimaryNode(), entry.getSecondaryNodes());
		}
		
		System.out.println();		
	}
			
}
