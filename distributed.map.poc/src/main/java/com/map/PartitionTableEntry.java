package com.map;

import java.util.ArrayList;
import java.util.List;

public class PartitionTableEntry {

	private final int partitionId;
		
	private final PartitionTable pt;
	
	private Integer primaryNode;
	
	private List<Integer> secondaryNodes = new ArrayList<>();

	public PartitionTableEntry(int partitionId, PartitionTable pt) {		
		this.partitionId = partitionId;
		this.pt = pt;
	}
	
	public int getPartitionId() {
		return this.partitionId;
	}
	
	public void setPrimaryNode(Integer node) {
		this.primaryNode = node;		
	}
	
	public NodeEntry getPrimaryNode() {
		return primaryNode != null ? pt.getNodeEntry(primaryNode) : null;
	}
	
	public void addSecondaryNode(Integer node) {		
		if (secondaryNodes.contains(node)) { //check there are no dups
			throw new RuntimeException(
				String.format("Failed to add secondary node for partition because it already exists. Partition: %s, node: %s", 
				this.partitionId, node));
		}		
		secondaryNodes.add(node);		
	}
	
	public List<NodeEntry> getSecondaryNodes() {
		List<NodeEntry> res = new ArrayList<>();
		for (Integer secNode : secondaryNodes) {
			res.add(pt.getNodeEntry(secNode));
		}
		return res;
	}
	
	public void removePrimaryNode(int node) {
		/*
		 * Remove entry only in case if nodes are the same.
		 * E.g. they may not be the same if partition was previously copied to another node.
		 */
		if (this.primaryNode == node) {
			this.primaryNode = null;			
		}		
	}
	
	public void removeSecondaryNode(Integer node) {
		if (!secondaryNodes.remove(node)) { //check if it existed
			throw new RuntimeException(
				String.format("Failed to remove secondary node from partition because it doesn't exist. Partition: %s, node: %s", 
				this.partitionId, node));
		}		
	}
	
	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}		
		if (obj instanceof PartitionTableEntry) {
			PartitionTableEntry n = (PartitionTableEntry) obj;
			return this.partitionId == n.getPartitionId();					
		}		
		return false;
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + partitionId;
		return result;
	}
	
	@Override
	public String toString() {		
		return String.format(
			"%s, primary: %s, secondary: %s", 
			this.partitionId, this.primaryNode == null ? "<null>" : this.primaryNode, this.secondaryNodes);
	}

}
