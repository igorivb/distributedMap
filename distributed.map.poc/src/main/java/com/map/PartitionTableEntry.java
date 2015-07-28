package com.map;

import java.util.ArrayList;
import java.util.List;

public class PartitionTableEntry {

	private final int partitionId;
	
	private Node primaryNode;
	
	private List<Node> secondaryNodes = new ArrayList<>();

	public PartitionTableEntry(int partitionId) {		
		this.partitionId = partitionId;
	}
	
	public int getPartitionId() {
		return this.partitionId;
	}
	
	public void setPrimaryNode(Node node) {
		this.primaryNode = node;		
	}
	
	public Node getPrimaryNode() {
		return primaryNode;
	}
	
	public void addSecondaryNode(Node node) {		
		if (secondaryNodes.contains(node)) { //check there are no dups
			throw new RuntimeException(
				String.format("Failed to add secondary node for partition because it already exists. Partition: %s, node: %s", 
				this.partitionId, node.getId()));
		}		
		secondaryNodes.add(node);		
	}
	
	public List<Node> getSecondaryNodes() {
		return secondaryNodes;
	}
	
	public void removePrimaryNode(Node node) {
		/*
		 * Remove entry only in case if nodes are the same.
		 * E.g. they may not be the same if partition was previously copied to another node.
		 */
		if (this.primaryNode.equals(node)) {
			this.primaryNode = null;			
		}		
	}
	
	public void removeSecondaryNode(Node node) {
		if (!secondaryNodes.remove(node)) { //check if it existed
			throw new RuntimeException(
				String.format("Failed to remove secondary node from partition because it doesn't exist. Partition: %s, node: %s", 
				this.partitionId, node.getId()));
		}		
	}
	
	@Override
	public String toString() {
		List<Integer> sec = new ArrayList<>();
		List<Node> secNodes = this.secondaryNodes;
		for (Node secNode : secNodes) {
			sec.add(secNode.getId());
		} 
		
		return String.format(
			"%s, primary: %s, secondary: %s", 
			this.partitionId, this.primaryNode == null ? "<null>" : this.primaryNode.getId(), sec);
	}
}
