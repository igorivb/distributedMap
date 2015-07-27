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
		if (this.primaryNode != null) {
			throw new RuntimeException(
				String.format("Failed to set primary node for partition. Partition: %s, exists: %s, new: %s", 
				this.partitionId, this.primaryNode.getId(), node.getId()));
		}
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
	
	public void removePrimaryNode() {
		if (this.primaryNode == null) {
			throw new RuntimeException(
				String.format("Failed to remove primary node from partition because it is null. Partition: %s", 
				this.partitionId));
		}
		this.primaryNode = null;		
	}
	
	public void removeSecondaryNode(Node node) {
		if (!secondaryNodes.remove(node)) {
			throw new RuntimeException(
				String.format("Failed to remove secondary node from partition because it doesn't exist. Partition: %s, node: %s", 
				this.partitionId, node.getId()));
		}		
	}
}
