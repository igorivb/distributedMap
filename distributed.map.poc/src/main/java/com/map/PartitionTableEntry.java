package com.map;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Contains information for partition: primary node, secondary nodes.
 * It returns nodes information even if nodes are marked as deleted.
 */
public class PartitionTableEntry implements Serializable {

	private static final long serialVersionUID = -6912456390999370785L;
	
	private static final Logger logger = LoggerFactory.getLogger(PartitionTableEntry.class);		
	

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
	
	/**
	 * Return node even if it is marked as deleted.
	 */
	public NodeEntry getPrimaryNode() {
		return primaryNode != null ? pt.getNodeEntry(primaryNode, true) : null;
	}
	
	public void addSecondaryNode(Integer node) {	
		logger.debug("Add secondary node. Partition id: {}, node: {}", partitionId, node);
		
		if (secondaryNodes.contains(node)) { //check there are no dups
			throw new RuntimeException(
				String.format("Failed to add secondary node for partition because it already exists. Partition: %s, node: %s", 
				this.partitionId, node));
		}		
		secondaryNodes.add(node);		
	}
	
	/**
	 * Return all nodes including deleted.
	 */
	public List<NodeEntry> getSecondaryNodes() {
		List<NodeEntry> res = new ArrayList<>();
		for (Integer secNode : secondaryNodes) {
			NodeEntry nodeEntry = pt.getNodeEntry(secNode, true);
			if (nodeEntry == null) {
				throw new RuntimeException(String.format(
					"Failed to get secondary node. Partition: %s, secNode: %s", this.partitionId, secNode
				));
			}
			res.add(nodeEntry);
		}
		return res;
	}
	
	public void removePrimaryNode(int node) {
		/*
		 * Remove entry only in case if nodes are the same.
		 * E.g. they may not be the same if partition was previously copied to another node.
		 */
		if (this.primaryNode == node) {
			logger.debug("Remove primary node. Partition id: {}, node: {}", partitionId, node);
			
			this.primaryNode = null;			
		}		
	}
	
	public void removeSecondaryNode(Integer node) {
		logger.debug("Remove secondary node. Partition id: {}, node: {}", partitionId, node);
		
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
	
	
	public String toStringFormatted() {		
		return String.format(
			"part: %s, primary: %s, secondary: %s", 
			this.partitionId, this.primaryNode == null ? "<null>" : this.primaryNode, this.secondaryNodes);
	}
	
	@Override
	public String toString() {		
		return String.valueOf(this.partitionId);
	}

}
