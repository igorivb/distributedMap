package com.map;

import java.io.Serializable;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.List;

public class NodeEntry implements Serializable {
	
	private static final long serialVersionUID = -2847324433652812586L;

	private final int nodeId;
	
	private final InetAddress address;
	
	private NodeStatus status;
	
	private PartitionTable pt;
	
	public NodeEntry(int nodeId, InetAddress address) {
		this.nodeId = nodeId;
		this.address = address;
		this.status = NodeStatus.NORMAL;				
	}

	public int getNodeId() {
		return nodeId;
	}

	public InetAddress getAddress() {
		return address;
	}

	public NodeStatus getStatus() {
		return status;
	}
	
	public void setPartitionTable(PartitionTable pt) {
		this.pt = pt;
	}
	
	public void markDeleted() {
		this.status = NodeStatus.DELETED;
	}
	
	public boolean isDeleted() {
		return this.status == NodeStatus.DELETED;
	}

	public List<PartitionTableEntry> getPrimaryPartitions() {
		return pt.getNodesPrimaryPartitions(Arrays.asList(this.nodeId));
	}
	
	public List<PartitionTableEntry> getSecondaryPartitions() {
		return pt.getNodesSecondaryPartitions(Arrays.asList(this.nodeId));
	}
	
	public int getPrimaryPartitionsCount() {
		return getPrimaryPartitions().size();
	}

	public int getSecondaryPartitionsCount() {
		return getSecondaryPartitions().size();
	}
	
	public List<PartitionTableEntry> getPartitions(NodeSection section) {
		return section == NodeSection.PRIMARY ? this.getPrimaryPartitions() : this.getSecondaryPartitions();
	}
	
	public int getPartitionsCount(NodeSection section) {
		return getPartitions(section).size();
	}
	
	public boolean hasPartition(int partitionId, NodeSection section) {
		for (PartitionTableEntry part : getPartitions(section)) {
			if (part.getPartitionId() == partitionId) {
				return true;
			}
		}
		return false;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}		
		if (obj instanceof NodeEntry) {
			NodeEntry n = (NodeEntry) obj;
			return this.nodeId == n.getNodeId();					
		}		
		return false;
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + nodeId;
		return result;
	}
	
	@Override
	public String toString() {	
		return String.valueOf(this.nodeId);
	}
}
