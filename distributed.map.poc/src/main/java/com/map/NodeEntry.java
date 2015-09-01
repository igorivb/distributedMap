package com.map;

import java.net.InetAddress;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

public class NodeEntry {
	
	private final int id;
	
	private final InetAddress address;
	
	private NodeStatus status;
	
	private PartitionTable pt;
	
	public NodeEntry(int id, InetAddress address) {
		this.id = id;
		this.address = address;
		this.status = NodeStatus.NORMAL;				
	}

	public int getId() {
		return id;
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
		return pt.getNodesPrimaryPartitions(Arrays.asList(this.id));
	}
	
	public List<PartitionTableEntry> getSecondaryPartitions() {
		return pt.getNodesSecondaryPartitions(Arrays.asList(this.id));
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
			return this.id == n.getId();					
		}		
		return false;
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + id;
		return result;
	}
	
	@Override
	public String toString() {	
		return String.valueOf(this.id);
	}
}
