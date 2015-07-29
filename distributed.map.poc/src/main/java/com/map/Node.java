package com.map;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


public class Node {

	enum NodeStatus { NORMAL, DELETED }
	
	//Use enum instead of boolean to indicate primary and secondary for better code readability.
	enum NodeSection { PRIMARY, SECONDARY }
	
	
	private final int id;
	
	private final InetAddress address;
	
	private NodeStatus status;
	
	private PartitionTable pt;
	
	private List<Partition> primaryData = new ArrayList<>();
	
	private List<Partition> secondaryData = new ArrayList<>();
	
	
	public Node(int id, InetAddress address) {
		this.id = id;
		this.address = address;
		this.status = NodeStatus.NORMAL;
	}
	
	public int getId() {
		return id;
	}
	
	public void setPartitionTable(PartitionTable pt) {
		this.pt = pt;
	}
	
	public boolean isDeleted() {
		return this.status == NodeStatus.DELETED;
	}
	
//	public int getPrimaryPartitionsCount() {
//		return this.primaryData.size();
//	}
//	
//	public int getSecondaryPartitionsCount() {
//		return this.secondaryData.size();
//	}
	
	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}		
		if (obj instanceof Node) {
			Node n = (Node) obj;
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
		
	public String toStringWithSizes() {	
		return String.format("%d: {primary: %s, secondary: %s}", id, this.primaryData.size(), this.secondaryData.size());
	}
	
	public List<Partition> getData(NodeSection section) {
		return section == NodeSection.PRIMARY ? this.primaryData : this.secondaryData;
	}
	
	public List<Partition> getPrimaryData() {
		return primaryData;
	}
	
	public List<Partition> getSecondaryData() {
		return secondaryData;
	}
	
	//---------------- Client operations: start
	
	
	public int size() {
		//TODO
		return 0;
	}
	
	public Object get(Object key) {
		//TODO
		return null;
	}
	
	public Object put(Object key, Object value) {
		//TODO
		return null;		
	}
	
	public Object remove(Object key) {
		//TODO
		return null;
	}
	
		
	//---------------- Client operations: end
	
	
	public void start() {
		//TODO
	}
	
	private void discoveryNodes() {
		//TODO
	}

	public void addPartition(NodeSection section, Partition partition) {				
		if (section == NodeSection.PRIMARY) {
			this.primaryData.add(partition);
		} else {
			this.secondaryData.add(partition);	
		}				
	}
	
	public void removePartition(NodeSection section, Partition partition) {		
		if (section == NodeSection.PRIMARY) {
			this.primaryData.remove(partition);
		} else {
			this.secondaryData.remove(partition);
		}				
	}

	public void markDeleted() {
		this.status = NodeStatus.DELETED;
	}

	public Partition createPartition(NodeSection section, int partitionId) {
		Partition part = new Partition(partitionId);
		getData(section).add(part);
		
		if (section == NodeSection.PRIMARY) {
			pt.getEntryForPartition(partitionId).setPrimaryNode(this);
		} else {
			pt.getEntryForPartition(partitionId).addSecondaryNode(this);	
		}
		
		return part;
	}

	public void deleteSecondaryPartition(int partitionId) {
		Iterator<Partition> iter = this.secondaryData.iterator();
		while (iter.hasNext()) {
			Partition part = iter.next();
			if (part.getId() == partitionId) {
				iter.remove();				
				
				pt.getEntryForPartition(partitionId).removeSecondaryNode(this);				
				return;
			}
		}
		
		throw new RuntimeException(String.format(
			"Failed to deleted secondary partition from node, "
			+ "because partition doesn't exist. Node: %s, partition: %s", this.getId(), partitionId));		
	}
}
