package com.map;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;


public class Node {				
	
	private final int id;
	
	private final InetAddress address;
	
	private NodeStatus status; //TODO: do we need it?
	
	private PartitionTable pt;
	
	private List<Partition> primaryData = new ArrayList<>();
	
	private List<Partition> secondaryData = new ArrayList<>();
	
	private Map<String, NodeMap<?, ?>> maps = new HashMap<>();
	
	private List<RemoteNode> nodes = new ArrayList<>();
	
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
	
	public Partition getPartition(int partitionId, NodeSection section) {
		for (Partition p : getData(section)) {
			if (p.getId() == partitionId) {
				return p;
			}
		}
		return null;
	}
	
	public List<Partition> getPrimaryData() {
		return primaryData;
	}
	
	public List<Partition> getSecondaryData() {
		return secondaryData;
	}
	
	public boolean hasPartition(int partitionId, NodeSection section) {
		for (Partition p : getData(section)) {
			if (p.getId() == partitionId) {
				return true;
			}
		}
		return false;
	}
	
		
	//---------------- Client operations: end
	
	
	public void start() {
		//implement
	}
	
	private void discoveryNodes() {
		//implement
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

	public void markDeleted() { //TODO: do we need it?
		this.status = NodeStatus.DELETED;
	}

	public Partition createPartition(NodeSection section, int partitionId) {
		Partition part = new Partition(partitionId);
		getData(section).add(part);
		
		if (section == NodeSection.PRIMARY) {
			pt.getEntryForPartition(partitionId).setPrimaryNode(this.getId());
		} else {
			pt.getEntryForPartition(partitionId).addSecondaryNode(this.getId());	
		}
		
		return part;
	}

	public void deleteSecondaryPartition(int partitionId) {
		Iterator<Partition> iter = this.secondaryData.iterator();
		while (iter.hasNext()) {
			Partition part = iter.next();
			if (part.getId() == partitionId) {
				iter.remove();				
				
				pt.getEntryForPartition(partitionId).removeSecondaryNode(this.getId());				
				return;
			}
		}
		
		throw new RuntimeException(String.format(
			"Failed to deleted secondary partition from node, "
			+ "because partition doesn't exist. Node: %s, partition: %s", this.getId(), partitionId));		
	}

	public PartitionTable getPartitionTable() {
		return this.pt;
	}
	
	/*
	 * TODO: correctly create maps, now I always create Map<String, String>
	 */
	public Map<?, ?> getMap(String mapId) {		
		NodeMap<?, ?> map = maps.get(mapId);
		if (map == null) {
			map = new NodeMap<String, String>(mapId, this);
			maps.put(mapId, map);
		}
		return map;
	}
}
