package com.map;

import java.util.ArrayList;
import java.util.List;

public class Node {

	private int id;
	
	List<Node> nodes;
	
	PartitionTable pt;
	
	private List<Partition> primaryData = new ArrayList<>();
	
	private List<Partition> secondaryData = new ArrayList<>();
	
	Scheduler scheduler;
	
	public Node(int id) {
		this.id = id;
	}
	
	public int getId() {
		return id;
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
	
	public List<Partition> getData(boolean isPrimary) {
		return isPrimary ? this.primaryData : this.secondaryData;
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
	
	
	public void start(MapConfig conf) {
		//TODO
	}
	
	private void discoveryNodes() {
		//TODO
	}

	public void addPrimaryPartition(Partition part) {
		this.primaryData.add(part);
		
	}
}
