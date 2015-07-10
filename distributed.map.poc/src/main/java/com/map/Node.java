package com.map;

import java.util.List;

public class Node<K, V> {

	int id;
	
	List<Node<K, V>> nodes;
	
	PartitionTable pt;
	
	List<Partition<K, V>> primaryData;
	
	List<Partition<K, V>> secondaryData;
	
	Scheduler scheduler;
	
	
	//---------------- Client operations: start
	
	
	public int size() {
		//TODO
		return 0;
	}
	
	public V get(Object key) {
		//TODO
		return null;
	}
	
	public V put(K key, V value) {
		//TODO
		return null;		
	}
	
	public V remove(Object key) {
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
}
