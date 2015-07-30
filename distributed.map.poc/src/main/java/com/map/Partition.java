package com.map;

import java.util.HashMap;
import java.util.Map;

public class Partition {

	private final int id;
	
	//internal data
	private Map<String, Map<?, ?>> maps = new HashMap<>();
	
	//TODO: add status ?
	
	public Partition(int id) {
		this.id = id;	
	}

	public int getId() {
		return id;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}		
		if (obj instanceof Partition) {
			Partition n = (Partition) obj;
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

	//TODO: correctly create maps
	public Map<?, ?> getMap(String mapId) {		
		Map<?, ?> map = maps.get(mapId);
		if (map == null) {
			map = new HashMap<String, String>();
			maps.put(mapId, map);
		}
		return map;
	}
}
