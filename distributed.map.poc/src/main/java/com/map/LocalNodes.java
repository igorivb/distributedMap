package com.map;

import java.util.HashMap;
import java.util.Map;

/**
 * List of nodes in cluster. Need it for local nodes discovery.
 * It will not be used, when we start discovering nodes with TCP, UDP.
 */
public class LocalNodes {

	private static LocalNodes instance = new LocalNodes();
	
	
	//need it only for node's cluster discovery operation
	
	private Map<Integer, Node> nodes = new HashMap<>();
	
	public static LocalNodes getInstance() {
		return instance;
	}
	
	//TODO: check that is used in limited places, i.e. only for nodes discovery
	public Map<Integer, Node> getNodes() {
		return nodes;
	}			
}
