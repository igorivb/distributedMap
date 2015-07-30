package com.map;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.map.Node.NodeSection;

public class NodeMap<K, V> implements Map<K, V> {

	private static final Logger logger = LoggerFactory.getLogger(NodeMap.class);
	
	
	private final String mapId;
	
	private final Node curNode;
	private final PartitionTable pt;

	public NodeMap(String mapId, Node node) {
		this.mapId = mapId;
		
		this.curNode = node;
		this.pt = node.getPartitionTable();
	}
	
	@Override
	public V put(K key, V value) {				
		//find partition where key should be located
		int partitionId = pt.getPartitionForKey(key);				
				
		logger.debug("Put: node: {}, partition: {}, key: {}", curNode, partitionId, key);
		
		//should update both primary and secondary partitions
		
		Node primaryNode = pt.getPrimaryNodeForPartition(partitionId);				
		logger.debug("Put: primary node: {}", primaryNode);
		
		V res;
		if (curNode.getId() == primaryNode.getId()) { //local operation		
			res = putLocal(key, value, partitionId, NodeSection.PRIMARY);
		} else { //delegate to another node			
			res = getNodeMap(primaryNode, mapId).putLocal(key, value, partitionId, NodeSection.PRIMARY);
		}	

		//secondary nodes: wait for operations to complete
		List<Node> secNodes = pt.getSecondaryNodesForPartition(partitionId);
		logger.debug("Put: secondary nodes: {}", secNodes);
		for (Node secNode : secNodes) {			
			getNodeMap(secNode, mapId).putLocal(key, value, partitionId, NodeSection.SECONDARY);
		}
		
		return res;
	}

	@Override
	public V get(Object key) {
//		int partitionId = pt.getPartitionForKey(key);	
//		
//		logger.debug("Get: node: {}, partition: {}, key: {}", curNode, partitionId, key);
//		
//		Node primaryNode = pt.getPrimaryNodeForPartition(partitionId);	
//		if (primaryNode != null) {
//			
//		} else {
//			pt.getSecondaryNodesForPartition(partitionId);
//		}
		return null;
	}
	
	@SuppressWarnings("unchecked")
	private NodeMap<K, V> getNodeMap(Node node, String mapId) {
		return (NodeMap<K, V>) node.getMap(mapId);
	}
		
	private V putLocal(K key, V value, int partitionId, NodeSection section) {
		Partition part = curNode.getPartition(partitionId, section);
		@SuppressWarnings("unchecked")
		Map<K, V> map = (Map<K, V>) part.getMap(this.mapId);
		return map.put(key, value);
	}

	@Override
	public int size() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public boolean isEmpty() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean containsKey(Object key) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean containsValue(Object value) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public V remove(Object key) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void putAll(Map<? extends K, ? extends V> m) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void clear() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Set<K> keySet() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Collection<V> values() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Set<java.util.Map.Entry<K, V>> entrySet() {
		// TODO Auto-generated method stub
		return null;
	}

}
