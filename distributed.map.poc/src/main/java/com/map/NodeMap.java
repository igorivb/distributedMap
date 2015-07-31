package com.map;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.map.Node.NodeSection;

/**
 * TODO: 
 * 1. handle that nodes may go down during operations, partitions are not available etc.
 * 2. getPrimaryNodeForPartition, getSecondaryNodesForPartition - may return 'removed' nodes, but we need only normal
 */
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
	
	/*
	 * First put to primary and then to secondary nodes.
	 * 
	 * TODO: Put to secondary nodes synchronously now, but allow to put either synchronously or asynchronously. The same for remove.
	 */
	@Override
	public V put(K key, V value) {						
		//find partition where key should be located
		int partitionId = pt.getPartitionForKey(key);				
				
		logger.debug("Put Request: node: {}, partition: {}, key: '{}'", curNode, partitionId, key);
		
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
	
	/*
	 * First remove from primary and then from secondary nodes.
	 * 
	 * TODO: Remove from secondary nodes synchronously now, see 'put' node docs.
	 */
	@Override
	public V remove(Object key) {	
		int partitionId = pt.getPartitionForKey(key);	
		
		logger.debug("Remove Request: node: {}, partition: {}, key: '{}'", curNode, partitionId, key);
								
		Node primaryNode = pt.getPrimaryNodeForPartition(partitionId);
		logger.debug("Remove: primary node: {}", primaryNode);
		
		V res;
		if (curNode.getId() == primaryNode.getId()) { //local operation
			res = removeLocal(key, partitionId, NodeSection.PRIMARY);
		} else { //delegate to another node	
			res = getNodeMap(primaryNode, mapId).removeLocal(key, partitionId, NodeSection.PRIMARY);
		}
						
		//secondary nodes: wait for operations to complete
		List<Node> secNodes = pt.getSecondaryNodesForPartition(partitionId);
		logger.debug("Remove: secondary nodes: {}", secNodes);
		for (Node secNode : secNodes) {			
			getNodeMap(secNode, mapId).removeLocal(key, partitionId, NodeSection.SECONDARY);
		}			
		
		return res;
	}

	/*
	 * Try to get data from primary partition first, even if current node has key in its secondary section.
	 */
	@Override
	public V get(Object key) {
		int partitionId = pt.getPartitionForKey(key);	
		
		logger.debug("Get Request: node: {}, partition: {}, key: '{}'", curNode, partitionId, key);
								
		Node node = pt.getPrimaryNodeForPartition(partitionId); 
		NodeSection section = NodeSection.PRIMARY;
		if (node == null) {
			List<Node> secNodes = pt.getSecondaryNodesForPartition(partitionId);
			node = secNodes.get(0); //take first
			section = NodeSection.SECONDARY;
		}	
		
		logger.debug("Get key from node: {}, section: {}, key: '{}'", node, section, key);
		
		V res;
		if (node.getId() == this.curNode.getId()) { //local operation
			res = this.getLocal(key, partitionId, section);  
		} else {
			res = getNodeMap(node, mapId).getLocal(key, partitionId, section); 
		}
		return res;
	}
	
	/*
	 * Size is calculated on primary partitions.
	 */
	@Override
	public int size() {
		logger.debug("Size Request");
		
		int size = 0;
		for (Node node : pt.getNodes()) {
			size += getNodeMap(node, mapId).sizeLocal();
		}
		return size;
	}

	@Override
	public boolean isEmpty() {
		return this.size() == 0;
	}
	
	@SuppressWarnings("unchecked")
	private NodeMap<K, V> getNodeMap(Node node, String mapId) {
		return (NodeMap<K, V>) node.getMap(mapId);
	}
	
	private Map<K, V> getLocalPartitionMap(int partitionId, NodeSection section) {
		Partition part = curNode.getPartition(partitionId, section);
		@SuppressWarnings("unchecked")
		Map<K, V> map = (Map<K, V>) part.getMap(this.mapId);
		return map;
	}

	private int sizeLocal() {
		int size = 0;
		for (Partition part : curNode.getPrimaryData()) {
			Map<K, V> map = getLocalPartitionMap(part.getId(), NodeSection.PRIMARY);
			size += map.size();
		}
		return size;
	}
	
	private V putLocal(K key, V value, int partitionId, NodeSection section) {
		Map<K, V> map = getLocalPartitionMap(partitionId, section);
		return map.put(key, value);
	}
	
	private V getLocal(Object key, int partitionId, NodeSection section) {
		Map<K, V> map = getLocalPartitionMap(partitionId, section);
		return map.get(key);
	}
	
	private V removeLocal(Object key, int partitionId, NodeSection section) {
		Map<K, V> map = getLocalPartitionMap(partitionId, section);
		return map.remove(key);
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
