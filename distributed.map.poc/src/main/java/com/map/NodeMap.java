package com.map;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Server side implementation of Map. It is used directly by Node.
 * Clients and other nodes can't call it directly.
 * 
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
		NodeEntry primaryNode = pt.getPrimaryNodeForPartition(partitionId);
		List<NodeEntry> secNodes = pt.getSecondaryNodesForPartition(partitionId);		
		info("Put Request: key: '{}', partition: {}, primary node: {}, secondary nodes: {}", key, partitionId, primaryNode, secNodes);
								
		V res;
		if (curNode.getId() == primaryNode.getNodeId()) { //local operation		
			res = putLocal(key, value, partitionId, NodeSection.PRIMARY);
		} else { //delegate to another node			
			res = getRemoteNodeMap(primaryNode, mapId).putLocal(key, value, partitionId, NodeSection.PRIMARY);
		}	

		//secondary nodes: wait for operations to complete		
		for (NodeEntry secNode : secNodes) {			
			getRemoteNodeMap(secNode, mapId).putLocal(key, value, partitionId, NodeSection.SECONDARY);
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
		
		info("Remove Request: key: '{}', partition: {}", key, partitionId);
								
		NodeEntry primaryNode = pt.getPrimaryNodeForPartition(partitionId);
		debug("Remove: primary node: {}", primaryNode);
		
		V res;
		if (curNode.getId() == primaryNode.getNodeId()) { //local operation
			res = removeLocal(key, partitionId, NodeSection.PRIMARY);
		} else { //delegate to another node	
			res = getRemoteNodeMap(primaryNode, mapId).removeLocal(key, partitionId, NodeSection.PRIMARY);
		}
						
		//secondary nodes: wait for operations to complete
		List<NodeEntry> secNodes = pt.getSecondaryNodesForPartition(partitionId);
		debug("Remove: secondary nodes: {}", secNodes);
		for (NodeEntry secNode : secNodes) {			
			getRemoteNodeMap(secNode, mapId).removeLocal(key, partitionId, NodeSection.SECONDARY);
		}			
		
		return res;
	}

	/*
	 * Try to get data from primary partition first, even if current node has key in its secondary section.
	 */
	@Override
	public V get(Object key) {
		int partitionId = pt.getPartitionForKey(key);	
		
		info("Get Request: key: '{}', partition: {}", key, partitionId);
								
		NodeEntry node = pt.getPrimaryNodeForPartition(partitionId); 
		NodeSection section = NodeSection.PRIMARY;
		if (node == null) {
			List<NodeEntry> secNodes = pt.getSecondaryNodesForPartition(partitionId);
			node = secNodes.get(0); //take first
			section = NodeSection.SECONDARY;
		}	
		
		debug("Get key from node: {}, section: {}, key: '{}'", node, section, key);
		
		V res;
		if (node.getNodeId() == this.curNode.getId()) { //local operation
			res = this.getLocal(key, partitionId, section);  
		} else {
			res = getRemoteNodeMap(node, mapId).getLocal(key, partitionId, section); 
		}
		return res;
	}
	
	/*
	 * Size is calculated on primary partitions.
	 */
	@Override
	public int size() {
		info("Size Request");
		
		int size = 0;
		for (NodeEntry node : pt.getNodeEntries()) {
			if (node.getNodeId() == this.curNode.getId()) {
				size += sizeLocal();
			} else {
				size += getRemoteNodeMap(node, mapId).sizeLocal();
			}
		}
		return size;
	}

	@Override
	public boolean isEmpty() {
		info("isEmpty Request");
		
		return this.size() == 0;
	}
	
	@SuppressWarnings("unchecked")
	private RemoteNodeMap<K, V> getRemoteNodeMap(NodeEntry nodeEntry, String mapId) {
		RemoteNode remoteNode = this.curNode.getRemoteNode(nodeEntry.getNodeId());
		return (RemoteNodeMap<K, V>) remoteNode.getMap(mapId);
	}
		
	private Map<K, V> getLocalPartitionMap(int partitionId, NodeSection section) {
		Partition part = curNode.getPartition(partitionId, section);
		@SuppressWarnings("unchecked")
		Map<K, V> map = (Map<K, V>) part.getMap(this.mapId);
		return map;
	}

	public int sizeLocal() {
		debug("Size Local Request");
		
		int size = 0;
		for (Partition part : curNode.getPrimaryData()) {
			@SuppressWarnings("unchecked")
			Map<K, V> map = (Map<K, V>) part.getMap(this.mapId);
			size += map.size();
		}
		return size;
	}
	
	public V putLocal(K key, V value, int partitionId, NodeSection section) {
		debug("Put Local Request");
		
		Map<K, V> map = getLocalPartitionMap(partitionId, section);
		return map.put(key, value);
	}
	
	public V getLocal(Object key, int partitionId, NodeSection section) {
		debug("Get Local Request");
		
		Map<K, V> map = getLocalPartitionMap(partitionId, section);
		return map.get(key);
	}
	
	public V removeLocal(Object key, int partitionId, NodeSection section) {
		debug("Remove Local Request");
		
		Map<K, V> map = getLocalPartitionMap(partitionId, section);
		return map.remove(key);
	}

	@Override
	public boolean containsKey(Object key) {
		//implement
		return false;
	}

	@Override
	public boolean containsValue(Object value) {
		//implement
		return false;
	}

	@Override
	public void putAll(Map<? extends K, ? extends V> m) {
		//implement		
	}

	@Override
	public void clear() {
		//implement
		
	}

	@Override
	public Set<K> keySet() {
		//implement
		return null;
	}

	@Override
	public Collection<V> values() {
		//implement
		return null;
	}

	@Override
	public Set<java.util.Map.Entry<K, V>> entrySet() {
		//implement
		return null;
	}

	private String logPrefix() {
		return "Node: " + this.curNode.getId() + ", map: " + mapId + ". ";
	}
	private void info(String msg, Object... arguments) {
		logger.info(logPrefix() + msg, arguments);
	}
	
	private void debug(String msg, Object... arguments) {
		logger.debug(logPrefix() + msg, arguments);
	}
	
//	private void warn(String msg, Object... arguments) {
//		logger.warn(logPrefix() + msg, arguments);
//	}
}
