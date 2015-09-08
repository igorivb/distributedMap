package com.map;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * Local implementation of remote map. Should be removed later.
 */
public class LocalRemoteNodeMapImpl<K, V> implements RemoteNodeMap<K, V> {

	private final int nodeId;
	private final String mapId;
	
	public LocalRemoteNodeMapImpl(int nodeId, String mapId) {
		this.nodeId = nodeId;
		this.mapId = mapId;
	}
	
	private Node getNode() {
		return LocalNodes.getInstance().getNodes().get(nodeId);
	}
	
	@SuppressWarnings("unchecked")
	private NodeMap<K, V> getMap() {
		return (NodeMap<K, V>) getNode().getMap(mapId);
	}
	
	@Override
	public V put(K key, V value) {
		return getMap().put(key, value);
	}

	@Override
	public V remove(Object key) {
		return getMap().remove(key);
	}
	
	@Override
	public int size() {
		return getMap().size();
	}

	@Override
	public boolean isEmpty() {
		return getMap().isEmpty();
	}
	
	@Override
	public V get(Object key) {
		return getMap().get(key);
	}

	@Override
	public V putLocal(K key, V value, int partitionId, NodeSection section) {
		return getMap().putLocal(key, value, partitionId, section);
	}
	
	@Override
	public V removeLocal(Object key, int partitionId, NodeSection section) {
		return getMap().removeLocal(key, partitionId, section);		
	}
	
	@Override
	public V getLocal(Object key, int partitionId, NodeSection section) {
		return getMap().getLocal(key, partitionId, section);
	}
	
	@Override
	public int sizeLocal() {
		return getMap().sizeLocal();
	}
	
	@Override
	public boolean containsKey(Object key) {
		///implement
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
}
