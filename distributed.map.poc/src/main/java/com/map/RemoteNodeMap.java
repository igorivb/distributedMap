package com.map;

import java.util.Map;

public interface RemoteNodeMap<K, V> extends Map<K, V> {

	V putLocal(K key, V value, int partitionId, NodeSection section);
	
	V removeLocal(Object key, int partitionId, NodeSection section);
	
	V getLocal(Object key, int partitionId, NodeSection section);
	
	/**
	 * Get map size.
	 * 
	 * Implementation notes:
	 *    looks only in primary partition.
	 */
	int sizeLocal();		
}
