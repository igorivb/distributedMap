package com.map;

import java.util.Map;

/**
 * Remote interface for Node.
 *
 * TODO: make INode interface instead where Node and RemoteNodeImpl will extend it?
 * 
 * TODO: all methods here should throw some remote exception, e.g. IOException 
 */
public interface RemoteNode {

	int getId();

	/**
	 * Serialize PartitionTable and send it over the wire.
	 */
	void setPartitionTable(PartitionTable pt);
	
	/**
	 * Only for clients, not for other nodes, because they have their own tables.
	 * Expose it to clients to allow view what is going on in cluster.
	 * 
	 * Serialize PartitionTable and send it over the wire.
	 */
	PartitionTable getPartitionTable();
	
	
	void createPartition(NodeSection section, int partitionId);
	
	void removePartition(NodeSection section, int partitionId);		
		
	void copyPartition(NodeSection srcSection, int partitionId, int destNodeId, NodeSection destSection);

	/**
	 * Serialize Partition and send it over the wire.
	 */
	void addPartition(NodeSection section, Partition partition);	
	
	
	/**
	 * Get remote map which makes remote calls to nodes.
	 */
	Map<?, ?> getMap(String mapId);		
}
