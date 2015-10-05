package com.map;

import java.util.Map;

/**
 * Common interface for local and remote nodes.
 */
public interface INode {
	
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

	void movePartitionLocally(NodeSection srcSection, int partitionId, NodeSection destSection);
	
	/**
	 * Serialize Partition and send it over the wire.
	 */
	void addPartition(NodeSection section, Partition partition);	
	
	
	void connect(NodeEntry nodeEntry);	
	
	void addNode(NodeEntry newNode);
	
	/**
	 * Get remote map which makes remote calls to nodes.
	 */
	Map<?, ?> getMap(String mapId);	
}
