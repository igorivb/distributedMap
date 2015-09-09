package com.map;

import java.net.InetAddress;
import java.util.Map;

/**
 * Local implementation of RemoteNode. Should be removed later.
 */
public class LocalRemoteNodeImpl implements RemoteNode {

	private final int nodeId;
	private final InetAddress address;
	
	/*
	 * Don't create this object with constructor.
	 */
	public LocalRemoteNodeImpl(int nodeId, InetAddress address) {
		this.nodeId = nodeId;
		this.address = address;
	}

	@Override
	public int getId() {
		return nodeId;
	}
	
	public InetAddress getAddress() {
		return address;
	}

	private Node getNode() {
		return LocalNodes.getInstance().getNodes().get(nodeId);
	}
	
	@Override
	public void setPartitionTable(PartitionTable pt) {
		getNode().setPartitionTable(Utils.copy(pt, PartitionTable.class));		
	}
	
	@Override
	public PartitionTable getPartitionTable() {
		PartitionTable pt = getNode().getPartitionTable();
		return Utils.copy(pt, PartitionTable.class);
	}

	@Override
	public void removePartition(NodeSection section, int partitionId) {
		getNode().removePartition(section, partitionId);	
	}

	@Override
	public void copyPartition(NodeSection srcSection, int partitionId, int destNodeId, NodeSection destSection) {
		getNode().copyPartition(srcSection, partitionId, destNodeId, destSection);
		
	}

	@Override
	public void addPartition(NodeSection section, Partition partition) {
		getNode().addPartition(Utils.copy(partition, Partition.class), section);		
	}

	@Override
	public void createPartition(NodeSection section, int partitionId) {
		getNode().createPartition(section, partitionId);
	}

	@Override
	public Map<?, ?> getMap(String mapId) {
		return new LocalRemoteNodeMapImpl<>(nodeId, mapId);
	}
	
	@Override
	public String toString() {	
		return String.valueOf(this.nodeId);
	}

	@Override
	public void movePartitionLocally(NodeSection srcSection, int partitionId, NodeSection destSection) {
		getNode().movePartitionLocally(srcSection, partitionId, destSection);		
	}

	@Override
	public void addNode(NodeEntry newNode) {
		getNode().addNode(newNode);		
	}

	@Override
	public void connect(NodeEntry newNode) {
		getNode().connect(newNode);	
	}
}
