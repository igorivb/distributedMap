package com.map;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Node /*implements RemoteNode*/ {				
	
	private static final Logger logger = LoggerFactory.getLogger(Node.class);
	
	
	private final int id;
	
	private final InetAddress address;
	
	private final Configuration config;
	
	private PartitionTable pt; //each node has its own copy of partition table
	
	private List<Partition> primaryData = new ArrayList<>();
	
	private List<Partition> secondaryData = new ArrayList<>();
	
	private Map<String, NodeMap<?, ?>> maps = new HashMap<>();
	
	/*
	 * List of remote nodes.
	 * Note that it doesn't contain current node.
	 */
	private Map<Integer, RemoteNode> remoteNodes = new HashMap<>(); //list of remote nodes
	
	/*
	 * Node comparators. Default sorting: desc
	 */
	static class NodesComparator implements Comparator<NodeEntry> {
		NodeSection nodeSection;
		public NodesComparator(NodeSection nodeSection) {
			this.nodeSection = nodeSection;
		}
		@Override
		public int compare(NodeEntry o1, NodeEntry o2) {
			int n1 = o1.getPartitionsCount(nodeSection);
			int n2 = o2.getPartitionsCount(nodeSection);
			return n1 == n2 ? 0 : (n1 < n2 ? 1 : -1);
		}		
	}	
	static class ReversedNodesComparator implements Comparator<NodeEntry> {
		Comparator<NodeEntry> cmp;
		public ReversedNodesComparator(Comparator<NodeEntry> cmp) {
			this.cmp = cmp;
		}
		@Override
		public int compare(NodeEntry o1, NodeEntry o2) {
			return cmp.compare(o2, o1);
		}		
	}
	//desc
	private Comparator<NodeEntry> primaryNodesDescCmp = new NodesComparator(NodeSection.PRIMARY);
	private Comparator<NodeEntry> secondaryNodesDescCmp = new NodesComparator(NodeSection.SECONDARY);
	//asc
	private Comparator<NodeEntry> primaryNodesAscCmp = new ReversedNodesComparator(primaryNodesDescCmp);
	private Comparator<NodeEntry> secondaryNodesAscCmp = new ReversedNodesComparator(secondaryNodesDescCmp);
	
	
	public Node(int id, InetAddress address, Configuration config) {
		this.id = id;
		this.address = address;
		this.config = config;			
	}
	
	//RemoteNode
	//@Override
	public int getId() {
		return id;
	}
	
	//RemoteNode
	//@Override
	public void setPartitionTable(PartitionTable pt) {
		trace("setPartitionTable");
		this.pt = pt;
	}
	
	//RemoteNode
	//@Override
	public void removePartition(NodeSection section, int partitionId) {
		Partition part = findPartition(section, partitionId);
		
		if (section == NodeSection.PRIMARY) {
			this.primaryData.remove(part);
		} else {
			this.secondaryData.remove(part);
		}
	}
	
	//RemoteNode
	//@Override
	public void copyPartition(NodeSection srcSection, int partitionId, int destNodeId, NodeSection destSection) {
		Partition part = findPartition(srcSection, partitionId);
		
		RemoteNode remote = getRemoteNode(destNodeId);		
		remote.addPartition(destSection, part);
	}
	
	//RemoteNode
	//@Override
	public void movePartitionLocally(NodeSection srcSection, int partitionId, NodeSection destSection) {
		Partition part = findPartition(srcSection, partitionId);
		getData(srcSection).remove(part);
		getData(destSection).add(part);		
	}	
	
	//throw Exception if it can't find partition
	private Partition findPartition(NodeSection section, int partitionId) {
		Partition part = null;
		for (Partition p : getData(section)) { //find partition
			if (p.getId() == partitionId) {
				part = p;
				break;
			}
		}
		if (part == null) {
			throw new RuntimeException(
				String.format("Failed to find partition in node. Node: %s, section: %s, partition: %s", this.id, section, partitionId));
		}
		return part;
	} 
	
	//RemoteNode
	//@Override
	public void addPartition(Partition partition, NodeSection section) {
		getData(section).add(partition);
	}
	
	//RemoteNode
	//@Override
	public Partition createPartition(NodeSection section, int partitionId) {
		info("Creating partition in node, node: {}, partition: {}, type: {}", this.id, partitionId, section);
		
		Partition part = new Partition(partitionId);
		getData(section).add(part);		
		
		return part;
	}
	
	/*
	 * RemoteNode
	 * @Override
	 * 
	 * TODO: correctly create maps, now I always create Map<String, String>
	 */
	public Map<?, ?> getMap(String mapId) {		
		NodeMap<?, ?> map = maps.get(mapId);
		if (map == null) {
			map = new NodeMap<String, String>(mapId, this);
			maps.put(mapId, map);
		}
		return map;
	}
	
	
	public void connect(NodeEntry newNode) {
		info("Connect to node: {}", newNode);
		
		//make connection between this node and remote one
		this.remoteNodes.put(newNode.getNodeId(), Node.createRemoteNode(newNode));
	}
	
	/**
	 * //RemoteNode
	 * //@Override
	 * 
	 * Add new node to cluster. Called by newNode during cluster discovery phase.
	 * 
	 * TODO: 
	 * 1. allows to add 1 node at a time. 
	 * If needed improve later to allow process several nodes at a time.
	 * 
	 * 2. make changes, if 'remove' occurred during 'add' operation
	 * e.g. some partitions may be available only in secondary or lost at all. 
	 * Is seems it should be handled by 'remove' and 'add' operation should be stopped.		
	 */
	public void addNode(NodeEntry newNode) {
		info("Adding node: {}", newNode);						
		
		if (this.isCoordinator()) {
			doAddNode(newNode);
		}
	}
	
	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}		
		if (obj instanceof Node) {
			Node n = (Node) obj;
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
		
//	private String toStringWithSizes() {	
//		return String.format("%d: {primary: %s, secondary: %s}", id, this.primaryData.size(), this.secondaryData.size());
//	}
	
	public List<Partition> getData(NodeSection section) {
		return section == NodeSection.PRIMARY ? this.primaryData : this.secondaryData;
	}
	
	public Partition getPartition(int partitionId, NodeSection section) {
		for (Partition p : getData(section)) {
			if (p.getId() == partitionId) {
				return p;
			}
		}
		return null;
	}
	
	public List<Partition> getPrimaryData() {
		return primaryData;
	}
	
	public List<Partition> getSecondaryData() {
		return secondaryData;
	}
	
	public boolean hasPartition(int partitionId, NodeSection section) {
		for (Partition p : getData(section)) {
			if (p.getId() == partitionId) {
				return true;
			}
		}
		return false;
	}
	
		
	//---------------- Client operations: end
	
	/**
	 * Starting point of node: read configuration, setup.
	 */
	public void init() {
		info("Starting new node");				
		
		this.discoveryNodes();
	}
	
	/**
	 * Discovery nodes in cluster. Should support different strategies, e.g. TCP, UDP.
	 * 
	 * At this stage we don't have PartitionTable and remote nodes.
	 * 
	 * TODO: Till we implement network communication between nodes, get nodes list from some in-memory storage.
	 */
	private void discoveryNodes() {
		info("Discovering cluster");				
		
		this.localNodesDiscovery();				
	}
	
	private void localNodesDiscovery() {				
		NodeEntry nodeEntry = new NodeEntry(id, address);

		Map<Integer, Node> nodes = new HashMap<>(LocalNodes.getInstance().getNodes()); //copy list so we are not affected by any changes
		boolean isClusterEmpty = nodes.isEmpty();
		
		LocalNodes.getInstance().getNodes().put(this.id, this); //add current node to nodes list
		
		if (!isClusterEmpty) {
			info("Found existing cluster");
			
			for (Node node : nodes.values()) { //make connection between newNode and existing nodes
				RemoteNode remoteNode = Node.createRemoteNode(node.getId(), node.getAddress());
				this.remoteNodes.put(remoteNode.getId(), remoteNode);
				
				remoteNode.connect(nodeEntry);
			}
			
			for (RemoteNode remoteNode : this.remoteNodes.values()) {								
				remoteNode.addNode(nodeEntry);									
			}	
		} else { //there are no nodes: cluster is empty
			initCluster(nodeEntry);
		}
	}

	public void addPartition(NodeSection section, Partition partition) {				
		if (section == NodeSection.PRIMARY) {
			this.primaryData.add(partition);
		} else {
			this.secondaryData.add(partition);	
		}				
	}
	
	private boolean isCoordinator() {		
		return this.id == this.pt.getCoordinator().getNodeId();
	}
	
	//called on coordinator
	private void doAddNode(NodeEntry newNode) {	
		boolean isClusterFull = pt.getNodesSize(false) == pt.getPartitionsSize();		
		boolean isBalanced = pt.getNodesSize(false) >= pt.getReplicationFactor() + 1; 
				
		if (isClusterFull) { //TODO: remove condition when we can expand cluster if it is full
			clusterFull(newNode);
		} else {
			debug("Is balanced: {}", isBalanced);
			
			//work with partition tables
			pt.addNodeEntry(newNode); //update own PartitionTable
			/*
			 * Update PartitionTable in all nodes, including newNode.
			 * 
			 * TODO: Probably when doing update of partition table in existing nodes, 
			 * better approach is to send only changed part instead of whole table.
			 */
			updatePartitionTable();
			
			//data re-distribution
			
			List<PartitionTableEntry> primaryPartitions = processAddPrimary(newNode, isBalanced);
			
			if (pt.hasReplica()) {
				processAddSecondary(newNode, isBalanced, primaryPartitions);
			}
												
			info("Added new node to nodes list: {}", newNode.getNodeId());
						
			checkClusterPartitions(true);	
		}												
	}
	
	/**
	 * Allows to delete multiple nodes at a time.
	 * 
	 * Coordinator sends heart beats periodically to all cluster members.
	 * If some node doesn't respond, it will be removed.  
	 * 
	 * TODO: issues:    
	 *   1. handle if we have previous 'remove' in progress: stop it and start this one
	 *   2. implement 'balanceRemoveNodes': 
	 *   e.g. partitions may be not balanced if 'remove' occurred in the middle of 'add' 
	 *   where only part of partitions were re-distributed. 
	 */
	public void removeNodes(List<NodeEntry> deleted) {
		//find entries in our PartitionTable and work with them
		List<NodeEntry> deletedNodes = new ArrayList<>();
		for (NodeEntry node : deleted) {
			deletedNodes.add(pt.getNodeEntry(node.getNodeId(), true)); //include node even if it was already marked as deleted
		}
		
		info("Removing nodes: {}", deletedNodes);
		
		/*
		 * TODO: delete nodes locally. Remove later.
		 * Nodes should not exist when this method is called.
		 * 
		 * Delete corresponding NodeEntry's in the end of processing.
		 */
		for (NodeEntry deletedNode : deletedNodes) { 
			LocalNodes.getInstance().getNodes().remove(deletedNode.getNodeId());
		}
		
		//check if we have nodes to delete
		for (NodeEntry deletedNode : deletedNodes) {
			NodeEntry n = pt.getNodeEntry(deletedNode.getNodeId(), true);
			if (n == null) {
				throw new RuntimeException(String.format("Failed to delete nodes, node: %s doesn't exist", deletedNode));
			}
		}
				
		boolean deleteAllNodes = deletedNodes.size() == pt.getNodesSize(true);
		
		if (!deleteAllNodes) {
			for (NodeEntry node : deletedNodes) { //mark nodes as deleted
				pt.markNodeAsDeleted(node.getNodeId());
			}	
			updatePartitionTable();
					
			processRemovePrimary(deletedNodes);
			
			if (pt.hasReplica()) {
				processRemoveSecondary(deletedNodes);
			}
			
			balanceRemoveNodes(deletedNodes);								
			
			//final nodes deletion
			for (NodeEntry deletedNode : deletedNodes) {
				this.remoteNodes.remove(deletedNode.getNodeId());
			}
			pt.deleteNodes(deletedNodes);			
			
			this.updatePartitionTable();
						
			checkClusterPartitions(true);
		} else {
			info("Deleting all nodes");			
			
			pt.deleteAll();
			this.remoteNodes.clear();
			
			this.updatePartitionTable();
		}						
	}
	
	/*
	 * Restore from primary if needed.
	 * If needed, remove extra secondary nodes.
	 */
	private void processRemoveSecondary(List<NodeEntry> deletedNodes) {
		//potential target nodes (sort them before usage), exclude deleted nodes 
		List<NodeEntry> targetNodes = getNodes(deletedNodes);
		
		int currentRF = Math.min(pt.getReplicationFactor(), targetNodes.size() - 1);
		
		debug("Processing remove secondary, current replication factor: {}", currentRF);
		
		//delete extra nodes if needed
		if (currentRF < pt.getReplicationFactor()) {						
			for (PartitionTableEntry entry : pt.getPartitionEntries().values()) {				
				int partId = entry.getPartitionId();
				
				int diff = excludeDeletedNodes(entry.getSecondaryNodes()).size() - currentRF;
				if (diff > 0) {
					debug("Deleting extra nodes for partition: {}, num: {}", partId, diff);
					
					//delete from max secondary.len
					Collections.sort(targetNodes, secondaryNodesDescCmp);
					
					for (int i = 0; i < diff; i ++) {						
						NodeEntry targetNode = targetNodes.get(i);
						debug("Delete secondary partition from node, node: {}, partition: {}", targetNode, partId);
												
						getRemoteNode(targetNode.getNodeId()).removePartition(NodeSection.SECONDARY, partId);	
						
						pt.getEntryForPartition(partId).removeSecondaryNode(this.getId());
					}
				}
			}
		}
		
		//restore secondary if needed
		for (NodeEntry deletedNode : deletedNodes) { //iterate nodes			
			List<PartitionTableEntry> deletedSecParts = deletedNode.getSecondaryPartitions();
			debug("Restoring secondary partitions deleted on node: {}, partitions: {}", deletedNode.getNodeId(), deletedSecParts);
			
			for (PartitionTableEntry deletedSecPart : deletedSecParts) { //iterate partitions								
				int diff = currentRF - excludeDeletedNodes(deletedSecPart.getSecondaryNodes()).size();				
				
				debug("Handle deleted secondary partition: {}, diff: {}", deletedSecPart, diff);
				
				if (diff == 0) { //need to restore ?
					debug("No need to restore secondary partition: {}", deletedSecPart);
				} else {					
					NodeEntry primaryNode = pt.getPrimaryNodeForPartition(deletedSecPart.getPartitionId(), false);
					if (primaryNode != null) { //restore												
						//find target node
						Collections.sort(targetNodes, secondaryNodesAscCmp);							
						NodeEntry targetNode = findTargetNode(targetNodes, deletedSecPart, NodeSection.SECONDARY);							
																											
						/*
						 * It may copy empty partition if there was data loss.
						 * But it is ok, because there may be some delay between handling primary and secondary partitions;
						 * and during that delay some data may be added to primary partition by clients, 
						 * so partition will not be empty. 
						 */
						copyPartition(primaryNode, deletedSecPart, NodeSection.PRIMARY, targetNode, NodeSection.SECONDARY);											
					} else {
						/*
						 * Data loss.
						 * Should not happen because primary should be restored on previous step. Delete this part ? 
						 */						
//						//create new secondary partition and register it in PartitionTable				
//						for (int i = 0; i < diff; i ++) {
//							Node targetNode = targetNodes.get(i);
//							logger.warn("Data are lost for secondary partition, create a new one, partition id: {}, target node id: {}", deletedSecPart, targetNode);						
//							targetNode.createPartition(NodeSection.SECONDARY, deletedSecPart);												
//						}
						throw new RuntimeException("Should not happen because primary should be restored on previous step.");
					}
				}
				
				//remove previous entry
				deletedSecPart.removeSecondaryNode(deletedNode.getNodeId());								
			}		
		}				
		
		this.updatePartitionTable();
	}		
	
	//Try to avoid collisions between primary and secondary.
	private NodeEntry findTargetNode(List<NodeEntry> targetNodes, PartitionTableEntry partitionId, NodeSection section) {
		NodeEntry withCollision = null;
		
		for (NodeEntry targetNode : targetNodes) {
			if (!targetNode.hasPartition(partitionId.getPartitionId(), section)) { //check if contains in section
				if (!targetNode.hasPartition(partitionId.getPartitionId(), NodeSection.reverse(section))) { //check if there is collision
					return targetNode;
				} else if (withCollision == null) {
					withCollision = targetNode;
				}				
			}
		}		
		
		if (withCollision != null) {
			return withCollision;
		}
		throw new RuntimeException(String.format("Failed to find target node. Partition: %s, section: %s", partitionId, section));		
	}

	/*
	 * Restore deleted primary partitions from secondary nodes, if there are any, 
	 * otherwise create empty partitions. 
	 */
	private void processRemovePrimary(List<NodeEntry> deletedNodes) {								
		//sort nodes by primaryData.len asc		
		List<NodeEntry> targetNodes = getNodes(deletedNodes);
		
		List<PartitionTableEntry> deletedPrimaryParts = this.getDeletedPrimaryPartitions(deletedNodes);
		
		debug("Processing remove primary, deleted partitions: {}", deletedPrimaryParts);
		
		for (PartitionTableEntry deletedPrimaryPart : deletedPrimaryParts) {			
			debug("Handle deleted primary partition: {}", deletedPrimaryPart);
			
			Collections.sort(targetNodes, primaryNodesAscCmp);
			
			//find nodes which contain deleted partition
			List<NodeEntry> secNodes = excludeDeletedNodes(deletedPrimaryPart.getSecondaryNodes());
			
			debug("Secondary nodes which contain deleted partition: {}, nodes: {}", deletedPrimaryPart, secNodes);
			if (!secNodes.isEmpty()) {
				NodeEntry replicaNode = secNodes.get(0); //take first one
				
				//get node with minimum primary
				NodeEntry targetNode = findTargetNode(targetNodes, deletedPrimaryPart, NodeSection.PRIMARY);
				
				copyPartition(replicaNode, deletedPrimaryPart, NodeSection.SECONDARY, targetNode, NodeSection.PRIMARY);				
			} else { //data loss
				//create new partition and register it in PartitionTable
				
				NodeEntry targetNode = targetNodes.get(0);
				
				warn("Data are lost for partition, create a new one, partition id: {}, node: {}", deletedPrimaryPart, targetNode);						
				
				getRemoteNode(targetNode.getNodeId()).createPartition(NodeSection.PRIMARY, deletedPrimaryPart.getPartitionId());
								
				pt.getEntryForPartition(deletedPrimaryPart.getPartitionId()).setPrimaryNode(targetNode.getNodeId()); //update PartitionTable
			}
		}
		
		this.updatePartitionTable();
	}		
	
	private List<NodeEntry> excludeDeletedNodes(List<NodeEntry> nodes) {
		List<NodeEntry> res = new ArrayList<>();
		for (NodeEntry node : nodes) {
			if (!node.isDeleted()) {
				res.add(node);
			}
		}
		return res;
	}
	
	private List<PartitionTableEntry> getDeletedPrimaryPartitions(List<NodeEntry> deletedNodes) {
		List<Integer> nodes = new ArrayList<>();
		for (NodeEntry deletedNode : deletedNodes) {
			nodes.add(deletedNode.getNodeId());
		}
		return pt.getNodesPrimaryPartitions(nodes);
	}

	private void balanceRemoveNodes(List<NodeEntry> nodesToDelete) {
		//TODO: implement		
	}
	
	private void processAddSecondary(NodeEntry newNode, boolean isBalanced, List<PartitionTableEntry> primaryPartitions) {				
		if (isBalanced) {									
			int num = (pt.getPartitionsSize() * pt.getReplicationFactor()) / pt.getNodesSize(false); //ignore remaining part of division
			debug("Processing add secondary: balanced, num: {}", num);
			
			Set<PartitionTableEntry> addedParts = new HashSet<>(primaryPartitions); 
			
			//sort nodes by secondaryData.len desc
			List<NodeEntry> sortedNodes = getNodes(Arrays.asList(newNode));			
									
			for (int i = 0; i < num;) {	
				if (sortedNodes.isEmpty()) {
					debug("Didn't all needed nodes to secondary, left: {}", num);
					break;
				}
				Collections.sort(sortedNodes, secondaryNodesDescCmp);
				
				NodeEntry node = sortedNodes.get(0);
				
				//find first partition which is not already present in primary and secondary
				PartitionTableEntry part = null;
				for (PartitionTableEntry p : node.getSecondaryPartitions()) {
					if (!addedParts.contains(p)) {
						part = p;
						addedParts.add(part);
						break;
					} else {
						debug("Don't add secondary partition: '{}' in node {}, because it already exists", p, node);
					}
				}
				if (part == null) { //don't process this node any more
					sortedNodes.remove(node);
					continue;
				}
												
				movePartition(node, part, NodeSection.SECONDARY, newNode, NodeSection.SECONDARY);
										
				i ++;
			}		
		} else {
			debug("Processing secondary: not balanced");
			
			//copy all primary to new node's secondary
			for (NodeEntry node : getNodes(Arrays.asList(newNode))) {
				for (PartitionTableEntry part : node.getPrimaryPartitions()) {
					copyPartition(node, part, NodeSection.PRIMARY, newNode, NodeSection.SECONDARY);
				}
			}
		}				
	}	
	
	private List<PartitionTableEntry> processAddPrimary(NodeEntry newNode, boolean isBalanced) {								
		int num = pt.getPartitionsSize() / pt.getNodesSize(false); //ignore remaining part of division
		debug("Processing add primary, num: {}", num);
		
		List<PartitionTableEntry> primaryPartitions = new ArrayList<>();
		
		//sort nodes by primaryData.len desc		
		List<NodeEntry> sortedNodes = getNodes(Arrays.asList(newNode));
		
		for (int i = 0; i < num; i ++) {
			Collections.sort(sortedNodes, primaryNodesDescCmp);
			
			NodeEntry node = sortedNodes.get(0);
			PartitionTableEntry p1 = node.getPrimaryPartitions().get(0);
			
			if (isBalanced) {
				movePartition(node, p1, NodeSection.PRIMARY, newNode, NodeSection.PRIMARY);
			} else {
				copyPartition(node, p1, NodeSection.PRIMARY, newNode, NodeSection.PRIMARY);
				
				//TODO: don't need extra 'copy' here: move right away
				movePartition(node, p1, NodeSection.PRIMARY, node, NodeSection.SECONDARY);	//move partition to the same node's secondary section
			}												
			
			primaryPartitions.add(p1);
		}
		return primaryPartitions;
	}	
	
	//don't include deleted nodes
	private List<NodeEntry> getNodes(List<NodeEntry> exclude) {
		List<NodeEntry> nodes = new ArrayList<>();
		for (NodeEntry n : pt.getNodeEntries(false)) {
			if (!exclude.contains(n)) {
				nodes.add(n);
			}
		}
		return nodes;
	}

	//called by coordinator
	private void copyPartition(
		NodeEntry src, PartitionTableEntry srcPartId, NodeSection srcSection,
		NodeEntry dest, NodeSection destSection) /*throws IOException*/ {
		
		transferPartition(false, src, srcPartId, srcSection, dest, destSection);		
	}
	
	//called by coordinator
	private void movePartition(
		NodeEntry src, PartitionTableEntry srcPartId, NodeSection srcSection, 
		NodeEntry dest, NodeSection destSection) /*throws IOException*/ {
		
		transferPartition(true, src, srcPartId, srcSection, dest, destSection);		
	}
	
	/*
	 * TODO: There are possible local copies and removes - handle appropriately.
	 */
	//called by coordinator
	private void transferPartition(boolean isMove, 
		NodeEntry srcEntry, PartitionTableEntry srcPartEntry, NodeSection srcSection, 
		NodeEntry destEntry, NodeSection destSection) /*throws IOException*/ {
		
		info((!isMove ? "Copying" : "Moving ") + 
			" partition '{}' from '{}'.{} to '{}'.{}", 
			srcPartEntry.getPartitionId(), srcEntry.getNodeId(), srcSection, destEntry.getNodeId(), destSection);
		
		//it is null for current node: call methods directly on this node
		//TODO: maybe it is better to use unique 'INode' interface ?
		RemoteNode src = remoteNodes.get(srcEntry.getNodeId());
				
		if (isMove && srcEntry.getNodeId() == destEntry.getNodeId()) { //local move
			debug("Move partition locally");
			if (src != null) {
				src.movePartitionLocally(srcSection, srcPartEntry.getPartitionId(), destSection);
			} else {
				this.movePartitionLocally(srcSection, srcPartEntry.getPartitionId(), destSection);
			}
		} else {
			//make actual copy
			if (src != null) {
				src.copyPartition(srcSection, srcPartEntry.getPartitionId(), destEntry.getNodeId(), destSection);	
			} else {
				this.copyPartition(srcSection, srcPartEntry.getPartitionId(), destEntry.getNodeId(), destSection);
			}
							
			if (isMove) { //delete from src			
				//logger.debug("Deleting partition '{}' from {} node, isPrimary: {}", srcPart.getId(), src.getId(), isSrcPrimary);
				if (src != null) {
					src.removePartition(srcSection, srcPartEntry.getPartitionId());	
				} else {
					this.removePartition(srcSection, srcPartEntry.getPartitionId());
				}			
			}			
		}
		
				
		//update partition table
		if (isMove) {			
			if (srcSection == NodeSection.PRIMARY) {		
				srcPartEntry.removePrimaryNode(srcEntry.getNodeId());
			} else {
				srcPartEntry.removeSecondaryNode(srcEntry.getNodeId());
			} 
		}				
		
		if (destSection == NodeSection.PRIMARY) {
			srcPartEntry.setPrimaryNode(destEntry.getNodeId());
		} else {
			srcPartEntry.addSecondaryNode(destEntry.getNodeId());
		}	
		
		updatePartitionTable();
	}
	

	/*
	 * TODO: Possible strategies:
	 * 
	 * 1. Do nothing
	 * 
	 * 2. Throw exception 
	 * 
	 * 3. We can move secondary parts to new node  
	 * 
	 * 4. Increase partitionsCount, e.g. double, and re-distribute nodes
	 * Probably it is best option, but do it later.
	 */
	private void clusterFull(NodeEntry newNode) {
		//logger.warn("Cluster is full, do nothing. partitionsCount: {}, nodesCount: {}", pt.getPartitionsSize(), pt.getNodesSize());
		throw new RuntimeException(String.format(
			"Failed to add node, because cluster is full. partitionsCount: %s, nodesCount: %s", 
			pt.getPartitionsSize(), pt.getNodesSize(false)));
	}

	private void initCluster(NodeEntry newNode) {
		int replicationFactor = config.getInt("replicationFactor");
		int partitionsCount = config.getInt("partitionsCount");
		
		info("Creating cluster, replicationFactor: {}, partitionsCount: {}", replicationFactor, partitionsCount);								
				
		pt = new PartitionTable(replicationFactor, partitionsCount); //create partition table
		pt.setCoordinator(newNode);	 //make first node as coordinator
		
		pt.addNodeEntry(newNode); //update own PartitionTable
		
		for (int i = 0; i < pt.getPartitionsSize(); i ++) { //create primary partitions in this node
			int partitionId = i;			
			this.createPartition(NodeSection.PRIMARY, partitionId);	
						
			pt.getEntryForPartition(partitionId).setPrimaryNode(this.getId()); //update PartitionTable
			
		}									
	}
	
	private void checkClusterPartitions(boolean checkCollisions) {
		Set<PartitionTableEntry> primaryTotal = new HashSet<>();
		int secondaryTotal = 0;
		
		List<String> errors = new ArrayList<>();
				
		for (NodeEntry n : pt.getNodeEntries(true)) {
			primaryTotal.addAll(n.getPrimaryPartitions());
			secondaryTotal += n.getSecondaryPartitions().size();			
			
			//check collisions
			if (checkCollisions) {
				for (PartitionTableEntry p : n.getSecondaryPartitions()) {
					if (n.getPrimaryPartitions().contains(p)) {
						errors.add(String.format("Secondary and primary collide for node: %s", n.getNodeId()));
					}
				}		
			}
			
			//duplicates
			if (n.getPrimaryPartitionsCount() != new HashSet<>(n.getPrimaryPartitions()).size()) {
				errors.add("There are duplicates in primary: " + n.getNodeId());
			}
			if (n.getSecondaryPartitionsCount() != new HashSet<>(n.getSecondaryPartitions()).size()) {
				errors.add("There are duplicates in secondary: " + n.getNodeId());
			}
		}		
				
		if (primaryTotal.size() != pt.getPartitionsSize()) {
			errors.add(String.format("Primary total is not correct. Expected: %d, was: %s", pt.getPartitionsSize(), primaryTotal));
		}
		
		if (pt.hasReplica()) {
			int secExpected = (pt.getNodesSize(true) > pt.getReplicationFactor() ? pt.getReplicationFactor() : pt.getNodesSize(true) - 1) * pt.getPartitionsSize();
			if (secondaryTotal != secExpected) {
				errors.add(String.format("Secondary total is not correct. Expected: %d, was: %d", secExpected, secondaryTotal));
			}							
		} else if (secondaryTotal > 0) {
			errors.add("Secondary total is not empty: " + secondaryTotal);
		}				
		
		if (!errors.isEmpty()) {
			throw new RuntimeException("Found errors: " + errors);
		}
	}
	
	public boolean isEmpty() {
		return this.pt.getNodesSize(false) == 0;
	}

	public PartitionTable getPartitionTable() {		
		return this.pt;
	}
	
	/**
	 * Update PartitionTable in all changes: this is current strategy to propagate changes in PartitionTable. 
	 */
	private void updatePartitionTable() {
		debug("Updating partition table");
		for (RemoteNode node : remoteNodes.values()) {	
			if (pt.getNodeEntry(node.getId(), false) != null) { //don't call deleted node
				node.setPartitionTable(pt);	
			}			
		}
	}
	
	public InetAddress getAddress() {
		return address;
	}
	
	public RemoteNode getRemoteNode(int nodeId) {
		RemoteNode node = this.remoteNodes.get(nodeId);
		if (node == null) {
			throw new RuntimeException(
				String.format("Failed to get remote node. Node: %s, remote node: %s", this.id, nodeId));
		}
		return node;
	}
	
	private String logPrefix() {
		return "Node: " + this.id + ". ";
	}
	private void info(String msg, Object... arguments) {
		logger.info(logPrefix() + msg, arguments);
	}
	
	private void trace(String msg, Object... arguments) {
		logger.trace(logPrefix() + msg, arguments);
	}
	
	private void debug(String msg, Object... arguments) {
		logger.debug(logPrefix() + msg, arguments);
	}
	
	private void warn(String msg, Object... arguments) {
		logger.warn(logPrefix() + msg, arguments);
	}
	
	public static RemoteNode createRemoteNode(NodeEntry nodeEntry) {
		return Node.createRemoteNode(nodeEntry.getNodeId(), nodeEntry.getAddress());
	}
	
	public static RemoteNode createRemoteNode(int nodeId, InetAddress address) {
		RemoteNode remoteNode = new LocalRemoteNodeImpl(nodeId, address);
		return remoteNode;
	}
	
}
