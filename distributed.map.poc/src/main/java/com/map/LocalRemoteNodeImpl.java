package com.map;

public class LocalRemoteNodeImpl implements RemoteNode {

	private final int id;
	
	public LocalRemoteNodeImpl(int id) {
		this.id = id;
	}

	@Override
	public int getId() {
		return id;
	}
}
