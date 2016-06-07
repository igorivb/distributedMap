package com;

public class MessageResponse {

	public final int res;
	
	public final int client;
	
	public final int correlationId;

	public MessageResponse(int res, int client, int correlationId) {
		super();
		this.res = res;
		this.client = client;
		this.correlationId = correlationId;
	}

	@Override
	public String toString() {
		return "MessageResponse [res=" + res + "]";
	}
	
	
}
