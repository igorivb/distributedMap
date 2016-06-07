package com;

public class Message {

	public final int n1;
	
	public final int n2;
	
	public final int client;
	
	public final int correlationId;
	
	public Message(int n1, int n2, int client, int correlationId) {
		this.n1 = n1;
		this.n2 = n2;
		this.client = client;
		this.correlationId = correlationId;
	}

	@Override
	public String toString() {
		return "Message [n1=" + n1 + ", n2=" + n2 + "]";
	}		
}
