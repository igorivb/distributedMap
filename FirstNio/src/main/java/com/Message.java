package com;

import java.nio.ByteBuffer;

public class Message {

	public int n1;
	
	public int n2;
	
	public int client;
	
	public int correlationId;
	
	//in order to track writing
	private int writeNum = 0;	
		
	
	//in order to track reading
	private int readNum = 0;
	
	public Message() { }		
	
	public Message(int n1, int n2, int client, int correlationId) {
		super();
		this.n1 = n1;
		this.n2 = n2;
		this.client = client;
		this.correlationId = correlationId;
	}

	public boolean write(ByteBuffer buf) {	
		if (writeNum == 0) {
			if (buf.remaining() >= 4) {
				buf.putInt(n1);
				
				writeNum ++;	
			} else {
				return false;
			}
		}
		
		if (writeNum == 1) {
			if (buf.remaining() >= 4) {
				buf.putInt(n2);
				
				writeNum ++;	
			} else {
				return false;
			}
		}
		
		if (writeNum == 2) {
			if (buf.remaining() >= 4) {
				buf.putInt(client);
				
				writeNum ++;	
			} else {
				return false;
			}
		}
		
		if (writeNum == 3) {
			if (buf.remaining() >= 4) {
				buf.putInt(correlationId);
				
				writeNum ++;	
			} else {
				return false;
			}
		}
	
		return true;
	}
	
	
	/**
	 * @return true if object is fully read.
	 */
	public boolean read(ByteBuffer buf) {		
		if (readNum == 0) {
			if (buf.remaining() >= 4) {
				n1 = buf.getInt();
				
				readNum ++;	
			} else {
				return false;
			}
		}
		
		if (readNum == 1) {
			if (buf.remaining() >= 4) {
				n2 = buf.getInt();
				
				readNum ++;	
			} else {
				return false;
			}
		}
		
		if (readNum == 2) {
			if (buf.remaining() >= 4) {
				client = buf.getInt();
				
				readNum ++;	
			} else {
				return false;
			}
		}
		
		if (readNum == 3) {
			if (buf.remaining() >= 4) {
				correlationId = buf.getInt();
				
				readNum ++;	
			} else {
				return false;
			}
		}
		
		return true;
	}
	
	@Override
	public String toString() {
		return "Message [n1=" + n1 + ", n2=" + n2 + ", client=" + client 
				+ ", correlationId=" + correlationId + "]";
	}

	
}
