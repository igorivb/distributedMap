package com;

import java.nio.ByteBuffer;

import com.io.MyConnection;

public class MessageResponse {

	public final MyConnection con; 
	
	
	public int result;
	
	public int client;
	
	public int correlationId;
	
	//in order to track writing
	private int writeNum = 0;	
	
	//in order to track reading
	private int readNum = 0;

	public MessageResponse(MyConnection con) {
		this.con = con;
	}
	
	public MessageResponse(int result, int client, int correlationId, MyConnection con) {
		super();
		this.result = result;
		this.client = client;
		this.correlationId = correlationId;
		
		this.con = con;
	}

	/**
	 * @return true if object is fully read.
	 */
	public boolean read(ByteBuffer buf) {	
		if (readNum == 0) {
			if (buf.remaining() >= 4) {
				result = buf.getInt();
				
				readNum ++;	
			} else {
				return false;
			}
		}
		
		if (readNum == 1) {
			if (buf.remaining() >= 4) {
				client = buf.getInt();
				
				readNum ++;	
			} else {
				return false;
			}
		}
		
		if (readNum == 2) {
			if (buf.remaining() >= 4) {
				correlationId = buf.getInt();
				
				readNum ++;	
			} else {
				return false;
			}
		}				
		
		return true;
	}
	
	/**
	 * @return true if object is fully written.
	 */
	public boolean write(ByteBuffer buf) {
		if (writeNum == 0) {
			if (buf.remaining() >= 4) {
				buf.putInt(result);
				
				writeNum ++;	
			} else {
				return false;
			}
		}
		
		if (writeNum == 1) {
			if (buf.remaining() >= 4) {
				buf.putInt(client);
				
				writeNum ++;	
			} else {
				return false;
			}
		}
		
		if (writeNum == 2) {
			if (buf.remaining() >= 4) {
				buf.putInt(correlationId);
				
				writeNum ++;	
			} else {
				return false;
			}
		}
	
		return true;
	}
	
	@Override
	public String toString() {
		return "MessageResponse [result=" + result + ", client=" + client
				+ ", correlationId=" + correlationId + "]";
	}

}
