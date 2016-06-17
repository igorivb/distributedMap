package com.io.tcp;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.apache.log4j.Logger;

import com.MessageResponse;

public class ConnectionWriteHandler {
	
	private final static Logger logger = Logger.getLogger(ConnectionWriteHandler.class);
		
	final MyTcpConnection con;
	
	//TODO it is used 1 buffer for all connection writes ?
	ByteBuffer buf;
	
	MessageResponse currentMessage;

	
	//processed messages (messages that we need to write to client)
	BlockingQueue<MessageResponse> responsesQueue = new ArrayBlockingQueue<>(10);
	
	
	public ConnectionWriteHandler(MyTcpConnection con) {
		this.con = con;
		
		buf = ByteBuffer.allocate(12);
	}

	//called by 1 thread
	public void handle() throws IOException {				
		
		//get message to write
		if (currentMessage == null) {
			
			//TODO: make it blocking in order not to waste CPU ?		
			currentMessage = responsesQueue.poll(); // non blocking
			
			if (currentMessage == null) {				
				logger.trace("Message is empty");				
				return;
			}	
		}
		
		boolean isFull = currentMessage.write(buf);
		
		if (buf.position() == 0) {			
			logger.debug("Buffer is empty");
			return;
		}
		
		
		buf.flip();
		
		con.socketChannel.write(buf);
		
		if (buf.hasRemaining()) {
			buf.compact();	
		} else {
			buf.clear();
		}
		
									
		if (isFull) { //message was sent, prepare for new one
			logger.info(String.format("%3d_%d. Sent: %s", 
				con.conManager.writeNum.incrementAndGet(), 
				currentMessage.client, 
				currentMessage));
			
			currentMessage = null;
		}				
		
	}
}
