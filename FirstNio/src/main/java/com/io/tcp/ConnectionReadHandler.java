package com.io.tcp;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.log4j.Logger;

import com.Message;
/*
 * It allows to read 1 message at a time: only when current message is processed, it goes to another message.  
 */
public class ConnectionReadHandler {

	private final static Logger logger = Logger.getLogger(ConnectionReadHandler.class);
	
	final MyTcpConnection con;
	
	//it is used 1 buffer for all connection reads
	ByteBuffer buf;
	
	Message currentMessage;
		
	
	public ConnectionReadHandler(MyTcpConnection con) {
		this.con = con;
		
		buf = ByteBuffer.allocate(16);
		
		currentMessage = new Message(con);
	}
	
	//called by 1 thread
	public void handle() throws IOException {
		int byteRead = con.socketChannel.read(buf);
		
		if (byteRead == -1) { //eof
			throw new EOFException("Remote socket closed!");
		} 
		
		if (byteRead > 0) {
			buf.flip();														
			boolean isFull = currentMessage.read(buf);
			
			if (buf.hasRemaining()) {
				buf.compact();	
			} else {
				buf.clear();
			}								
			
			if (isFull) { //message is full: start processing and create new one
				logger.info(String.format("%3d_%d. Read by selector, %s", 
					con.conManager.readNum.incrementAndGet(), 
					currentMessage.client, 
					currentMessage));
				
				con.conManager.handleMessage(currentMessage);
				
				currentMessage = new Message(con);
			}	
		}
	}
	
}
