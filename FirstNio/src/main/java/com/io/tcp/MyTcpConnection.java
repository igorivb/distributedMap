package com.io.tcp;

import java.nio.channels.SocketChannel;

import com.MessageResponse;
import com.io.MyConnection;

public class MyTcpConnection implements MyConnection {

	final SocketChannel socketChannel;
	final MyTcpConnectionManager conManager;	
	
	ConnectionReadHandler readHandler;	
	ConnectionWriteHandler writeHandler;
	
	
	public MyTcpConnection(SocketChannel socketChannel, MyTcpConnectionManager conManager) {
		this.socketChannel = socketChannel;
		this.conManager = conManager;
		
		readHandler = new ConnectionReadHandler(this);
		writeHandler = new ConnectionWriteHandler(this);
	}
	
	public ConnectionReadHandler getReadHandler() {
		return readHandler;
	}
	
	public ConnectionWriteHandler getWriteHandler() {
		return writeHandler;
	}

	@Override
	public void write(MessageResponse response) {
		try {
			writeHandler.responsesQueue.put(response);
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
		
	}

}
