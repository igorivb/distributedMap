package com.io.tcp;

import java.io.EOFException;
import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;

import com.io.MyConnection;

public class InSelector extends Thread {

	private final static Logger logger = Logger.getLogger(InSelector.class);
	
	final MyTcpConnectionManager connectionManager;
	final int selectorNum;
	
	Selector selector;
	
	private BlockingQueue<Runnable> selectionQueue = new LinkedBlockingQueue<>();
			
	
	final int selectWaitTime = 5000; 
	
	public InSelector(MyTcpConnectionManager connectionManager, int selectorNum) {
		super("in-selector-" + selectorNum);
		this.connectionManager = connectionManager;
		this.selectorNum = selectorNum;
	}
	
	@Override
	public void run() {
		try {
			selector = Selector.open();

			while (true) {				
				processSelectionQueue();				
				
				int nKeys = selector.select(selectWaitTime); //blocking
				if (nKeys == 0) {
					continue;
				}
				
				Set<SelectionKey> keys = selector.selectedKeys();
				Iterator<SelectionKey> iter = keys.iterator();								
				
				while (iter.hasNext()) {										
					SelectionKey key = iter.next();					
					iter.remove();
					
					if (key.isReadable()) {																		
						doRead(key);														
					} 				
				}				
			}		
		} catch (IOException ie) {
			throw new RuntimeException(ie);
		}	
	}
	
	public void addTaskAndWakeup(Runnable task) {
		selector.wakeup();
		selectionQueue.add(task);		
	}
	
	void processSelectionQueue() {		
		while (true) { //loop till there tasks
			Runnable task = selectionQueue.poll();
			if (task != null) {
				task.run();
			} else {
				return;
			}		
		}
	}
	
	
	//TODO: 1. check that it works if object is transfered in several iterations
	//TODO: 2. how to handle if body size is more than ByteBuffer size ?
	void doRead(SelectionKey key) throws IOException {
		SocketChannel socketChannel = (SocketChannel) key.channel();
		
		ConnectionReadHandler readHandler = (ConnectionReadHandler) key.attachment();
		
		try {
			readHandler.handle();	
		} catch (EOFException e) {
			logger.info("Closed connection to: " + socketChannel.getRemoteAddress());
			
			key.cancel();
			socketChannel.close();		
			
			connectionManager.removeConnection(readHandler.con);						
		} 																															
	}
	
	
	Object registeringSync = new Object();
	volatile boolean initialized = false;
	
	//called only by 1 thread
	public void register(MyConnection con) throws IOException {
		logger.info("Add connection by InSelector: " + selectorNum);
		
		MyTcpConnection tcpCon = (MyTcpConnection) con;
				
		this.addTaskAndWakeup(() -> {
				try {
					logger.info("Register connection by InSelector: " + selectorNum);
					
														
					tcpCon.socketChannel.register(selector, SelectionKey.OP_READ, tcpCon.getReadHandler());										
				} catch (IOException e) {
					throw new RuntimeException(e);
				}				
		});			
	}

}
