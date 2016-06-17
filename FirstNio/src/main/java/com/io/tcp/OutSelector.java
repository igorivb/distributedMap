package com.io.tcp;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;

import com.io.MyConnection;

public class OutSelector extends Thread {

	private final static Logger logger = Logger.getLogger(OutSelector.class);
	
	final MyTcpConnectionManager connectionManager;
	final int selectorNum ;
	
	Selector selector;
	
	private BlockingQueue<Runnable> selectionQueue = new LinkedBlockingQueue<>();
	
	final int selectWaitTime = 5000;
	
	
	public OutSelector(MyTcpConnectionManager connectionManager, int selectorNum) {
		super("out-selector-" + selectorNum);
				
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
				} else {
					logger.trace(String.format("Selector %d, keys num: %d", selectorNum, nKeys));
				}								
				
				Set<SelectionKey> keys = selector.selectedKeys();
				Iterator<SelectionKey> iter = keys.iterator();								
				
				while (iter.hasNext()) {										
					SelectionKey key = iter.next();										
					iter.remove();
					
					
					logger.trace("Key operations: " + key.readyOps());
					
					if (key.isWritable()) {
						doWrite(key);																	
					}	
					
					
				}				
			}	
		} catch (IOException e) {
			throw new RuntimeException(e);
		}					
	}
	

	void doWrite(SelectionKey key) throws IOException {
		//SocketChannel socketChannel = (SocketChannel) key.channel();
		
		ConnectionWriteHandler writeHandler = (ConnectionWriteHandler) key.attachment();		
		writeHandler.handle();						
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
	
	public void addTaskAndWakeup(Runnable task) {
		selector.wakeup();
		selectionQueue.add(task);		
	}
	
	//called only by 1 thread
	public void register(MyConnection con) throws IOException {
		logger.debug("Adding connection by OutSelector: " + selectorNum);
		
		MyTcpConnection tcpCon = (MyTcpConnection) con;
		
		this.addTaskAndWakeup(() -> {
			try {				
				logger.debug("Register connection by OutSelector: " + selectorNum);			
				
				tcpCon.socketChannel.register(selector, SelectionKey.OP_WRITE, tcpCon.getWriteHandler());			
			} catch (IOException e) {
				throw new RuntimeException(e);
			}										
		});
		
		
	}

}
