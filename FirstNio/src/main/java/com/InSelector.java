package com;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class InSelector extends Thread {

	final MyServer server;
	final int num;
	
	Selector selector;
	
	private BlockingQueue<Runnable> selectionQueue = new LinkedBlockingQueue<>();
	
	final int selectWaitTime = 5000; 
	
	public InSelector(MyServer server, int num) {
		super("in-selector-" + num);
		this.server = server;
		this.num = num;
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
																																														
					if (key.isReadable()) {						
						iter.remove(); //remove so we don't process it twice
						
						doRead(key);														
					} 				
				}				
			}		
		} catch (IOException ie) {
			throw new RuntimeException(ie);
		}	
	}
	
	public void addTaskAndWakeup(Runnable task) {
		selectionQueue.add(task);
		selector.wakeup();
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
		InputPart ioPart = (InputPart) key.attachment();		
		ByteBuffer buf = ioPart.buf;
																						
		int byteRead = socketChannel.read(buf);
		
		if (byteRead == -1) { //eof										
			System.out.println("Closed connection to: " + socketChannel.getRemoteAddress());
			
			key.cancel();
			socketChannel.close();		
			
			//throw new EOFException("Remote socket closed!");			
			return;
		} 
		
		if (byteRead > 0) {
			buf.flip();														
			boolean isFull = ioPart.msg.read(buf);
			
			if (buf.hasRemaining()) {
				buf.compact();	
			} else {
				buf.clear();
			}								
			
			if (isFull) { //message is full: start processing and create new one
				System.out.printf("%3d_%d. Read by selector: %d, %s%n", server.readNum.incrementAndGet(), ioPart.msg.client, num, ioPart.msg);
				
				server.handleMessage(ioPart.msg);
				
				ioPart.msg = new Message();
			}	
		}								
	}
	
	
	Object registeringSync = new Object();
	volatile boolean initialized = false;
	
	//called only by 1 thread
	public void register(SocketChannel socketChannel) throws IOException {
		System.out.println("Add connection by InSelector: " + num);				
				
		this.addTaskAndWakeup(() -> {
				try {
					System.out.println("Register connection by InSelector: " + num);
					
					InputPart readPart = new InputPart();		
					readPart.buf = ByteBuffer.allocate(16);
					readPart.msg = new Message();
															
					socketChannel.register(selector, SelectionKey.OP_READ, readPart);										
				} catch (IOException e) {
					throw new RuntimeException(e);
				}				
		});			
	}

}
