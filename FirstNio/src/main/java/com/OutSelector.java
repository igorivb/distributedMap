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

public class OutSelector extends Thread {

	final MyServer server;
	final int num ;
	
	Selector selector;
	
	private BlockingQueue<Runnable> selectionQueue = new LinkedBlockingQueue<>();
	
	final int selectWaitTime = 5000;
	
	
	public OutSelector(MyServer server, int num) {
		super("out-selector-" + num);
		
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
				} else {
					System.out.println("Keys num: " + nKeys);
				}
				
				
				
				Set<SelectionKey> keys = selector.selectedKeys();
				Iterator<SelectionKey> iter = keys.iterator();								
				
				while (iter.hasNext()) {										
					SelectionKey key = iter.next();
					
					if (key.isWritable()) {
						if (doWrite(key)) {
							iter.remove(); //remove so we don't process it twice
						}							
					}																
				}				
			}	
		} catch (IOException e) {
			throw new RuntimeException(e);
		}					
	}
	
	/**
	 * 
	 * @return true if selection key can be removed.
	 */
	boolean doWrite(SelectionKey key) throws IOException {
		SocketChannel socketChannel = (SocketChannel) key.channel();
		OutputPart ioPart = (OutputPart) key.attachment();		
		ByteBuffer buf = ioPart.buf;				
		
		//get message to write
		if (ioPart.msg == null) {
			ioPart.msg = server.responsesQueue.poll(); // non blocking
			if (ioPart.msg == null) {
				return false;
			}	
		}
		
		boolean isFull = ioPart.msg.write(buf);
		
		if (buf.position() == 0) {
			return false;
		}
		
		
		buf.flip();
		
		socketChannel.write(buf);
		
		if (buf.hasRemaining()) {
			buf.compact();	
		} else {
			buf.clear();
		}
		
									
		if (isFull) { //message was sent, prepare for new one
			System.out.printf("%3d_%d. Sent: %s%n", server.writeNum.incrementAndGet(), ioPart.msg.client, ioPart.msg);
			
			ioPart.msg = null;
		}				
		
		return true;
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
		selectionQueue.add(task);
		selector.wakeup();
	}
	
	//called only by 1 thread
	public void register(SocketChannel socketChannel) throws IOException {
		System.out.println("Add connection by OutSelector: " + num);
		
		this.addTaskAndWakeup(() -> {
			try {
				
				System.out.println("Register connection by OutSelector: " + num);
				
				OutputPart writePart = new OutputPart();		
				writePart.buf = ByteBuffer.allocate(12);
				writePart.msg = null;
				
				socketChannel.register(selector, SelectionKey.OP_WRITE, writePart);			
			} catch (IOException e) {
				throw new RuntimeException(e);
			}										
		});
		
		
	}

}
