package com;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/*
 * TODO: 
 * 	1. implement that we can have multiple servers and clients
 *  2. allow communication read write
 */


//accept, read and write
public class MyServer implements Runnable {

	public static int SERVER_PORT = 9898;
	
	
	private BlockingQueue<Message> workQueue = new ArrayBlockingQueue<>(10);
	
	private BlockingQueue<MessageResponse> readQueue = new ArrayBlockingQueue<>(10);
	
	final int workThreads = 2;
	
	@Override
	public void run() {		
		SocketAddress serverAddress = new InetSocketAddress(SERVER_PORT);
		
		try (ServerSocketChannel serverChannel = ServerSocketChannel.open()) {												
			ServerSocket ss = serverChannel.socket();
			ss.bind(serverAddress);												
			
			System.out.println("Listening in address: " + serverAddress);
			
			//TODO serverChannel.setOption(name, value)
			serverChannel.configureBlocking(false); //not blocking
			
			Selector selector = Selector.open();
			serverChannel.register(selector, SelectionKey.OP_ACCEPT); //register for accept												
		
			
			while (true) {
				
				selector.select(); //blocking
				
				Set<SelectionKey> keys = selector.selectedKeys();
				Iterator<SelectionKey> iter = keys.iterator();
				
				while (iter.hasNext()) {
					SelectionKey key = iter.next();
					
					if (key.isAcceptable()) { //accept new connection
						
						SocketChannel socketChannel = ((ServerSocketChannel) key.channel()).accept();
						socketChannel.configureBlocking(false);
						//TODO socketChannel.setOption(name, value)
						
						SelectionKey key2 = socketChannel.register(selector, SelectionKey.OP_READ | SelectionKey.OP_WRITE); //register for read and write
						key2.attach(ByteBuffer.allocate(16));						
						
						System.out.println("Accept client connection: " + socketChannel.getRemoteAddress());
						
						iter.remove(); //remove so we don't process it twice						
					} else {
						SocketChannel clientChannel = (SocketChannel) key.channel();
						
						ByteBuffer buf = (ByteBuffer) key.attachment();
						
						//TODO: can key have read and write at the same time ?
						
						if (key.isReadable()) { //read																		
																											
							int byteRead = clientChannel.read(buf);
							if (byteRead == -1) { //eof		
								
								System.out.println("Closed connection to: " + clientChannel.getRemoteAddress());
								
								key.cancel();
								clientChannel.close();		
								
								continue;
							} 
							
							if (!buf.hasRemaining()) { //message is full
								buf.flip();
									
								handleRead(buf);	
									
								buf.clear();															
							}																				

						} 
						
						if (key.isWritable()) { //write TODO: send response to client
							
							
							
							
							//clientChannel.write();
						}
						
						iter.remove(); //remove so we don't process it twice
					}
				}
				
			}
			
		} catch (IOException ie) {
			throw new RuntimeException(ie);
		}	
	}
	
	int readNum = 0;
	private void handleRead(ByteBuffer buf) {
		Message msg = new Message(buf.getInt(), buf.getInt(), buf.getInt(), buf.getInt());
		
		System.out.printf("%3d_%d. Read: %s%n", (readNum++ + 1), msg.client, msg);		
		
		//add to working queue for processing
		try {
			workQueue.put(msg);  //blocking
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
	}
	
	private void handleWrite(ByteBuffer buf) {
		// TODO Auto-generated method stub
		
	}
	
	public void init() {
		ExecutorService executors = Executors.newFixedThreadPool(workThreads);
		executors.execute(new Runnable() {			
			@Override
			public void run() {
				while (true) {
				
					try {
						Message msg = workQueue.take();
						
						//do some calculations
						System.out.println("Executing: " + msg);
											
						MessageResponse response = new MessageResponse(msg.n1 + msg.n2, msg.client, msg.correlationId);
						readQueue.put(response);
					} catch (Exception e) {
						throw new RuntimeException(e);
					}										
				}				
			}
		});
	}

	public void exec() {
		init();
		
		Thread t = new Thread(this);
		t.start();
		
		try {
			t.join();
		} catch (InterruptedException e) {
			 /* ignore */
		}
		
	}
	
	public static void main(String[] args) {
		MyServer server = new MyServer();
		server.exec();
	}

	
}
