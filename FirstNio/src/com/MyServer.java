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
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/*
 * TODO: 
 * 	1. implement that we can have multiple servers and clients
 *  2. allow communication read write
 */


//accept, read and write
public class MyServer implements Runnable {

	public static int SERVER_PORT = 9898;
	
	final int workThreads = 1;
	
	final AtomicInteger readNum = new AtomicInteger();
	
	final AtomicInteger writeNum= new AtomicInteger();
	
	
	//messages to process (messages that we read from client)
	private BlockingQueue<Message> workQueue = new ArrayBlockingQueue<>(10);
	
	//processed messages (messages that we need to write to client)
	private BlockingQueue<MessageResponse> responsesQueue = new ArrayBlockingQueue<>(10);

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
					boolean removeKey = false;
					
					if (key.isAcceptable()) { // accept new connection
						doAccept(key);	
						
						removeKey = true;						
					} else { // read or write																																							
						if (key.isReadable()) {
							doRead(key);	
							
							removeKey = true;
						} 
						
						if (key.isWritable()) {
							if (doWrite(key)) {
								removeKey = true;
							}							
						}												
					}
					
					if (removeKey) {
						iter.remove(); //remove so we don't process it twice	
					}
					
				}				
			}
			
		} catch (IOException ie) {
			throw new RuntimeException(ie);
		}	
	}	
	
	void doAccept(SelectionKey serverKey) throws IOException {
		SocketChannel socketChannel = ((ServerSocketChannel) serverKey.channel()).accept();
		socketChannel.configureBlocking(false);
		//TODO socketChannel.setOption(name, value)
		
		SelectionKey clientKey = socketChannel.register(serverKey.selector(), SelectionKey.OP_READ | SelectionKey.OP_WRITE); //register for read and write
		
		//key2.attach(ByteBuffer.allocate(16));
		SelectionWrapper selectionWrapper = new SelectionWrapper();
		
		InputPart readPart = new InputPart();
		selectionWrapper.readPart = readPart;
		readPart.buf = ByteBuffer.allocate(16);
		readPart.msg = new Message();
		
		OutputPart writePart = new OutputPart();
		selectionWrapper.writePart = writePart;
		writePart.buf = ByteBuffer.allocate(12);
		writePart.msg = null;
		
		clientKey.attach(selectionWrapper);
		
		
		System.out.println("Accept client connection: " + socketChannel.getRemoteAddress());				
	}		
	
	/**
	 * 
	 * @return true if selection key can be removed.
	 */
	boolean doWrite(SelectionKey key) throws IOException {
		SelectionWrapper selectionWrapper = (SelectionWrapper) key.attachment();
		SocketChannel socketChannel = (SocketChannel) key.channel();
		
		OutputPart ioPart = selectionWrapper.writePart;
		ByteBuffer buf = ioPart.buf;				
		
		//get message to write
		if (ioPart.msg == null) {
			ioPart.msg = responsesQueue.poll(); // non blocking
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
			System.out.printf("%3d_%d. Sent: %s%n", writeNum.incrementAndGet(), ioPart.msg.client, ioPart.msg);
			
			ioPart.msg = null;
		}				
		
		return true;
	}
	
	//TODO: 1. check that it works if object is transfered in several iterations
	//TODO: 2. how to handle if body size is more than ByteBuffer size ?
	void doRead(SelectionKey key) throws IOException {
		SelectionWrapper selectionWrapper = (SelectionWrapper) key.attachment();
		SocketChannel socketChannel = (SocketChannel) key.channel();
		
		InputPart ioPart = selectionWrapper.readPart;
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
				handleMessage(ioPart.msg);
				
				ioPart.msg = new Message();
			}	
		}								
	}
		
	private void handleMessage(Message msg) {										
		System.out.printf("%3d_%d. Read: %s%n", readNum.incrementAndGet(), msg.client, msg);		
		
		//add to working queue for further processing
		try {
			workQueue.put(msg);  //blocking
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
	}
	
	public void init() {
		
		System.out.printf("Working threads: %d%n", workThreads);
		
		ThreadFactory threadFactory = new ThreadFactory() {
			final AtomicInteger threadNumber = new AtomicInteger(0);
			
			@Override
			public Thread newThread(Runnable r) {
				String name = "work-pool-" + threadNumber.incrementAndGet(); 
				Thread t = new Thread(r, name);
				return t;
			}
		};
		
		ExecutorService executors = Executors.newFixedThreadPool(workThreads, threadFactory);
		executors.execute(new Runnable() {			
			@Override
			public void run() {
				while (true) {
				
					try {
						Message msg = workQueue.take(); //blocked
						
						//do some calculations
						System.out.println("Executing: " + msg);
											
						MessageResponse response = new MessageResponse(msg.n1 + msg.n2, msg.client, msg.correlationId);
						responsesQueue.put(response);
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
