package com;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

//read and write. Connect to one server.
public class MyClient {
	
	final int clientThreads = 1;
	
	final AtomicInteger readNum = new AtomicInteger();

	final AtomicInteger writeNum= new AtomicInteger();
	
	//messages to process (send to server)
	private BlockingQueue<Message> writeQueue = new ArrayBlockingQueue<>(10);
			
	final AtomicInteger correlationIds = new AtomicInteger(1);
	
	void generateRequests() {													
		ExecutorService executorService = Executors.newFixedThreadPool(clientThreads);
				
		for (int i = 0; i < clientThreads; i ++) {
			final int client = i;
			executorService.execute(new Runnable() {	
				Random rand = new Random(System.currentTimeMillis());
				
				@Override
				public void run() {
					while (true) {
						try {
							Message msg = new Message(rand.nextInt(10), rand.nextInt(10), client, correlationIds.incrementAndGet());								
							writeQueue.put(msg);
							
							//TODO: return Future<Message> to use
							
							//wait some time between requests
							Thread.sleep(rand.nextInt(1000) + 100);
							
						} catch (InterruptedException ie) {
							break;
						}
					}										
				}
			});
		}
	}		
	
	public void exec() throws Exception {	
		System.out.printf("Client threads: %d%n", clientThreads);
		
		generateRequests();					
					
		Selector selector = initConnections();			
		
		//process messages in infinite loop
		while (true) {						
			selector.select();
			
			Set<SelectionKey> keys = selector.selectedKeys();
			Iterator<SelectionKey> iter = keys.iterator();
			while (iter.hasNext()) {
				SelectionKey key = iter.next();
				boolean removeKey = false;
				
				if (key.isReadable()) {
					doRead(key);	
					
					removeKey = true;	
				} 
				
				if (key.isWritable()) {
					if (doWrite(key)) {
						removeKey = true;	
					}							
				}			
				
				if (removeKey) {
					iter.remove(); //remove so we don't process it twice	
				}
			}
			
		}		
		
		//executorService.shutdownNow();		
	}
		
	private void handleMessage(MessageResponse msg) {										
		System.out.printf("%3d_%d. Read: %s%n", readNum.incrementAndGet(), msg.client, msg);		
		
		//TODO: notify user by correlation id about result
	}
	
	void doRead(SelectionKey key) throws IOException {
		SelectionWrapper selectionWrapper = (SelectionWrapper) key.attachment();
		SocketChannel socketChannel = (SocketChannel) key.channel();
		
		OutputPart ioPart = selectionWrapper.writePart;
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
				
				ioPart.msg = new MessageResponse();
			}	
		}								
	}
	
	/**
	 * 
	 * @return true if selection key can be removed.
	 */
	boolean doWrite(SelectionKey key) throws IOException {
		SelectionWrapper selectionWrapper = (SelectionWrapper) key.attachment();
		SocketChannel socketChannel = (SocketChannel) key.channel();
		
		InputPart ioPart = selectionWrapper.readPart;
		ByteBuffer buf = ioPart.buf;				
		
		//get message to write
		if (ioPart.msg == null) {
			ioPart.msg = writeQueue.poll(); // non blocking
			if (ioPart.msg == null) {
				return false;
			}	
		}
		
		boolean isFull = ioPart.msg.write(buf);
		
		if (buf.position() == 0) {
			return false;
		}
		
		
		buf.flip();
		
		socketChannel.write(buf);  //if server closed connection: 	IOException: Broken pipe	
		
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
	
	/*
	 * Create one connection one, but it may connect to multiple servers.
	 */
	Selector initConnections() throws IOException {				
		SocketAddress serverAddress = new InetSocketAddress("localhost", MyServer.SERVER_PORT);
		
		SocketChannel socketChannel =  SocketChannel.open();
		
		//TODO socketChannel.setOption(name, value);
		
		socketChannel.connect(serverAddress); //connect in blocking
		
		socketChannel.configureBlocking(false); //non blocking
		
		final Selector selector = Selector.open();
		SelectionKey key = socketChannel.register(selector, SelectionKey.OP_READ | SelectionKey.OP_WRITE); //register for read and write
				
		//ByteBuffer buf = ByteBuffer.allocate(16);
		//key.attach(new MessageWrapper(buf, null));
		
		SelectionWrapper selectionWrapper = new SelectionWrapper();
		
		InputPart readPart = new InputPart();
		selectionWrapper.readPart = readPart;
		readPart.buf = ByteBuffer.allocate(16);
		readPart.msg = null;
		
		OutputPart writePart = new OutputPart();
		selectionWrapper.writePart = writePart;
		writePart.buf = ByteBuffer.allocate(12);
		writePart.msg = new MessageResponse();
		
		key.attach(selectionWrapper);		
		
		
		return selector;
	}
	
	public static void main(String[] args) throws Exception {
		MyClient client = new MyClient();
		client.exec();
	}

}
