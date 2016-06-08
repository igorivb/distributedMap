package com;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

//read and write. Connect to one server.
public class ClientServiceImpl implements ClientService {		

	private final static Logger logger = Logger.getLogger(ClientServiceImpl.class);
	
	
	final AtomicInteger readNum = new AtomicInteger();

	final AtomicInteger writeNum= new AtomicInteger();
	
	//messages to process (send to server)
	private BlockingQueue<Message> writeQueue = new ArrayBlockingQueue<>(10);
			
	
	//useb by all clients, as correlationIds are unique for all clients
	static final AtomicInteger correlationIds = new AtomicInteger(0);

	Map<Integer, ResponseFuture> correlations = new ConcurrentHashMap<>();
	
	
	private final int clientNum; //id of this client
	
	public ClientServiceImpl(int clientNum)  {
		this.clientNum = clientNum;				
	}		
	
	@Override
	public void init() {								
		Selector selector;
		try {
			selector = initConnections();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}			
		
		Thread ioThread = new Thread(() -> {
			try {
				//process messages in infinite loop
				while (true) {						
					selector.select();
					
					Set<SelectionKey> keys = selector.selectedKeys();
					Iterator<SelectionKey> iter = keys.iterator();
					while (iter.hasNext()) {
						SelectionKey key = iter.next();
						iter.remove();
						
						if (key.isReadable()) {
							doRead(key);											
						} 
						
						if (key.isWritable()) {
							doWrite(key);		
						}			
					}
					
				}			
			} catch (Exception e) {
				throw new RuntimeException(e);
			}			
		}, "client-io-" + clientNum);	
		
		ioThread.start();
	}
		
	private void handleMessageResponse(MessageResponse msg) {										
		logger.info(String.format("%3d_%d. Read: %s%n", readNum.incrementAndGet(), msg.client, msg));		
		
		ResponseFuture future = correlations.get(msg.correlationId);
		if (future == null) {
			logger.error("No registered ResponseFuture for correlationId: " + msg.correlationId);
		} else {
			future.processResult(msg);
		}				
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
				handleMessageResponse(ioPart.msg);
				
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
			logger.info(String.format("%3d_%d. Sent: %s%n", writeNum.incrementAndGet(), ioPart.msg.client, ioPart.msg));
			
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
	
	Future<MessageResponse> handleMessage(Message msg) {
		try {
			writeQueue.put(msg);						
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
		
		ResponseFuture future = new ResponseFuture(this);
		correlations.put(msg.correlationId, future);
		
		return future;
	}
	
	@Override
	public int add(int n1, int n2) {
		try {
			Message msg = new Message(n1, n2, clientNum, correlationIds.incrementAndGet());
			
			Future<MessageResponse> fResponse = handleMessage(msg);
			MessageResponse response = fResponse.get(); //blocking to emulate sync call		
			return response.result;			
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public void onCorrelationProcessed(int correlationId) {
		correlations.remove(correlationId);		
	}

}
