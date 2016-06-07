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

	static class MessageWrapper {
		ByteBuffer buf;
		Message msg;		
		public MessageWrapper(ByteBuffer buf, Message msg) {
			super();
			this.buf = buf;
			this.msg = msg;
		}		
	}
	
	private BlockingQueue<Message> writeQueue = new ArrayBlockingQueue<>(10);
	
	final int clientThreads = 2;
	
	final AtomicInteger correlationIds = new AtomicInteger();
	
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
	
	int writeNum = 0;
	
	public void exec() throws Exception {		
		generateRequests();					
					
		Selector selector = initConnections();			
		
		//process messages in infinite loop
		while (true) {						
			selector.select();
			
			Set<SelectionKey> keys = selector.selectedKeys();
			Iterator<SelectionKey> iter = keys.iterator();
			while (iter.hasNext()) {
				SelectionKey key = iter.next();
				SocketChannel socketChannel = (SocketChannel) key.channel();							
				MessageWrapper msgWrapper = (MessageWrapper) key.attachment();
				ByteBuffer buf = msgWrapper.buf;
				
				if (key.isWritable()) {
				
					if (msgWrapper.msg == null) { //get new element to process
						Message msg = msgWrapper.msg = writeQueue.poll(); //non blocking
						if (msg != null) {
							//System.out.println("Start processing message: " + msg);																
							
							buf.putInt(msg.n1);
							buf.putInt(msg.n2);
							buf.putInt(msg.client);
							buf.putInt(msg.correlationId);
							
							buf.flip();
						}
					}
					
					if (msgWrapper.msg != null) {
						socketChannel.write(buf);	//if server closed connection: 	IOException: Broken pipe									
						
						if (!buf.hasRemaining()) {
							System.out.printf("%3d_%d. Sent: %s%n", writeNum++ + 1, msgWrapper.msg.client, msgWrapper.msg);
							
							msgWrapper.msg = null;						
							buf.clear();							
						}
						
						iter.remove();
					}																						
				}
				
				if (key.isReadable()) {
					//TODO: implement later
				}																	
			}
			
		}		
		
		//executorService.shutdownNow();		
	}

	Selector initConnections() throws IOException {
		SocketAddress serverAddress = new InetSocketAddress("localhost", MyServer.SERVER_PORT);
		
		SocketChannel socketChannel =  SocketChannel.open();
		
		//TODO socketChannel.setOption(name, value);
		
		socketChannel.connect(serverAddress); //connect in blocking
		
		socketChannel.configureBlocking(false); //non blocking
		
		final Selector selector = Selector.open();
		SelectionKey key = socketChannel.register(selector, SelectionKey.OP_READ | SelectionKey.OP_WRITE); //register for read and write
				
		ByteBuffer buf = ByteBuffer.allocate(16);
		key.attach(new MessageWrapper(buf, null));
		
		return selector;
	}
	
	public static void main(String[] args) throws Exception {
		MyClient client = new MyClient();
		client.exec();
	}

}
