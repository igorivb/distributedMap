package com;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.SocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/*
 * TODO:   
 *  1. implement that we can have multiple clients
 *  
 *  3. set correctly TCP options
 *  
 *  --------- Notes:
 *  
 *  1. By default Hz has 3 threads for read, 3 threads for write and 1 thread for accept.
 *  Number of threads for read and write are configurable.
 *  Accept thread is always 1.
 */


//accept, read and write
public class MyServer {

	
	//---------------- Config: start
	
	public static int SERVER_PORT = 9898;
	
	final int workThreadsNum = 2;
	
	//TODO: default 2
	final int ioThreadsNum = 2;
	
	//---------------- Config: end
	
	
	InSelector[] inSelectors;
	int inSelectorsPos;
	
	OutSelector[] outSelectors;
	int outSelectorsPos;
	
	MySocketAcceptor socketAcceptor;
	
	
	
	
	public final AtomicInteger readNum = new AtomicInteger();
	
	public final AtomicInteger writeNum= new AtomicInteger();
	
	
	//TODO: which implementation is used in Hz ? 
	//messages to process (messages that we read from client)
	private BlockingQueue<Message> workQueue = new ArrayBlockingQueue<>(10);
	
	//processed messages (messages that we need to write to client)
	public BlockingQueue<MessageResponse> responsesQueue = new ArrayBlockingQueue<>(10);

	
	private ServerSocketChannel createServerSocketChannel() throws IOException {
		SocketAddress serverAddress = new InetSocketAddress(SERVER_PORT);
		
		ServerSocketChannel serverChannel = ServerSocketChannel.open();												
		ServerSocket ss = serverChannel.socket();
		ss.bind(serverAddress);												
		
		System.out.println("Listening in address: " + serverAddress);
		
		//TODO serverChannel.setOption(name, value)
		serverChannel.configureBlocking(false); //not blocking					
		
		return serverChannel;
	}
	
	public void exec() throws IOException {		
		System.out.printf("Working threads num: %d%n", workThreadsNum);
		System.out.printf("IO threads num: %d%n", ioThreadsNum);
		
		//create selectors
		
		ServerSocketChannel serverChannel = createServerSocketChannel();
		
		socketAcceptor = new MySocketAcceptor(serverChannel, this);		
		socketAcceptor.start();
		
		
		inSelectors = new InSelector[ioThreadsNum];
		outSelectors = new OutSelector[ioThreadsNum];
		
		for (int i = 0; i < ioThreadsNum; i ++) {
			inSelectors[i] = new InSelector(this, i);						
			inSelectors[i].start();
		}
		
		for (int i = 0; i < ioThreadsNum; i ++) {
			outSelectors[i] = new OutSelector(this, i);
			outSelectors[i].start();
		}
		
		
		
		
		ThreadFactory threadFactory = new ThreadFactory() {
			final AtomicInteger threadNumber = new AtomicInteger(0);
			
			@Override
			public Thread newThread(Runnable r) {
				String name = "work-pool-" + threadNumber.incrementAndGet(); 
				Thread t = new Thread(r, name);
				return t;
			}
		};
		
		ExecutorService executors = Executors.newFixedThreadPool(workThreadsNum, threadFactory);
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
		
		//TODO: remove
		try {
			Thread.sleep(100000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	//called only by 1 thread
	public void onNewConnection(SocketChannel socketChannel) throws IOException {
		InSelector inSelector = inSelectors[(inSelectorsPos ++) % inSelectors.length];
		OutSelector outSelector = outSelectors[(outSelectorsPos ++) % outSelectors.length];
		
		inSelector.register(socketChannel);
		outSelector.register(socketChannel);
	}
	
	//called by multiple InSelectors
	public void handleMessage(Message msg) {																
		//add to working queue for further processing
		try {
			workQueue.put(msg);  //blocking
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
	}
	
	public static void main(String[] args) throws Exception {
		MyServer server = new MyServer();
		server.exec();
	}

	
}
