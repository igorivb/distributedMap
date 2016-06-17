package com.io;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import com.Message;
import com.MessageResponse;
import com.server.MyServer;

public class IoServiceImpl implements MyIoService {
	
	private final static Logger logger = Logger.getLogger(IoServiceImpl.class);

	//TODO: which implementation is used in Hz ? 
	//messages to process (messages that we read from client)
	private BlockingQueue<Message> workQueue = new ArrayBlockingQueue<>(10);

	
	public IoServiceImpl() {
		
		ThreadFactory threadFactory = new ThreadFactory() {
			final AtomicInteger threadNumber = new AtomicInteger(0);
			
			@Override
			public Thread newThread(Runnable r) {
				String name = "work-pool-" + threadNumber.incrementAndGet(); 
				Thread t = new Thread(r, name);
				return t;
			}
		};
		
		ExecutorService executors = Executors.newFixedThreadPool(MyServer.workThreadsNum, threadFactory);
		executors.execute(new Runnable() {			
			@Override
			public void run() {
				while (true) {
				
					try {
						Message msg = workQueue.take(); //blocked
						
						//do some calculations
						logger.info("Executing: " + msg);
		
						
						MessageResponse response = new MessageResponse(msg.n1 + msg.n2, msg.client, msg.correlationId, msg.con);
						response.con.write(response);
						
												
					} catch (Exception e) {
						throw new RuntimeException(e);
					}										
				}				
			}
		});
	}
	
	@Override
	//called by multiple InSelectors
	public void handleMessage(Message msg) {																
		//add to working queue for further processing
		try {
			workQueue.put(msg);  //blocking
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
	}
}
