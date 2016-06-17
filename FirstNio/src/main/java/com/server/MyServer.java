package com.server;

import java.io.IOException;

import org.apache.log4j.Logger;

import com.io.IoServiceImpl;
import com.io.MyConnectionManager;
import com.io.MyIoService;
import com.io.tcp.MyTcpConnectionManager;

/*
 * TODO:
 *  1. server should remember to which client to send response.
 *  Now I get error when I send response to client that didn't provide me this message. 
 *    
 *  2. implement that we can have multiple clients
 *  	
 *  
 *  3. set correctly TCP options
 *  
 *  --------- Notes:
 *  
 */


//accept, read and write
public class MyServer {

	private final static Logger logger = Logger.getLogger(MyServer.class);
	
	//---------------- Config: start
	
	public final static int SERVER_PORT = 9898;
	
	public final static int workThreadsNum = 2;

	//TODO: default 2
	public final static int ioThreadsNum = 2;
	
	
	//---------------- Config: end
	
	
	public MyConnectionManager connectionManager;
	
	MyIoService ioService;
	
	
	
	public void exec() throws IOException {		
		logger.info(String.format("Working threads num: %d", workThreadsNum));
		
		ioService = new IoServiceImpl();
		
		connectionManager = createConnectionManager();
	}	

	//TODO: it should support multiple implementations
	protected MyConnectionManager createConnectionManager() throws IOException {
		MyTcpConnectionManager cm = new MyTcpConnectionManager(ioService);
		cm.init();
		
		return cm;
	}
	
	public static void main(String[] args) throws Exception {
		MyServer server = new MyServer();
		server.exec();
	}
	
}
