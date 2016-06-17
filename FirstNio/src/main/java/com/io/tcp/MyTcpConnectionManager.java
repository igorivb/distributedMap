package com.io.tcp;

import static com.server.MyServer.ioThreadsNum;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.SocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import com.Message;
import com.io.MyConnection;
import com.io.MyConnectionManager;
import com.io.MyIoService;
import com.server.MyServer;

/**
 * --------- Notes:
 * 
 *  1. By default Hz has 3 threads for read, 3 threads for write and 1 thread for accept.
 *  Number of threads for read and write are configurable.
 *  Accept thread is always 1.
 *
 */
public class MyTcpConnectionManager implements MyConnectionManager {
	
	private final static Logger logger = Logger.getLogger(MyTcpConnectionManager.class);
	
	
	final MyIoService ioService;
	
	
	InSelector[] inSelectors;
	int inSelectorsPos;
	
	OutSelector[] outSelectors;
	int outSelectorsPos;
	
	MySocketAcceptor socketAcceptor;
	
	
	//maintain a list of active connections
	List<MyConnection> connections = new ArrayList<>();
	
	
	public final AtomicInteger readNum = new AtomicInteger();
	
	public final AtomicInteger writeNum= new AtomicInteger();
	
	
	public MyTcpConnectionManager(MyIoService ioService) {
		this.ioService = ioService;
	}
	
	
	public void init() throws IOException {
		logger.info("IO threads num: " + ioThreadsNum);
		
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
	}
	
	private ServerSocketChannel createServerSocketChannel() throws IOException {
		SocketAddress serverAddress = new InetSocketAddress(MyServer.SERVER_PORT);
		
		ServerSocketChannel serverChannel = ServerSocketChannel.open();												
		ServerSocket ss = serverChannel.socket();
		ss.bind(serverAddress);												
		
		logger.info("Listening in address: " + serverAddress);
		
		//TODO serverChannel.setOption(name, value)
		serverChannel.configureBlocking(false); //not blocking					
		
		return serverChannel;
	}

	public void handleMessage(Message msg) {
		ioService.handleMessage(msg);		
	}
	
	//called only by 1 thread
	@Override
	public void registerConnection(MyConnection con) throws IOException {
		InSelector inSelector = inSelectors[(inSelectorsPos ++) % inSelectors.length];
		OutSelector outSelector = outSelectors[(outSelectorsPos ++) % outSelectors.length];
		
		connections.add(con);
		
		inSelector.register(con);
		outSelector.register(con);
	}

	@Override
	public void removeConnection(MyConnection con) {
		// TODO: implement
		
	}
}
