package com.io.tcp;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Set;

import org.apache.log4j.Logger;

public class MySocketAcceptor extends Thread {

	private final static Logger logger = Logger.getLogger(MySocketAcceptor.class);
	
	private final ServerSocketChannel serverChannel;
	
	
	private final MyTcpConnectionManager connectionManager;
	
	public MySocketAcceptor(ServerSocketChannel serverChannel, MyTcpConnectionManager connectionManager) {
		super("accept-selector");
		
		this.serverChannel = serverChannel;
	
		this.connectionManager = connectionManager;
	}
	
	@Override
	public void run() {
		try {
			Selector selector = Selector.open();
			serverChannel.register(selector, SelectionKey.OP_ACCEPT); //register for accept
			
			while (true) {
				
				selector.select(); //blocking
				
				Set<SelectionKey> keys = selector.selectedKeys();
				Iterator<SelectionKey> iter = keys.iterator();								
				
				while (iter.hasNext()) {										
					SelectionKey key = iter.next();
														
					if (key.isAcceptable()) {
						iter.remove();	
						
						doAccept(key);						
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
		
		logger.info("Accept client connection: " + socketChannel.getRemoteAddress());
		
		MyTcpConnection con = new MyTcpConnection(socketChannel, connectionManager);		
		connectionManager.registerConnection(con);						
	}		

}
