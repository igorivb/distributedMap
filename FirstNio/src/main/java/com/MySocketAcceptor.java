package com;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Set;

public class MySocketAcceptor extends Thread {

	private final ServerSocketChannel serverChannel;
	private final MyServer server;
	
	public MySocketAcceptor(ServerSocketChannel serverChannel, MyServer server) {
		super("accept-selector");
		
		this.serverChannel = serverChannel;
		this.server = server;
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
		
		System.out.println("Accept client connection: " + socketChannel.getRemoteAddress());
		
		server.onNewConnection(socketChannel);						
	}		

}
