package com;

import java.util.Random;

import org.apache.log4j.Logger;

public class ClientDriver {	
	
	private final static Logger logger = Logger.getLogger(ClientDriver.class);
	
	
	//default: 2
	static final int numClients = 2;
	
	
	static class Client extends Thread {		
		final int clientNum;
		
		final ClientService clientService; //each client has its own service
		
		Random rand = new Random(System.currentTimeMillis());			
						
		public Client(int clientNum) {
			super("main-client-" + clientNum);
			this.clientNum = clientNum;
			
			this.clientService = new ClientServiceImpl(this.clientNum);			
		}
		
		@Override
		public void run() {
			this.clientService.init();
			
			while (true) {										
				try {
					int n1 = rand.nextInt(10);
					int n2 = rand.nextInt(10);
					
					int res = clientService.add(n1, n2);
					
					logger.info(String.format("Calc. Client %2d: %d + %d = %d%n", clientNum, n1, n2, res));
					
					//wait some time between requests
					Thread.sleep(rand.nextInt(1000) + 100);
				} catch (InterruptedException e) {
					throw new RuntimeException(e);
				}
			}
		}
	}
	
	
	public void exec() {	
		logger.info("numClients: " + numClients);
		
		//simulate multiple clients with different connections
		for (int i = 0; i < numClients; i ++) {
			Client client = new Client(i);
			client.start();
		}						
	}
	
	public static void main(String[] args) {
		ClientDriver app = new ClientDriver();
		app.exec();					
	}
}
