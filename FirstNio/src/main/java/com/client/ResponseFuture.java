package com.client;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.MessageResponse;

/*
 * TODO:
 * 
 * 1. implement all methods
 * 
 * 2. Check how Hz implements it
 */
public class ResponseFuture implements Future<MessageResponse> {

	final ClientServiceImpl clientService;
	
	//TODO make it AtomicReference or volatile as it is accessible by multiple threads
	private MessageResponse msg;
	
	public ResponseFuture(ClientServiceImpl clientService) {
		this.clientService = clientService;
	}
	
	//called when we get result from server
	public void processResult(MessageResponse msg) {
		boolean processed = false;
		synchronized (this) {
			if (!isDone()) {
				this.msg = msg; 
				
				this.notifyAll();
				
				processed = true;			
			}		
		}
		
		//remove correlationId from map when we get result
		if (processed) {
			clientService.onCorrelationProcessed(msg.correlationId);
		}
	}
	
	@Override
	public MessageResponse get() throws InterruptedException, ExecutionException {
		synchronized (this) {
			while (!isDone()) {
				this.wait();
			}	
		}
		return msg;
	}
	
	@Override
	public boolean isDone() {
		synchronized (this) {
			return msg != null;
		}
	}
	
	@Override
	public MessageResponse get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
		// TODO Auto-generated method stub
		return null;
	}
	
	@Override
	public boolean cancel(boolean mayInterruptIfRunning) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isCancelled() {
		// TODO Auto-generated method stub
		return false;
	}

}
