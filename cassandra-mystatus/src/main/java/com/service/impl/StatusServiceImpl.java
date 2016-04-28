package com.service.impl;

import java.util.List;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.QueryLogger;
import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.MappingManager;
import com.datastax.driver.mapping.Result;
import com.model.User;
import com.service.StatusService;

public class StatusServiceImpl implements StatusService {

	private Cluster cluster;
	private Session session;
	private MappingManager manager;
		
	public StatusServiceImpl(String[] contactPoints) {
		
		//set other options if needed
		
		QueryLogger queryLogger = QueryLogger.builder()
			//.withConstantThreshold(...)
			//.withMaxQueryStringLength(...)
			.build();
		
		cluster = Cluster.builder()
			.addContactPoints(contactPoints)
	        .build();
	    
		cluster.register(queryLogger);
		
	    session = cluster.connect();
	    
	    manager = new MappingManager(session);	  	    	   
	}
	
	@Override
	public User createUser(User user) {	
		manager.mapper(User.class).save(user);
		return user;
	}
	
	@Override
	public List<User> getAllUsers() {
		UserAccessor userAccessor = manager.createAccessor(UserAccessor.class);
		Result<User> res = userAccessor.getAllUsers();
		return res.all();
	}

	@Override
	public User getUser(String userName) {
		return manager.mapper(User.class).get(userName);
	}

	@Override
	public void deleteUser(String userName) {		
		manager.mapper(User.class).delete(userName);
	}
	
	@Override
	public void close() {
		session.close();
		cluster.close(); 
	}	
}
