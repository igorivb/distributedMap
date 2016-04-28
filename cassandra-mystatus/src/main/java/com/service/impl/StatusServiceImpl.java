package com.service.impl;

import java.util.List;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.model.User;
import com.service.StatusService;

public class StatusServiceImpl implements StatusService {

	private Cluster cluster;
	private Session session;
	private MappingManager manager;
		
	public StatusServiceImpl(String[] contactPoints) {
		
		//set other options if needed
		
		cluster = Cluster.builder()
			.addContactPoints(contactPoints)
	        .build();
	    
	    session = cluster.connect();
	    
	    manager = new MappingManager(session);	  	    	   
	}
	
	@Override
	public User createUser(User user) {	
		Mapper<User>  mapper = manager.mapper(User.class);
		mapper.save(user);
		return user;
	}
	
	@Override
	public List<User> getAllUsers() {
		UserAccessor userAccessor = manager.createAccessor(UserAccessor.class);
		return userAccessor.getAllUsers();
	}

	@Override
	public void close() {
		session.close();
		cluster.close(); 
	}
}
