package com.service.impl;

import java.util.List;

import com.datastax.driver.core.PagingState;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.mapping.MappingManager;
import com.datastax.driver.mapping.Result;
import com.model.User;
import com.service.StatusService;
import com.service.UsersList;

public class StatusServiceImpl implements StatusService {

	private Session session;
	
	//TODO use one instance of MappingManager
	/*
	 * MappingManager is thread-safe and can be safely shared throughout your application. 
	 * You would typically create one instance at startup, right after your Session
	 */
	private MappingManager manager;
		
	public StatusServiceImpl() { }
	
	//One session may be used by many services.
	public void setSession(Session session) {
		this.session = session;
	}
	
	@Override
	public void init() {		
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
	public UsersList getUsersPages(int pageSize, String pageState) {		
		Statement st = new SimpleStatement("select * from my_status.users");
		st.setFetchSize(pageSize);
		if (pageState != null) {
			st.setPagingState(PagingState.fromString(pageState));	
		}
						
		ResultSet resultSet = session.execute(st);
		
		
		UsersList usersList = new UsersList();
		PagingState ps = resultSet.getExecutionInfo().getPagingState();
		if (ps != null) {
			usersList.setPageState(ps.toString());	
		}
						
		Result<User> result = manager.mapper(User.class).map(resultSet);
						
		int remaining = pageSize;				
		for (User user : result) {
			usersList.getUsers().add(user);
			
			if (--remaining == 0) {
				break;
			}
		}
				
		return usersList;		
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
	public void close() { }	
}
