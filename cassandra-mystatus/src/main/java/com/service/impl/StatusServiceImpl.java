package com.service.impl;

import java.util.List;
import java.util.UUID;

import com.datastax.driver.core.PagingState;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.mapping.MappingManager;
import com.datastax.driver.mapping.Result;
import com.model.HomeStatusUpdate;
import com.model.StatusUpdate;
import com.model.User;
import com.service.PagedResult;
import com.service.StatusService;

public class StatusServiceImpl implements StatusService {

	private Session session;
	
	//TODO use one instance of MappingManager
	/*
	 * MappingManager is thread-safe and can be safely shared throughout your application. 
	 * You would typically create one instance at startup, right after your Session
	 */
	private MappingManager manager;
		
	private PreparedStatement QUERY_GET_USER_STATUS_UPDATES;
	
	public StatusServiceImpl() { }
	
	//One session may be used by many services.
	public void setSession(Session session) {
		this.session = session;
	}
	
	@Override
	public void init() {		
		manager = new MappingManager(session);
				
		QUERY_GET_USER_STATUS_UPDATES = session.prepare("select * from my_status.user_status_updates where username = ?");		
	}
	
	@Override
	public void createUser(User user) {	
		manager.mapper(User.class).save(user);
	}
	
	@Override
	public List<User> getAllUsers() {
		UserAccessor userAccessor = manager.createAccessor(UserAccessor.class);
		Result<User> res = userAccessor.getAllUsers();
		return res.all();
	}
		
	@Override
	public PagedResult<User> getUsers(int pageSize, String pageState) {		
		Statement st = new SimpleStatement("select * from my_status.users");
		st.setFetchSize(pageSize);
		if (pageState != null) {
			st.setPagingState(PagingState.fromString(pageState));	
		}
						
		ResultSet resultSet = session.execute(st);
				
		PagingState ps = resultSet.getExecutionInfo().getPagingState();
		String newPageState = null;
		if (ps != null) {
			newPageState = ps.toString();	
		}
		PagedResult<User> usersList = new PagedResult<>(pageSize, newPageState);
							
		Result<User> result = manager.mapper(User.class).map(resultSet);
						
		int remaining = pageSize;				
		for (User user : result) {
			usersList.getList().add(user);
			
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

	@Override
	public void createStatusUpdate(StatusUpdate status) {
		manager.mapper(StatusUpdate.class).save(status);		
	}
	
	@Override
	public StatusUpdate getStatusUpdate(String username, UUID id) {
		return manager.mapper(StatusUpdate.class).get(username, id);
	}
				
	@Override
	public PagedResult<StatusUpdate> getUserStatusUpdates(String userName, int pageSize, String pageState) {						
		Statement st = QUERY_GET_USER_STATUS_UPDATES.bind(userName);
		st.setFetchSize(pageSize);
		if (pageState != null) {
			st.setPagingState(PagingState.fromString(pageState));	
		}
						
		ResultSet resultSet = session.execute(st);
		
		PagingState ps = resultSet.getExecutionInfo().getPagingState();
		String newPageState = null; 
		if (ps != null) {
			newPageState = ps.toString();	
		}
		PagedResult<StatusUpdate> statusList = new PagedResult<>(pageSize, newPageState);		
						
		Result<StatusUpdate> result = manager.mapper(StatusUpdate.class).map(resultSet);
						
		int remaining = pageSize;				
		for (StatusUpdate status : result) {
			statusList.getList().add(status);
			
			if (--remaining == 0) {
				break;
			}
		}
				
		return statusList;
	}
	
	@Override
	public void createFollow(String followedUsername, String followerUsername) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public List<String> getFollowedUsers(String userName) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<String> getFollowerUsers(String userName) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<HomeStatusUpdate> getHomeStatusUpdates(String userName) {
		// TODO Auto-generated method stub
		return null;
	}	
}
