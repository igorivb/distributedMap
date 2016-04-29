package com.service.impl;

import java.nio.ByteBuffer;
import java.util.List;

import org.cassandraunit.CassandraCQLUnit;
import org.cassandraunit.dataset.cql.ClassPathCQLDataSet;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.model.StatusUpdate;
import com.model.User;
import com.service.PagedResult;
import com.service.StatusService;

/**
 * Use embedded Cassandra instance started in the same JVM. 
 */
public class StatusServiceImplEmbeddedTest {
	
	//TODO what is rule?
	@Rule
    public CassandraCQLUnit cassandraCQLUnit = new CassandraCQLUnit(new ClassPathCQLDataSet("simple.cql","my_status"));

	
	private StatusService statusService;
	
	@Before
	public void setUp() throws Exception {						
		statusService = new StatusServiceImpl();		
		((StatusServiceImpl) statusService).setSession(cassandraCQLUnit.getSession());
		statusService.init();		
	}
	
	@After
	public void tearDown() {
		if (statusService != null) {
			statusService.close();	
		}
	}
	
	@Test
	public void testCreateUser() {
		User alice = createUser("alice");
		statusService.createUser(alice);
				
		User res = statusService.getUser("alice");
		Assert.assertNotNull(res);
		Assert.assertEquals(alice, res);					
	}
	
	@Test
	public void testGetAllUsers() {			
		User alice = createUser("alice");
		statusService.createUser(alice);
		
		User bob = createUser("bob");
		statusService.createUser(bob);
				
		List<User> users = statusService.getAllUsers();
		Assert.assertEquals(2, users.size());	
	}
	
	@Test
	public void testGetUsersPages() {
		final int usersNum = 11;
		final int pageSize = 2;
		
		//create users		
		for (int i = 0; i < usersNum; i ++) {
			statusService.createUser(createUser("user_" + i));			
		}
				
		int iterCount = (int) Math.ceil((double) usersNum / pageSize);
		String pageState = null;
		
		int j;
		for (j = 0; j < iterCount; j ++) {								
			PagedResult<User> usersList = statusService.getUsers(pageSize, pageState);				
			pageState = usersList.getPageState();
			
			if (j < iterCount - 1) {
				Assert.assertEquals(pageSize, usersList.getList().size());
				Assert.assertNotNull(usersList.getPageState());
			} else { //last element
				Assert.assertEquals(usersNum - pageSize * j, usersList.getList().size());
				Assert.assertNull(usersList.getPageState());
			}								
		}
		
		Assert.assertEquals(iterCount, j);						
	}
	
	@Test
	public void testCreateStatusUpdate() {
		StatusUpdate status = createStatusUpdate("alice", "alice 1");
		statusService.createStatusUpdate(status);
		
		StatusUpdate result = statusService.getStatusUpdate(status.getUserName(), status.getId());
		Assert.assertNotNull(result);
		Assert.assertEquals(status, result);
	}
	
	@Test
	public void testGetUserStatusUpdates() {
		final int statusNum = 11;
		final int pageSize = 2;
		
		//create statuses		
		String userName = "alice";
		for (int i = 0; i < statusNum; i ++) {
			statusService.createStatusUpdate(createStatusUpdate(userName, userName + " " + i));
		}
				
		int iterCount = (int) Math.ceil((double) statusNum / pageSize);
		String pageState = null;
		
		int j;
		for (j = 0; j < iterCount; j ++) {								
			PagedResult<StatusUpdate> resultList = statusService.getUserStatusUpdates(userName, pageSize, pageState);
			pageState = resultList.getPageState();
			
			if (j < iterCount - 1) {
				Assert.assertEquals(pageSize, resultList.getList().size());
				Assert.assertNotNull(resultList.getPageState());
			} else { //last element
				Assert.assertEquals(statusNum - pageSize * j, resultList.getList().size());
				Assert.assertNull(resultList.getPageState());
			}								
		}
		
		Assert.assertEquals(iterCount, j);					
	} 
	
	static StatusUpdate createStatusUpdate(String userName, String body) {
		return new StatusUpdate(userName, body);
	}
	
	static User createUser(String userName) {
		User user = new User(userName, userName + "@gmail.com", ByteBuffer.wrap(userName.getBytes()));						
		//user.setVersion(UUID.randomUUID());
		//user.setLocation(location);
		return user;
	}
}
