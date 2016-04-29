package com.service.impl;

import java.nio.ByteBuffer;
import java.util.List;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.QueryLogger;
import com.datastax.driver.core.Session;
import com.model.User;
import com.service.PagedResult;
import com.service.StatusService;

/**
 * Use local Cassandra instance, which is already installed. 
 */
public class StatusServiceImplLocalTest {

	private static Cluster cluster;
	private static Session session;
	
	private static StatusService statusService;
	
	@BeforeClass
	public static void setUp() {
		String[] contactPoints = { "127.0.0.1" };
		
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
		
		statusService = new StatusServiceImpl();		
		((StatusServiceImpl) statusService).setSession(session);
		statusService.init();
	}
	
	@AfterClass
	public static void tearDown() {		
		if (statusService != null) {
			statusService.close();	
		}
		
		if (session != null) {
			session.close();	
		}
		
		if (cluster != null) {
			cluster.close();	
		}		
	}
	
	@Test
	public void testCreateUser() {
		User alice = createUser("alice");
		statusService.createUser(alice);
		
		try {
			User res = statusService.getUser("alice");
			Assert.assertNotNull(res);
			Assert.assertEquals(alice, res);			
		} finally {
			statusService.deleteUser(alice.getUserName());
		}
	}
	
	@Test
	public void testGetAllUsers() {
		User alice = createUser("alice");
		statusService.createUser(alice);
		
		User bob = createUser("bob");
		statusService.createUser(bob);
		
		try {
			List<User> users = statusService.getAllUsers();
			Assert.assertEquals(2, users.size());
		} finally {
			statusService.deleteUser(alice.getUserName());
			statusService.deleteUser(bob.getUserName());
		}				
	}
	
	@Test
	public void testGetUsersPages() {
		final int usersNum = 11;
		final int pageSize = 2;
		
		//create users		
		for (int i = 0; i < usersNum; i ++) {
			statusService.createUser(createUser("user_" + i));			
		}
		
		try {
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
				
		} finally {
			//delete users
			for (int i = 0; i < usersNum; i ++) {
				statusService.deleteUser("user_" + i);
			}
		}				
	}
	
	public User createUser(String userName) {
		User user = new User(userName, userName + "@gmail.com", ByteBuffer.wrap(userName.getBytes()));						
		//user.setVersion(UUID.randomUUID());
		//user.setLocation(location);
		return user;
	}
}
