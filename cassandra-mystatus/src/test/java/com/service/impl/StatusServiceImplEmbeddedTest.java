package com.service.impl;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import org.cassandraunit.CassandraCQLUnit;
import org.cassandraunit.dataset.cql.ClassPathCQLDataSet;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.model.HomeStatusUpdate;
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
			
//			for (StatusUpdate su : resultList.getList()) {
//				System.out.printf("User: %s | %s | %s%n", su.getUserName(), su.getBody(), UUIDs.unixTimestamp(su.getId()));
//			}
		}
		
		Assert.assertEquals(iterCount, j);					
	} 
	
	@Test
	public void testGetFollowerUsers() {
		String alice = "alice";
		statusService.createFollow(alice, "bob");
		statusService.createFollow(alice, "dave");
		statusService.createFollow(alice, "cat");		
		statusService.createFollow("sam", alice);
				
		List<String> followers = statusService.getFollowerUsers(alice);
		Assert.assertEquals(3, followers.size());
		
		List<String> followed = statusService.getFollowedUsers(alice);
		Assert.assertEquals(1, followed.size());
		
	}
	
	@Test
	public void testGetFollowedUsers() {
		String alice = "alice";
		statusService.createFollow("bob", alice);
		statusService.createFollow("dave", alice);
		statusService.createFollow("cat", alice);		
		statusService.createFollow(alice, "sam");
				
		List<String> followers = statusService.getFollowerUsers(alice);
		Assert.assertEquals(1, followers.size());
		
		List<String> followed = statusService.getFollowedUsers(alice);
		Assert.assertEquals(3, followed.size());
		
	}
		
	@Test
	public void testGetHomeStatusUpdates1() {
		String alice = "alice";
		statusService.createFollow(alice, "bob");
		statusService.createFollow(alice, "dave");
		statusService.createFollow(alice, "cat");		
		statusService.createFollow("sam", alice);
		
		StatusUpdate statusUpdate = createStatusUpdate(alice, "alice update");
		statusService.createStatusUpdate(statusUpdate);
		
		final int maxResults = 5; 
		
		List<String> users = Arrays.asList("bob", "dave", "cat");
		for (String user : users) {
			List<HomeStatusUpdate> homeUpdates = statusService.getHomeStatusUpdates(user, maxResults);
			Assert.assertEquals(1, homeUpdates.size());
			
			HomeStatusUpdate hu = homeUpdates.get(0);
			compareHomeStatusUpdate(statusUpdate, hu);
		}
	}
	
	@Test
	public void testGetHomeStatusUpdatesSimple2() {
		String alice = "alice";
		statusService.createFollow("bob", alice);
		statusService.createFollow("dave", alice);
		statusService.createFollow("sam", alice);
		statusService.createFollow(alice, "cat");		
				
		StatusUpdate b1 = createStatusUpdate("bob", "bob 1");
		statusService.createStatusUpdate(b1);
		
		StatusUpdate d1 = createStatusUpdate("dave", "dave 1");
		statusService.createStatusUpdate(d1);
		
		StatusUpdate s1 = createStatusUpdate("sam", "sam 1");
		statusService.createStatusUpdate(s1);
		
		StatusUpdate c1 = createStatusUpdate("cat", "cat 1");
		statusService.createStatusUpdate(c1);
		
		StatusUpdate a1 = createStatusUpdate("alice", "alice 1");
		statusService.createStatusUpdate(a1);
		
		StatusUpdate b2 = createStatusUpdate("bob", "bob 2");
		statusService.createStatusUpdate(b2);
		
		final int maxResults = 5; 
		
		List<HomeStatusUpdate> homeUpdates = statusService.getHomeStatusUpdates(alice, maxResults);
		Assert.assertEquals(4, homeUpdates.size());
		
		for (HomeStatusUpdate hu : homeUpdates) {
			switch (hu.getStatusUpdateUserName()) {
				case "bob":	
					if (hu.getBody().contains("1")) {
						compareHomeStatusUpdate(b1, hu);
					} else {
						compareHomeStatusUpdate(b2, hu);
					}
					break;
				case "dave":
					compareHomeStatusUpdate(d1, hu);
					break;
				case "sam":
					compareHomeStatusUpdate(s1, hu);
					break;
				default:
					Assert.fail("Not expected user: " + hu.getStatusUpdateUserName());
			}
		}
	}
	
	static void compareHomeStatusUpdate(StatusUpdate su, HomeStatusUpdate hu) {
		Assert.assertEquals(su.getUserName(), hu.getStatusUpdateUserName());
		Assert.assertEquals(su.getId(), hu.getStatusUpdateId());
		Assert.assertEquals(su.getBody(), hu.getBody());
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
