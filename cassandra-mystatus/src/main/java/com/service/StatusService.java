package com.service;

import java.util.List;
import java.util.UUID;

import com.model.HomeStatusUpdate;
import com.model.StatusUpdate;
import com.model.User;

public interface StatusService {

	//------------------ User
	
	void createUser(User user);
	
	User getUser(String userName);
	
	void deleteUser(String userName);	
	
	/**
	 * This query is only for demo purposes. Use paged version instead.
	 */
	List<User> getAllUsers();
	
	/**
	 * If there are no more results, pagesState is null.
	 */
	PagedResult<User> getUsers(int pageSize, String pageState);
	
	
	//------------------ Follow relationship
	
	void createFollow(String followedUsername, String followerUsername);		
	
	//void deleteFollow(String followedUsername, String followerUsername);
	
	/**
	 * Return all follower users.
	 */
	List<String> getFollowedUsers(String userName);
	
	/**
	 * Return all follower users. Core query access pattern.
	 */
	List<String> getFollowerUsers(String userName);
	
	
	//------------------ Status updates
	
	void createStatusUpdate(StatusUpdate status);
		
	PagedResult<StatusUpdate> getUserStatusUpdates(String userName, int pageSize, String pageState);
	
	StatusUpdate getStatusUpdate(String username, UUID id);
	
	/**
	 * Return last N status updates, so don't need pagination here.
	 */
	List<HomeStatusUpdate> getHomeStatusUpdates(String userName, int maxResults);
	
	//------------------ Lifecycle. Maybe we don't need it?
	
	void init();
	
	void close();
}
