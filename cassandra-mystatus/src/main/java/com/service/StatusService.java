package com.service;

import java.util.List;

import com.model.User;

public interface StatusService {

	//------------------ User
	
	User createUser(User user);
	
	User getUser(String userName);
	
	void deleteUser(String userName);	
	
	/**
	 * This query is only for demo purposes. Use paged version instead.
	 */
	List<User> getAllUsers();
	
	/**
	 * If there are no more results, pagesState is null.
	 */
	UsersList getUsersPages(int pageSize, String pageState);
	
	
	void close();
}
