package com.service;

import java.util.List;

import com.model.User;

public interface StatusService {

	//------------------ User
	
	User createUser(User user);
	
	User getUser(String userName);
	
	void deleteUser(String userName);	
	
	//TODO: use paginated version
	List<User> getAllUsers();
	
	
	
	
	void close();
	
//	//TODO: make paginated
//	List<User> getAccounts();
}
