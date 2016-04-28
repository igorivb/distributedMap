package com.service;

import java.util.List;

import com.model.User;

public interface StatusService {

	User createUser(User user);
	
	//TODO: use paginated version
	List<User> getAllUsers();
	
	void close();
	
//	//TODO: make paginated
//	List<User> getAccounts();
}
