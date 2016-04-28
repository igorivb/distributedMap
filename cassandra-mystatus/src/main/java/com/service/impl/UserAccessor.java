package com.service.impl;

import java.util.List;

import com.datastax.driver.mapping.annotations.Accessor;
import com.datastax.driver.mapping.annotations.Query;
import com.model.User;

@Accessor
public interface UserAccessor {

	@Query("select * from user")
	List<User> getAllUsers();
}
