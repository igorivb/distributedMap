package com.service.impl;

import com.datastax.driver.mapping.Result;
import com.datastax.driver.mapping.annotations.Accessor;
import com.datastax.driver.mapping.annotations.Query;
import com.model.User;

@Accessor
public interface UserAccessor {

	@Query("select * from my_status.users")
	Result<User> getAllUsers();
}
