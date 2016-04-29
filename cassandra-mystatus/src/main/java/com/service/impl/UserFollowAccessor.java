package com.service.impl;

import com.datastax.driver.mapping.Result;
import com.datastax.driver.mapping.annotations.Accessor;
import com.datastax.driver.mapping.annotations.Query;

@Accessor
public interface UserFollowAccessor {

	@Query("select * from my_status.user_follows where followed_username = ?")
	Result<UserFollow> getInUsers(String userName);
	
	@Query("select * from my_status.user_follows where follower_username = ?")
	Result<UserFollow> getOutUsers(String userName);
}
