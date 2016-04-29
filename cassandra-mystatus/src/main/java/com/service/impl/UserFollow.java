package com.service.impl;

import com.datastax.driver.mapping.annotations.ClusteringColumn;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;

@Table(
	keyspace = "my_status", 
	name = "user_follows" 	
)
public class UserFollow {

	@PartitionKey
	@Column(name = "followed_username")
	private String followedUserName;
	
	@ClusteringColumn
	@Column(name = "follower_username")
	private String followerUserName;

	public UserFollow() { }
	
	public UserFollow(String followedUserName, String followerUserName) {
		super();
		this.followedUserName = followedUserName;
		this.followerUserName = followerUserName;
	}

	public String getFollowedUserName() {
		return followedUserName;
	}

	public void setFollowedUserName(String followedUserName) {
		this.followedUserName = followedUserName;
	}

	public String getFollowerUserName() {
		return followerUserName;
	}

	public void setFollowerUserName(String followerUserName) {
		this.followerUserName = followerUserName;
	}		
}
