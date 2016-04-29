package com.service;

import java.util.ArrayList;
import java.util.List;

import com.model.User;

//TODO: make it generic for other classes ?
public class UsersList {

	private List<User> users = new ArrayList<>();
	private String pageState;
	
	public List<User> getUsers() {
		return users;
	}

	public String getPageState() {
		return pageState;
	}

	public void setPageState(String pageState) {
		this.pageState = pageState;
	}		
}
