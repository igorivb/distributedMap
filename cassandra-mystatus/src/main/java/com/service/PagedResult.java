package com.service;

import java.util.ArrayList;
import java.util.List;

public class PagedResult<T> {

	private List<T> list = new ArrayList<>();
	private int pageSize;
	private String pageState;
	
	public PagedResult(int pageSize, String pageState) {
		this.pageSize = pageSize;
		this.pageState = pageState;
	}
	
	public List<T> getList() {
		return list;
	}

	public String getPageState() {
		return pageState;
	}

	public int getPageSize() {
		return pageSize;
	}
}
