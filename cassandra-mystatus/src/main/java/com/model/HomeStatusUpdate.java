package com.model;

import java.util.UUID;

import com.datastax.driver.mapping.annotations.ClusteringColumn;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;

@Table(
	keyspace = "my_status", 
	name = "home_status_updates" 	
)
public class HomeStatusUpdate {

	@PartitionKey
	@Column(name = "timeline_username")
	private String userName;
	
	@ClusteringColumn
	@Column(name = "status_update_id")
	private UUID statusUpdateId;
	
	@Column(name = "status_update_username")
	private String statusUpdateUserName;
	
	private String body;
	
	public HomeStatusUpdate() { }
	
	public HomeStatusUpdate(String userName, UUID statusUpdateId, String statusUpdateUserName, String body) {
		super();
		this.userName = userName;
		this.statusUpdateId = statusUpdateId;
		this.statusUpdateUserName = statusUpdateUserName;
		this.body = body;
	}

	public String getUserName() {
		return userName;
	}

	public void setUserName(String userName) {
		this.userName = userName;
	}

	public UUID getStatusUpdateId() {
		return statusUpdateId;
	}

	public void setStatusUpdateId(UUID statusUpdateId) {
		this.statusUpdateId = statusUpdateId;
	}

	public String getStatusUpdateUserName() {
		return statusUpdateUserName;
	}

	public void setStatusUpdateUserName(String statusUpdateUserName) {
		this.statusUpdateUserName = statusUpdateUserName;
	}

	public String getBody() {
		return body;
	}

	public void setBody(String body) {
		this.body = body;
	}
				
}
