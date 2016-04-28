package com.model;

import java.nio.ByteBuffer;
import java.util.UUID;

import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;

@Table(
keyspace = "my_status", 
name = "users", 
writeConsistency = "QUORUM", readConsistency = "QUORUM")
public class User {

	@PartitionKey
	@Column(name = "username")
	private String userName;
		
	private String email;
	
	@Column(name = "encrypted_password")
	private ByteBuffer encryptedPassword;
	
    private String location;
        
    private UUID version;

	public String getUserName() {
		return userName;
	}

	public void setUserName(String userName) {
		this.userName = userName;
	}

	public String getEmail() {
		return email;
	}

	public void setEmail(String email) {
		this.email = email;
	}

	public ByteBuffer getEncryptedPassword() {
		return encryptedPassword;
	}

	public void setEncryptedPassword(ByteBuffer encryptedPassword) {
		this.encryptedPassword = encryptedPassword;
	}

	public String getLocation() {
		return location;
	}

	public void setLocation(String location) {
		this.location = location;
	}

	public UUID getVersion() {
		return version;
	}

	public void setVersion(UUID version) {
		this.version = version;
	}
    
}
