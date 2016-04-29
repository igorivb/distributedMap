package com.model;

import java.nio.ByteBuffer;

import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;

@Table(
	keyspace = "my_status", 
	name = "users", 
	writeConsistency = "QUORUM", readConsistency = "QUORUM"
)
public class User {
	
	@PartitionKey
	@Column(name = "username")
	private String userName;
		
	private String email;
	
	@Column(name = "encrypted_password")
	private ByteBuffer encryptedPassword;
	
//	//TODO: use these fields in tests
//    private String location;       
//    private UUID version;        

    public User() { }
    
	public User(String userName, String email, ByteBuffer encryptedPassword) {
		super();
		this.userName = userName;
		this.email = email;
		this.encryptedPassword = encryptedPassword;
	}

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

//	public String getLocation() {
//		return location;
//	}
//
//	public void setLocation(String location) {
//		this.location = location;
//	}
//
//	public UUID getVersion() {
//		return version;
//	}
//
//	public void setVersion(UUID version) {
//		this.version = version;
//	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((email == null) ? 0 : email.hashCode());
		result = prime
				* result
				+ ((encryptedPassword == null) ? 0 : encryptedPassword
						.hashCode());		
		result = prime * result
				+ ((userName == null) ? 0 : userName.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		User other = (User) obj;
		if (email == null) {
			if (other.email != null)
				return false;
		} else if (!email.equals(other.email))
			return false;
		if (encryptedPassword == null) {
			if (other.encryptedPassword != null)
				return false;
		} else if (!encryptedPassword.equals(other.encryptedPassword))
			return false;		
		if (userName == null) {
			if (other.userName != null)
				return false;
		} else if (!userName.equals(other.userName))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "User [userName=" + userName + ", email=" + email
				//+ ", location=" + location 
				+ "]";
	}		
    
}
