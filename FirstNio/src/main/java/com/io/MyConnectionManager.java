package com.io;

import java.io.IOException;

public interface MyConnectionManager {

	void registerConnection(MyConnection con) throws IOException;
	
	void removeConnection(MyConnection con);

}
