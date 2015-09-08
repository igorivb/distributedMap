package com.map;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class Utils {

	/**
	 * Copy object. 
	 * Object should implement Serializable.
	 * 
	 * TODO: assume we will not need this method in remote environment. 
	 */
	public static <T> T copy(T obj, Class<T> clazz) {
		try {
			ByteArrayOutputStream bout = new ByteArrayOutputStream();		
			ObjectOutputStream out = new ObjectOutputStream(bout);
			out.writeObject(obj);
			
			ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bout.toByteArray()));
			
			Object copy = in.readObject();
			return clazz.cast(copy);			
		} catch (ClassNotFoundException | IOException e) {
			throw new RuntimeException("Failed to copy object", e);
		}
	}
}
