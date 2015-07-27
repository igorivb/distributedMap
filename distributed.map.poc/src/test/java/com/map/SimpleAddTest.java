package com.map;

import org.junit.Test;

public class SimpleAddTest {

	/*
	 * Test first version of adding node.
	 * Should work without exceptions. 
	 */
	@Test
	public void testAddFirst() {
		int partitionsCount = 13;
		
		for (int replicationFactor = 0; replicationFactor < 7; replicationFactor ++) {		
					
			Cluster cluster = new Cluster(replicationFactor, partitionsCount);
			for (int i = 0; i < partitionsCount + 1; i ++) {
				cluster.addNode(new Node(i, null));
			}
			
			System.out.println();
			System.out.println();
			System.out.println("-------------------------------------------------------");
			System.out.println("-------------------------------------------------------");
			System.out.println("-------------------------------------------------------");
			System.out.println("-------------------------------------------------------");
			System.out.println();
			System.out.println();
		}
	}
}
