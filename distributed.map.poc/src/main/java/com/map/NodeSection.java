package com.map;

/**
 * Use enum instead of boolean to indicate primary and secondary for better code readability.
 */
public enum NodeSection {

	PRIMARY, SECONDARY;
	
	static NodeSection reverse(NodeSection section) {
		return section == PRIMARY ? SECONDARY : PRIMARY;
	}
}
