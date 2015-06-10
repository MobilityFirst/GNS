package edu.umass.cs.reconfiguration.examples;

import java.util.Set;
import java.util.TreeSet;


/**
 * @author V. Arun
 */
@SuppressWarnings("javadoc")
public class TestConfig {
	public static final int numNodes = 3;
	public static final int startNodeID = 100;
	public static enum ServerSelectionPolicy {FIRST, RANDOM};
	public static final ServerSelectionPolicy serverSelectionPolicy = ServerSelectionPolicy.FIRST;

	public static Set<Integer> getNodes() {
		TreeSet<Integer> nodes = new TreeSet<Integer>();
		for(int i=startNodeID; i<startNodeID+numNodes; i++) {
			nodes.add(i);
		}
		return nodes;
	}
	
	public static void main(String[] args) {
	}
}
