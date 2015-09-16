/*
 * Copyright (c) 2015 University of Massachusetts
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 * 
 * Initial developer(s): V. Arun
 */
package edu.umass.cs.gigapaxos.paxosutil;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Logger;

import edu.umass.cs.utils.Util;

/**
 * @author V. Arun
 * @param <NodeIDType>
 * 
 *            This class maintains a map of NodeIDType objects to integers, and
 *            provides some convenience methods to go back and forth between
 *            collections of the two kinds. It is synchronized because
 *            PaxosManager and Messenger (used by the BatchLogger thread in
 *            AbstractPaxosLogger) use this class.
 */
public class IntegerMap<NodeIDType> {
	/**
	 * -1 can not be used as a node ID.
	 */
	public static final Integer NULL_INT_NODE = -1;
	/**
	 * 
	 */
	public static final String NULL_STR_NODE = NULL_INT_NODE.toString();
	
	private HashMap<Integer, NodeIDType> nodeMap = new HashMap<Integer, NodeIDType>();
	private static boolean allInteger = true;

	private static Logger log = Logger.getLogger(IntegerMap.class.getName());
	
	/**
	 * Maps NodeIDType to int and stores the mapping
	 * 
	 * @param node
	 * @return Returns {@code int} corresponding to {@code node}.
	 */
	public int put(NodeIDType node) {
		assert (node != null);
		int id = getID(node);

		if (!node.toString().equals(Integer.valueOf(id).toString()))
			allInteger = false;

		// address hashcode collisions
		while (this.nodeMap.containsKey(id)
				&& !this.nodeMap.get(id).equals(node))
			id++;
		this.nodeMap.put(id, node);
		return id;
	}
	

	/**
	 * @return True if no non-integer node ID was ever encountered.
	 * If so, we can be slightly more efficient in messaging by
	 * avoiding node to int conversion and back.
	 */
	public static final boolean allInt() {
		return allInteger;
	}

	private static String message = ": Unable to translate integer ID "
			+ " to NodeIDType; this is likely a bug";

	// get(int) maps int to NodeIDType
	/**
	 * @param id
	 * @return Node ID corresponding to int id.
	 */
	public synchronized NodeIDType get(int id) {
		NodeIDType node = this.nodeMap.get(id);
		if (node == null) {
			log.severe(id + message);
			// this should never happen
			//assert (false) : id + message;
			throw new RuntimeException(id + message);
		}
		return node;
	}

	/**
	 * @param nodes
	 * @return Integer set corresponding to node ID set.
	 */
	public synchronized Set<Integer> put(Set<NodeIDType> nodes) {
		Set<Integer> intNodes = new HashSet<Integer>();
		for (NodeIDType node : nodes) {
			intNodes.add(put(node));
		}
		return intNodes;
	}

	/**
	 * @param intNodes
	 * @return Node ID set.
	 */
	public synchronized Set<NodeIDType> get(Set<Integer> intNodes) {
		Set<NodeIDType> nodes = new HashSet<NodeIDType>();
		for (int id : intNodes) {
			nodes.add(get(id));
		}
		return nodes;
	}

	/**
	 * @param members
	 * @return Node ID set.
	 */
	public synchronized Set<NodeIDType> getIntArrayAsNodeSet(int[] members) {
		Set<NodeIDType> nodes = new HashSet<NodeIDType>();
		for (int member : members) {
			NodeIDType node = this.nodeMap.get(member);
			nodes.add(node);
		}
		return nodes;
	}

	/*
	 * Might need to change this if hashcode is not unique. Generally, object
	 * equality => hashcode equality but not the other way round.
	 */
	private Integer getID(NodeIDType node) {
		if (node == null)
			return null;
		/*
		 * Relies on the following assumptions 1) an Integer's hashcode is the
		 * integer value itself, not a different integer value. This assumption
		 * is needed primarily because -1 is a special "invalid" node ID and
		 * encoding it to something else will break code.
		 * 
		 * 2) a String's hashcode does not change in the lifetime of a JVM. We
		 * do *not* need the assumption that the hashcode not change across
		 * reboots or be the same on different machines (which would be a bad
		 * assumption anyway).
		 */

		// Changed this to only return positive values - Westy
		/*
		 * We can't just return Math.abs(.) to get positive values because the
		 * ID -1 is special. It is used to mean an invalid node ID. Changing
		 * that to +1 breaks code.
		 */
		int hash = node.hashCode();
		return (hash != NULL_INT_NODE ? Math.abs(node.hashCode()) : NULL_INT_NODE);
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Util.assertAssertionsEnabled();
		IntegerMap<String> map = new IntegerMap<String>();
		int id1 = map.put("hello1");
		int id2 = map.put("hello2");
		assert (id1 != id2);
		int id3 = map.put("hello1");
		assert (id1 == id3);
		InetAddress iaddr1 = null;
		InetAddress iaddr2 = null;
		InetAddress iaddr3 = null;
		InetAddress iaddr4 = null;

		try {
			iaddr1 = InetAddress.getByName("localhost");
			iaddr2 = InetAddress.getByName("plum.cs.umass.edu");
			iaddr3 = InetAddress.getByName("localhost");
			iaddr4 = InetAddress.getByName("plum.cs.umass.edu");
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}

		IntegerMap<InetAddress> iMap = new IntegerMap<InetAddress>();
		int id5 = iMap.put(iaddr1);
		int id6 = iMap.put(iaddr2);
		int id7 = iMap.put(iaddr3);
		int id8 = iMap.put(iaddr4);
		assert (id5 != id6);
		assert (id5 == id7);
		assert (id6 == id8);
		System.out.println("SUCCESS!");
	}
}
