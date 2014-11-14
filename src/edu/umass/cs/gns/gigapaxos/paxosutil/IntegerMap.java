package edu.umass.cs.gns.gigapaxos.paxosutil;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

/**
 * @author V. Arun
 */
/*
 * This class maintains a map of NodeIDType objects
 * to integers, and provides some convenience methods
 * to go back and forth between collections of the
 * two kinds. It is synchronized because PaxosManager
 * and Messenger (used by the BatchLogger thread in
 * AbstractPaxosLogger) use this class.
 */
public class IntegerMap<NodeIDType> {
	private HashMap<Integer, NodeIDType> nodeMap =
			new HashMap<Integer, NodeIDType>();

	// put(NodeIDType) maps NodeIDType to int and stores the mapping
	public int put(NodeIDType node) {
		assert (node != null);
		int id = getID(node);
		assert (!this.nodeMap.containsKey(id) || this.nodeMap.get(id).equals(
			node));
		this.nodeMap.put(id, node);
		return id;
	}

	// get(int) maps int to NodeIDType
	public synchronized NodeIDType get(int id) {
		NodeIDType node = this.nodeMap.get(id);
		if (node == null) {
			System.out.println("!!!!!!!!Unable to get " + id);
			assert (false); // FIXME: What to do here?
		}
		return node;
	}

	public synchronized Set<Integer> put(Set<NodeIDType> nodes) {
		Set<Integer> intNodes = new HashSet<Integer>();
		for (NodeIDType node : nodes) {
			intNodes.add(put(node));
		}
		return intNodes;
	}

	public synchronized Set<NodeIDType> get(Set<Integer> intNodes) {
		Set<NodeIDType> nodes = new HashSet<NodeIDType>();
		for (int id : intNodes) {
			nodes.add(get(id));
		}
		return nodes;
	}

	public synchronized Set<NodeIDType> getIntArrayAsNodeSet(int[] members) {
		Set<NodeIDType> nodes = new HashSet<NodeIDType>();
		for (int member : members) {
			NodeIDType node = this.nodeMap.get(member);
			nodes.add(node);
		}
		return nodes;
	}

	/*
	 * Might need to change this if hashcode is not unique.
	 * Generally, object equality => hashcode equality but
	 * not the other way round.
	 */
	private Integer getID(NodeIDType node) {
		if (node == null) return null;
		/*
		 * Relies on the following assumptions
		 * 1) an Integer's hashcode is the integer value
		 * itself, not a different integer value
		 * 
		 * 2) a String's hashcode does not change in the
		 * lifetime of a JVM. We do *not* need the
		 * assumption that the hashcode not change
		 * across reboots or be the same on different
		 * machines (which would be a bad assumption
		 * anyway).
		 */
		return node.hashCode();
	}

	public static void main(String[] args) {
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
