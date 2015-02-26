package edu.umass.cs.gns.reconfiguration.reconfigurationutils;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import edu.umass.cs.gns.util.Util;

/**
 * @author V. Arun
 */

/*
 * A utility class with mostly static methods to help with consistent hashing
 * related functions.
 * 
 * It is okay to suppress warnings about unchecked types of serversArray objects
 * as they have to be of type NodeIDType.
 */
public class ConsistentHashing<NodeIDType> {

	private static final int DEFAULT_NUM_REPLICAS = 3;
	private static MessageDigest md;
	static {
		try {
			md = MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
	}
	private int numReplicas;
	private SortedMap<Integer, NodeIDType> servers = new TreeMap<Integer, NodeIDType>();

	public ConsistentHashing(NodeIDType[] servers) {
		this.refresh(servers, DEFAULT_NUM_REPLICAS);
	}

	public ConsistentHashing(NodeIDType[] servers, int numReplicas) {
		this.refresh(servers, numReplicas);
	}

	public ConsistentHashing(Set<NodeIDType> servers) {
		this.refresh(servers, DEFAULT_NUM_REPLICAS);
	}

	public void refresh(NodeIDType[] servers, int numReplicas) {
		for (NodeIDType server : servers)
			this.servers.put(hash(server.toString()), server);
		this.numReplicas = numReplicas;
	}

	public void refresh(Set<NodeIDType> servers, int numReplicas) {
		for (NodeIDType server : servers)
			this.servers.put(hash(server.toString()), server);
		this.numReplicas = numReplicas;
	}

	public void refresh(NodeIDType[] servers) {
		refresh(servers, DEFAULT_NUM_REPLICAS);
	}

	public void refresh(Set<NodeIDType> servers) {
		refresh(servers, DEFAULT_NUM_REPLICAS);
	}

	public Set<NodeIDType> getReplicatedServers(String name) {
		return this.getReplicatedServers(name, this.numReplicas);
	}
	
	public ArrayList<NodeIDType> getReplicatedServersArray(String name) {
		return this.getReplicatedServersArray(name, this.numReplicas);
	}

	public ArrayList<NodeIDType> getReplicatedServersArray(String name, int k) {
		int hash = hash(name);
		SortedMap<Integer, NodeIDType> tailMap = this.servers.tailMap(hash);
		Iterator<Integer> iterator = tailMap.keySet().iterator();
		ArrayList<NodeIDType> replicas = new ArrayList<NodeIDType>();
		for (int i = 0; i < k; i++) {
			if (!iterator.hasNext())
				iterator = this.servers.keySet().iterator();
			replicas.add(this.servers.get(iterator.next()));
		}
		return replicas;
	}

	protected NodeIDType getNode(String name) {
		int hash = hash(name);
		if (!this.servers.containsKey(hash)) {
			SortedMap<Integer, NodeIDType> tailMap = this.servers.tailMap(hash);
			hash = tailMap.isEmpty() ? this.servers.firstKey() : tailMap
					.firstKey();
		}
		return this.servers.get(hash);
	}
	
	private Set<NodeIDType> getReplicatedServers(String name, int k) {
		return new HashSet<NodeIDType>(this.getReplicatedServersArray(name, k));
	}


	/*
	 * Bad idea to use hashCode here because we need this hash to be consistent
	 * across platforms.
	 */
	private static int hash(String name) {
		byte[] digest = md.digest(name.getBytes());
		int hash = 0;
		for (int i = 0; i < digest.length; i++)
			hash = (hash ^ (digest[i] << (i % 4)));
		return hash;
	}

	// only for testing
	private Collection<NodeIDType> getServers() {
		return this.servers.values();
	}

	public static void main(String[] args) {
		Util.assertAssertionsEnabled();
		String[] names = { "World", "Hello", "Hello World", "1", "10", "12",
				"9", "34" };
		ConsistentHashing<String> ch = new ConsistentHashing<String>(names);
		System.out.println("Ring ordering = " + ch.getServers());
		for (int i = 0; i < names.length; i++) {
			String name = "fbdsjlfbnlsfs" + i;
			System.out.println("Replicated servers for " + name + " = "
					+ ch.getReplicatedServersArray(name, 4));
			assert (ch.getNode(names[i]).equals(names[i]));
		}
	}
}
