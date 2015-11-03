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
package edu.umass.cs.reconfiguration.reconfigurationutils;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import edu.umass.cs.utils.Util;

/**
 * @author V. Arun
 * @param <NodeIDType> 
 * 
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
	
	private boolean replicateAll = false;
	private int numReplicas;
	private SortedMap<Integer, NodeIDType> servers = new TreeMap<Integer, NodeIDType>();

	/**
	 * @param servers
	 */
	public ConsistentHashing(NodeIDType[] servers) {
		this.refresh(servers, DEFAULT_NUM_REPLICAS);
	}

	/**
	 * @param servers
	 * @param numReplicas
	 */
	public ConsistentHashing(NodeIDType[] servers, int numReplicas) {
		this.refresh(servers, numReplicas);
	}

	/**
	 * @param servers
	 */
	public ConsistentHashing(Set<NodeIDType> servers) {
		this.refresh(servers, DEFAULT_NUM_REPLICAS);
	}
	/**
	 * @param servers
	 * @param replicateAll
	 */
	public ConsistentHashing(Set<NodeIDType> servers, boolean replicateAll) {
		this.replicateAll = replicateAll;
		this.refresh(servers, DEFAULT_NUM_REPLICAS);
	}

	/**
	 * @param servers
	 * @param numReplicas
	 */
	public void refresh(NodeIDType[] servers, int numReplicas) {
		this.servers = new TreeMap<Integer, NodeIDType>();
		for (NodeIDType server : servers)
			this.servers.put(hash(server.toString()), server);
		this.numReplicas = (replicateAll ? this.servers.size() : numReplicas);
	}

	/**
	 * @param servers
	 * @param numReplicas
	 */
	public void refresh(Set<NodeIDType> servers, int numReplicas) {
		this.servers = new TreeMap<Integer, NodeIDType>();
		for (NodeIDType server : servers)
			this.servers.put(hash(server.toString()), server);
		this.numReplicas = (replicateAll ? this.servers.size() : numReplicas);

	}

	/**
	 * @param servers
	 */
	public void refresh(NodeIDType[] servers) {
		refresh(servers, DEFAULT_NUM_REPLICAS);
	}

	/**
	 * @param servers
	 */
	public void refresh(Set<NodeIDType> servers) {
		refresh(servers, DEFAULT_NUM_REPLICAS);
	}

	/**
	 * @param name
	 * @return Consecutive servers on the consistent hash ring to which
	 * this name hashes.
	 */
	public Set<NodeIDType> getReplicatedServers(String name) {
		return this.getReplicatedServers(name, this.numReplicas);
	}
	
	/**
	 * @param name
	 * @return Consecutive servers on the consistent hash ring to which
	 * this name hashes returned as an array.
	 */
	public ArrayList<NodeIDType> getReplicatedServersArray(String name) {
		return this.getReplicatedServersArray(name, this.numReplicas);
	}

	/**
	 * @param name
	 * @param k
	 * @return {@code k} consecutive servers on the consistent hash ring to which
	 * this name hashes returned as an array.
	 */
	public ArrayList<NodeIDType> getReplicatedServersArray(String name, int k) {
		int hash = hash(name);
		//if(name.equals("1103")) System.out.println("hash(1103) = " + hash);
		SortedMap<Integer, NodeIDType> tailMap = this.servers.tailMap(hash);
		Iterator<Integer> iterator = tailMap.keySet().iterator();
		ArrayList<NodeIDType> replicas = new ArrayList<NodeIDType>();
		for (int i = 0; i < k; i++) {
			if (!iterator.hasNext())
				iterator = this.servers.keySet().iterator();
			replicas.add(this.servers.get(iterator.next()));
		}
		//if(name.equals("1103")) System.out.println("hash(1103) = " + hash);

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
	 * across platforms. Needs to be synchronized as md is not thread safe.
	 */
	private synchronized static int hash(String name) {
		byte[] digest = md.digest(name.getBytes());
		int hash = 0;
		for (int i = 0; i < digest.length; i++)
			hash = (hash ^ (digest[i] << (i % 4)));
		md.reset();
		return hash;
	}

	// only for testing
	private Collection<NodeIDType> getServers() {
		return this.servers.values();
	}

	 /**
	 * @param args
	 */
	public static void main(String[] args) {
		Util.assertAssertionsEnabled();
		String[] names = { "World", "Hello", "Hello World", "1", "10", "12",
				"9", "34" };
		ConsistentHashing<String> ch = new ConsistentHashing<String>(names);
		System.out.println("ring ordering = " + ch.getServers());
		for (int i = 0; i < names.length; i++) {
			String name = "fbdsjlfbnlsfs" + i;
			System.out.println("Replicated servers for " + name + " = "
					+ ch.getReplicatedServersArray(name, 4));
			assert (ch.getNode(names[i]).equals(names[i]));
		}
		
		Integer[] ids = {1100, 1101, 1102};
		ConsistentHashing<Integer> chi = new ConsistentHashing<Integer>(ids);
		System.out.println("ring ordering = " + chi.getServers());

		Integer[] ids1 = {1100, 1101, 1102, 19150};
		ConsistentHashing<Integer> chi1 = new ConsistentHashing<Integer>(ids1);
		System.out.println("ring ordering = " + chi1.getServers());

		String[] IDs = {"RC0", "RC1", "RC2"};
		ConsistentHashing<String> CHI = new ConsistentHashing<String>(IDs);
		System.out.println("ring ordering = " + CHI.getServers());

		String[] IDs1 = {"RC0", "RC1", "RC2", "RC317"};
		ConsistentHashing<String> CHI1 = new ConsistentHashing<String>(IDs1);
		System.out.println("ring ordering = " + CHI1.getServers());

	}
}
