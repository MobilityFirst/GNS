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
/*
 * Copyright (C) 2014 University of Massachusetts All Rights Reserved
 */
package edu.umass.cs.nio.nioutils;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;

import edu.umass.cs.nio.interfaces.NodeConfig;

import java.util.HashSet;

import org.json.JSONArray;
import org.json.JSONException;

/**
 * @author V. Arun
 * @param <NodeIDType>
 * 
 *            This class is a simple utility to quickly set up a simple node
 *            config object when port numbers can take "don't care" values. It
 *            is also especially convenient for a local (single-machine) setup.
 *            This class is mainly useful for quick testing.
 */
public class SampleNodeConfig<NodeIDType> implements
		NodeConfig<NodeIDType> {

	/**
	 * The default starting port number beyond which nodes automatically get
	 * port numbers assigned.
	 */
	public static final int DEFAULT_START_PORT = 2000;
	private boolean local = false;
	private HashMap<NodeIDType, InetAddress> nmap = new HashMap<NodeIDType, InetAddress>();;
	private final int defaultPort;

	/**
	 * @param defaultPort
	 *            Assigns port numbers to nodes starting from defaultPort
	 */
	public SampleNodeConfig(int defaultPort) {
		this.defaultPort = defaultPort;
	}
	/**
	 * @param defaultPort
	 * @param startNodeID 
	 * @param numLocalNodes
	 */
	public SampleNodeConfig(int defaultPort, int startNodeID, int numLocalNodes) {
		this.defaultPort = defaultPort;
		this.localSetup(startNodeID, numLocalNodes);
	}

	/**
	 * 
	 */
	public SampleNodeConfig() {
		this(DEFAULT_START_PORT);
	}

	/**
	 * Maps each element in members to the local IP address.
	 * 
	 * @param members
	 */
	public void localSetup(Set<NodeIDType> members) {
		local = true;
		for (NodeIDType member : members) {
			this.add(member, getLocalAddress());
		}
	}

	/**
	 * The caller can either specify the number of nodes, nNodes, or specify a
	 * set of integer node IDs explicitly. In the former case, nNodes from 0 to
	 * nNodes-1 will the node IDs. In the latter case, the explicit set of node
	 * IDs will be used.
	 * @param startNodeID 
	 * 
	 * @param nNodes
	 *            Number of nodes created.
	 */
	@SuppressWarnings("unchecked")
	public void localSetup(int startNodeID, int nNodes) {
		local = true;
		for (Integer i = startNodeID; i < nNodes+startNodeID; i++) {
			this.add((NodeIDType) i, getLocalAddress());
		}
	}
	/**
	 * @param nNodes
	 */
	public void localSetup(int nNodes) {
		localSetup(0, nNodes);
	}

	@Override
	public boolean nodeExists(NodeIDType ID) {
		return nmap.containsKey(ID);
	}

	@Override
	public Set<NodeIDType> getNodeIDs() {
		// throw new UnsupportedOperationException();
		return this.nmap.keySet();
	}

	@Override
	public InetAddress getNodeAddress(NodeIDType ID) {
		InetAddress addr = nmap.get(ID);
		return addr != null ? addr : (local ? getLocalAddress() : null);
	}
        
        @Override
	public InetAddress getBindAddress(NodeIDType ID) {
		InetAddress addr = nmap.get(ID);
		return addr != null ? addr : (local ? getLocalAddress() : null);
	}

	/**
	 * Maps each node ID to a port number. If ID is an integer, then the port
	 * number is calculated as {@code defaultport + ID}, else it is
	 * {@code defaultPort + ID.hashCode()}. The port calculated above modulo
	 * 65536 is returned in order to ensure legitimate port numbers.
	 */
	@Override
	public int getNodePort(NodeIDType ID) {
		int maxPort = 65536;
		int port = ID != null ? ((defaultPort + ID.hashCode()) % maxPort) : 0;
		if (port < 0) {
			port = (port + maxPort) % maxPort;
		}
		return port;
	}
	/**
	 * @param ID
	 * @return Port + default offset.
	 */
	public static int getPort(int ID) {
		int maxPort = 65536;
		int port = (DEFAULT_START_PORT + ID) % maxPort;
		if (port < 0) 
			port = (port + maxPort) % maxPort;
		return port;
	}

	/**
	 * @return Set of all nodes.
	 */
	public Set<NodeIDType> getNodes() {
		return nmap.keySet();
	}

	/**
	 * Add node with id mapped to IP and an auto-selected port number.
	 * @param id Node id.
	 * @param IP IP address.
	 */
	public void add(NodeIDType id, InetAddress IP) {
		nmap.put(id, IP);
	}

	/**
	 * @param id
	 */
	public void addLocal(NodeIDType id) {
		local = true;
		nmap.put(id, getLocalAddress());
	}


	/**
	 * Pretty prints this node config information.
	 */
	public String toString() {
		String s = "";
		for (NodeIDType i : nmap.keySet()) {
			s += i + " : " + getNodeAddress(i) + ":" + getNodePort(i) + "\n";
		}
		return s;
	}

	@SuppressWarnings("unchecked")
	@Override
	public NodeIDType valueOf(String nodeAsString) {
		NodeIDType node = null;
		Iterator<NodeIDType> nodeIter = this.nmap.keySet().iterator();
		if (nodeIter.hasNext() && (node = nodeIter.next()) != null) {
			if (node instanceof String) {
				return (NodeIDType) nodeAsString;
			} else if (node instanceof Integer) {
				return (NodeIDType) (Integer.valueOf(nodeAsString.trim()));
			} else if (node instanceof InetAddress) {
				try {
					return (NodeIDType) (InetAddress.getByName(nodeAsString
							.trim()));
				} catch (UnknownHostException e) {
					e.printStackTrace();
				}
			}
		}
		return null;
	}

	@Override
	public Set<NodeIDType> getValuesFromStringSet(Set<String> strNodes) {
		Set<NodeIDType> nodes = new HashSet<NodeIDType>();
		for (String strNode : strNodes) {
			nodes.add(valueOf(strNode));
		}
		return nodes;
	}

	@Override
	public Set<NodeIDType> getValuesFromJSONArray(JSONArray array)
			throws JSONException {
		Set<NodeIDType> nodes = new HashSet<NodeIDType>();
		for (int i = 0; i < array.length(); i++) {
			nodes.add(valueOf(array.getString(i)));
		}
		return nodes;
	}

	/**
	 * @return Local host address.
	 */
	public static InetAddress getLocalAddress() {
		InetAddress localAddr = null;
		try {
			localAddr = InetAddress.getByName("localhost");
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		return localAddr;
	}
	
	/**
	 * 
	 */
	public void clear() {
		this.nmap.clear();
	}

	static class Main {
		public static void main(String[] args) {
			int dp = (args.length > 0 ? Integer.valueOf(args[0]) : 2222);
			SampleNodeConfig<Integer> snc = new SampleNodeConfig<Integer>(dp);

			System.out.println("Adding node 0, printing nodes 0 and 1");
			try {
				snc.add(0, InetAddress.getByName("localhost"));
			} catch (UnknownHostException e) {
				e.printStackTrace();
			}
			System.out.println("0 : " + snc.getNodeAddress(0) + ":"
					+ snc.getNodePort(0));
			System.out.println("1 : " + snc.getNodeAddress(1) + ":"
					+ snc.getNodePort(1));
		}
	}

}
