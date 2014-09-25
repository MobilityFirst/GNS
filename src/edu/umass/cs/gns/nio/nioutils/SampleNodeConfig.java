package edu.umass.cs.gns.nio.nioutils;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Set;

import edu.umass.cs.gns.nio.InterfaceNodeConfig;

/**
@author V. Arun
 */
public class SampleNodeConfig<NodeIDType> implements InterfaceNodeConfig<NodeIDType> {

	public static final int DEFAULT_START_PORT = 2000;
	private boolean local = false;
	private HashMap<NodeIDType,InetAddress> nmap=new HashMap<NodeIDType,InetAddress>();;
	private int defaultPort=DEFAULT_START_PORT;

	public SampleNodeConfig(int dp) {
		defaultPort = dp;
	}
	public SampleNodeConfig() {}

	public void localSetup(Set<NodeIDType> members) {
		local = true;
		for(NodeIDType i : members) {
			this.add(i, getLocalAddress());
		}
	}
	/* The caller can either specify the number of nodes, nNodes,
	 * or specify a set of integer node IDs explicitly. In the former
	 * case, nNodes from 0 to nNodes-1 will the node IDs. In the 
	 * latter case, the explicit set of node IDs will be used.
	 */
	@SuppressWarnings("unchecked")
	public void localSetup(int nNodes) {
		local = true;
		for(Integer i=0; i<nNodes; i++) {
			this.add((NodeIDType)i, getLocalAddress());
		}
	}
	private InetAddress getLocalAddress() {
		InetAddress localAddr=null;
		try {
			localAddr = InetAddress.getByName("localhost");
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		return localAddr;
	}

	@Override
	public boolean nodeExists(NodeIDType ID) {
		return nmap.containsKey(ID);
	}


	@Override
	public Set<NodeIDType> getNodeIDs() {
		//throw  new UnsupportedOperationException();
		return this.nmap.keySet();
	}

	@Override
	public InetAddress getNodeAddress(NodeIDType ID) {
		InetAddress addr = nmap.get(ID);
		return addr!=null ? addr : (local ? getLocalAddress() : null);
	}

	@Override
	public int getNodePort(NodeIDType ID) {
		int maxPort = 65536;
		int port = ID!=null ? ((defaultPort + ID.hashCode()) % maxPort) : 0;
		if(port < 0) port = (port + maxPort) % maxPort;
		return port;
	}

	public Set<NodeIDType> getNodes() {
		return nmap.keySet();
	}

	public void add(NodeIDType id, InetAddress IP) { 
		nmap.put(id, IP);
	}
	public void addLocal(NodeIDType id) {
		local = true;
		nmap.put(id, getLocalAddress());
	}

	public String toString() {
		String s="";
		for(NodeIDType i : nmap.keySet()) {
			s += i + " : " + getNodeAddress(i) + ":" + getNodePort(i) + "\n";
		}
		return s;
	}

	public static void main(String[] args) {
		int dp = (args.length>0 ? Integer.valueOf(args[0]) : 2222);
		SampleNodeConfig<Integer> snc = new SampleNodeConfig<Integer>(dp);

		System.out.println("Adding node 0, printing nodes 0 and 1");
		try {
			snc.add(0, InetAddress.getByName("localhost"));
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		System.out.println("0 : " + snc.getNodeAddress(0) + ":" + snc.getNodePort(0));
		System.out.println("1 : " + snc.getNodeAddress(1) + ":" + snc.getNodePort(1));
	}
}
