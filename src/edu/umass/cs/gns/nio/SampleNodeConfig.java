package edu.umass.cs.gns.nio;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Set;

/**
@author V. Arun
 */
public class SampleNodeConfig implements NodeConfig {

	private boolean local = false;
	HashMap<Integer,InetAddress> nmap=new HashMap<Integer,InetAddress>();;
	int defaultPort=2000;
	
	public SampleNodeConfig(int dp) {
		defaultPort = dp;
	}
	public SampleNodeConfig() {}

	/* The caller can either specify the number of nodes, nNodes,
	 * or specify a set of integer node IDs explicitly. In the former
	 * case, nNodes from 0 to nNodes-1 will the node IDs. In the 
	 * latter case, the explicit set of node IDs will be used.
	 */
	public void localSetup(int nNodes) {
		local = true;
		for(int i=0; i<nNodes; i++) {
			this.add(i, getLocalAddress());
		}
	}
	public void localSetup(Set<Integer> members) {
		local = true;
		for(int i : members) {
			this.add(i, getLocalAddress());
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
	public boolean containsNodeInfo(int ID) {
		return nmap.containsKey(ID);
	}

	@Override
	public int getNodeCount() {
		return nmap.size();
	}

	@Override
	public InetAddress getNodeAddress(int ID) {
		InetAddress addr = nmap.get(ID);
		return addr!=null ? addr : (local ? getLocalAddress() : null);
	}

	@Override
	public int getNodePort(int ID) {
		return defaultPort + ID;
	}
	
	public void add(int id, InetAddress IP) { 
		nmap.put(id, IP);
	}
	
	public String toString() {
		String s="";
		for(Integer i : nmap.keySet()) {
			s += i + " : " + getNodeAddress(i) + ":" + getNodePort(i) + "\n";
		}
		return s;
	}
	
	public static void main(String[] args) {
		int dp = (args.length>0 ? Integer.valueOf(args[0]) : 2222);
		SampleNodeConfig snc = new SampleNodeConfig(dp);

		System.out.println("Adding node 0, printing nodes 0 and 1");
    try {
      snc.add(0, InetAddress.getLocalHost());
    } catch (UnknownHostException e) {
      e.printStackTrace();
    }
    System.out.println("0 : " + snc.getNodeAddress(0) + ":" + snc.getNodePort(0));
		System.out.println("1 : " + snc.getNodeAddress(1) + ":" + snc.getNodePort(1));

		System.out.println("Adding nodes 0 to 9 and printing all");
		snc.localSetup(10);
		System.out.println(snc);
		
	}

}
