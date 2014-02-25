package edu.umass.cs.gns.nio;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Set;

/**
@author V. Arun
 */
public class SampleNodeConfig implements NodeConfig {

	HashMap<Integer,InetAddress> nmap=null;
	int defaultPort=0;
	
	public SampleNodeConfig(int dp) {
		nmap = new HashMap<Integer,InetAddress>();
		defaultPort = dp;
	}

	/* The caller can either specify the number of nodes, nNodes,
	 * or specify a set of integer node IDs explicitly. In the former
	 * case, nNodes from 0 to nNodes-1 will the node IDs. In the 
	 * latter case, the explicit set of node IDs will be used.
	 */
	public void localSetup(int nNodes) {
		for(int i=0; i<nNodes; i++) {
			try {
				this.add(i, InetAddress.getByName("localhost"));
			} catch (UnknownHostException e) {
				e.printStackTrace();
			}
		}
	}
	public void localSetup(Set<Integer> members) {
		for(int i : members) {
			try {
				this.add(i, InetAddress.getByName("localhost"));
			} catch (UnknownHostException e) {
				e.printStackTrace();
			}
		}
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
		return nmap.get(ID);
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
