package edu.umass.cs.msocket.contextsocket;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Set;

import org.json.JSONArray;
import org.json.JSONException;

import edu.umass.cs.nio.interfaces.NodeConfig;





/**
 * 
 * @author ayadav
 *
 * @param <NodeIDType>
 */
public class CSNodeConfig<NodeIDType> implements NodeConfig<NodeIDType> 
{
	private boolean local = false;
	HashMap<NodeIDType,InetSocketAddress> nmap=new HashMap<NodeIDType,InetSocketAddress>();
	int defaultPort=2000;

	public CSNodeConfig(int dp)
	{
		defaultPort = dp;
	}
	public CSNodeConfig() {}

	public void localSetup(Set<NodeIDType> members) 
	{
		local = true;
		for(NodeIDType i : members) 
		{
			this.add(i, getLocalAddress());
		}
	}
	/* The caller can either specify the number of nodes, nNodes,
	 * or specify a set of integer node IDs explicitly. In the former
	 * case, nNodes from 0 to nNodes-1 will the node IDs. In the 
	 * latter case, the explicit set of node IDs will be used.
	 */
	@SuppressWarnings("unchecked")
	public void localSetup(Integer nNodes) 
	{
		local = true;
		for(Integer i=0; i<nNodes; i++)
		{
			this.add((NodeIDType)i, getLocalAddress());
		}
	}
	
	private InetAddress getLocalAddress() 
	{
		InetAddress localAddr=null;
		try 
		{
			localAddr = InetAddress.getByName("localhost");
		} catch (UnknownHostException e) 
		{
			e.printStackTrace();
		}
		return localAddr;
	}

	@Override
	public boolean nodeExists(NodeIDType ID) 
	{
		return nmap.containsKey(ID);
	}

	@Override
	public Set<NodeIDType> getNodeIDs() 
	{
		//throw  new UnsupportedOperationException();
		return this.nmap.keySet();
	}

	@Override
	public InetAddress getNodeAddress(NodeIDType ID) 
	{
		InetSocketAddress addr = nmap.get(ID);
		return addr!=null ? addr.getAddress() : (local ? getLocalAddress() : null);
	}

	public int getNodePort(NodeIDType ID) 
	{
		int maxPort = 65536;
		int port = (defaultPort + ID.hashCode()) % maxPort;
		if(port < 0) port = (port + maxPort) % maxPort;
		//return port;
		
		InetSocketAddress addr = nmap.get(ID);
		return addr!=null ? addr.getPort() : port;
	}

	public Set<NodeIDType> getNodes()
	{
		return nmap.keySet();
	}
	
	public void add(NodeIDType id, InetAddress IP)
	{
		nmap.put(id, new InetSocketAddress(IP, defaultPort));
	}
	
	
	public void add(NodeIDType id, InetSocketAddress socketAddr) 
	{
		nmap.put(id, socketAddr);
	}
	
	public void addLocal(NodeIDType id) 
	{
		local = true;
		nmap.put(id, new InetSocketAddress(getLocalAddress(), defaultPort));
	}

	public String toString()
	{
		String s="";
		for(NodeIDType i : nmap.keySet())
		{
			s += i + " : " + getNodeAddress(i) + ":" + getNodePort(i) + "\n";
		}
		return s;
	}

	public static void main(String[] args)
	{
		int dp = (args.length>0 ? Integer.valueOf(args[0]) : 2222);
		CSNodeConfig<Integer> snc = new CSNodeConfig<Integer>(dp);
		
		System.out.println("Adding node 0, printing nodes 0 and 1");
		try
		{
			snc.add(0, InetAddress.getByName("localhost"));
		} catch (UnknownHostException e)
		{
			e.printStackTrace();
		}
		
		System.out.println("0 : " + snc.getNodeAddress(0) + ":" + snc.getNodePort(0));
		System.out.println("1 : " + snc.getNodeAddress(1) + ":" + snc.getNodePort(1));
	}
	
	@Override
	public Set<NodeIDType> getValuesFromJSONArray(JSONArray arg0)
			throws JSONException 
	{
		return null;
	}
	
	@Override
	public Set<NodeIDType> getValuesFromStringSet(Set<String> arg0) 
	{
		return null;
	}
	
	@Override
	public NodeIDType valueOf(String arg0)
	{
		return null;
	}
	@Override
	public InetAddress getBindAddress(NodeIDType id) {
		return nmap.get(id).getAddress();
	}
}