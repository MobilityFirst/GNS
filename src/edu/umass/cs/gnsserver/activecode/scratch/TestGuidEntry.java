package edu.umass.cs.gnsserver.activecode.scratch;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;

import edu.umass.cs.gnsclient.client.GNSClient;
import edu.umass.cs.gnsclient.client.GNSClientConfig;
import edu.umass.cs.gnsclient.client.GuidEntry;

public class TestGuidEntry {
	
	public static void main(String[] args) throws Exception{
		String address = "128.119.245.20";
		String name = "testGuid@gigapaxos.net";
		String password = "123";
		String file_name = "guid";
		
		GNSClient client = new GNSClient( (InetSocketAddress) null, new InetSocketAddress(address, GNSClientConfig.LNS_PORT), true);
		 
		GuidEntry guidEntry = client.accountGuidCreate(name, password);
		
		FileOutputStream fos = new FileOutputStream(file_name);
		ObjectOutputStream os = new ObjectOutputStream(fos);
		guidEntry.writeObject(os);
		
		FileInputStream fis = new FileInputStream(file_name);
		ObjectInputStream ois = new ObjectInputStream(fis);
		
		GuidEntry newEntry = new GuidEntry(ois);
	}
}
