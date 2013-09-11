package edu.umass.cs.gns.mSocketApplications;

import edu.umass.cs.gns.client.Intercessor;
import edu.umass.cs.gns.nameserver.NameRecordKey;
//import edu.umass.cs.gns.packet.QueryResultValue;
import edu.umass.cs.gns.client.UpdateOperation;

import java.util.ArrayList;

public class MSocketSampleClient
{

	
	public static void main(String[] args) {
		
		Intercessor myInter = Intercessor.getInstance();
		
		
		String name = "www.google.com";
		String key = NameRecordKey.EdgeRecord.getName();
		String address = "127.0.0.1";
		
		boolean result = myInter.sendAddRecordWithConfirmation(name, key, address);

    ArrayList<String> queryResult = myInter.sendQuery(name, key);
		
		System.out.println("Query Result: " + queryResult.toString());
		
		String newAddress = "127.0.0.10";
		myInter.sendUpdateRecordWithConfirmation(name, key, newAddress, null, UpdateOperation.REPLACE_ALL);
		
		
		
		
	}
	
	
}
