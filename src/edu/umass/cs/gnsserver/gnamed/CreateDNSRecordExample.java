package edu.umass.cs.gnsserver.gnamed;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.json.JSONObject;

import edu.umass.cs.gnsclient.client.GNSClient;
import edu.umass.cs.gnsclient.client.GNSCommand;
import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.gnsclient.client.util.GuidUtils;

/**
 * This example creates an A record for a domain name.
 * 
 * @author gaozy
 *
 */
public class CreateDNSRecordExample {
	
	private static GNSClient client;
	
	// The last dot is very important, it tells the DNS to search from the root
	private static String DOMAIN;
	
	private static String ACCOUNT;
	
	private static String RECORD_FILE;
	
	private final static int TTL = 30;
	
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception{
		// initialize parameters
		DOMAIN = "example.com.";
		if(System.getProperty("domain")!=null){
			DOMAIN = System.getProperty("domain");
		}
		// if domain does not end with a dot, add a dot to its end
		if(!DOMAIN.endsWith(".")){
			DOMAIN = DOMAIN+".";
		}
		
		// use domain name and a random number as account
		ACCOUNT = DOMAIN;
		
		/**
		 *  default record file is "conf/activeCode/records", it list the IP addresses in separate lines as:
		 *  1.1.1.1
		 *  2.2.2.2
		 *  ...
		 */
		
		RECORD_FILE = "conf/activeCode/records";
		if(System.getProperty("record_file")!=null){
			RECORD_FILE = System.getProperty("record_file");
		}
		// read in records
		List<String> records = new ArrayList<String>();
		BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(RECORD_FILE)));
		String line = reader.readLine();
		while(line != null){
            records.add(line);
            line = reader.readLine();
        } 
		reader.close();
		
		// initialize client and create record for the domain
		try {
			client = new GNSClient();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		GuidEntry accountGuid = GuidUtils.lookupOrCreateAccountGuid(client, ACCOUNT,
				"password", true);
		
		JSONObject recordObj = ManagedDNSServiceProxy.recordToCreate(records, TTL);
		
		client.execute(GNSCommand.fieldUpdate(accountGuid, 
				"A", recordObj));
		
		System.out.println("A record for domain " + DOMAIN+" has been created.");
		
		System.exit(0);
	}
}
