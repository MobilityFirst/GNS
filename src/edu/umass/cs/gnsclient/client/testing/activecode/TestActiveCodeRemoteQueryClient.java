package edu.umass.cs.gnsclient.client.testing.activecode;

import static org.junit.Assert.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;

import edu.umass.cs.gnsclient.client.GNSClientCommands;
import edu.umass.cs.gnsclient.client.testing.GNSClientCapacityTest;
import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.gnsclient.client.util.GuidUtils;
import edu.umass.cs.utils.Util;

/**
 * @author gaozy
 *
 */
public class TestActiveCodeRemoteQueryClient {
	
	private static GNSClientCommands client;
	private static GuidEntry[] entries;
	private final static int numGuid = 2;
	private static String ACCOUNT_GUID_PREFIX = "ACCOUNT_GUID";
	private static String PASSWORD = "";
	private static String targetGuid = "";
	private final static String field = "someField";
	
	/**
	 * @throws Exception
	 */
	public TestActiveCodeRemoteQueryClient() throws Exception {
	}
	
	/**
	 * @throws Exception
	 */
	@BeforeClass
	public static void setup() throws Exception {
		setupClientsAndGuids();
	}
	
	private static void setupClientsAndGuids() throws Exception{
		client = new GNSClientCommands();
		entries = new GuidEntry[2];
		
		// initialize two GUID
		for (int i=0; i<numGuid; i++){
			entries[i] = GuidUtils.lookupOrCreateAccountGuid(
					client, ACCOUNT_GUID_PREFIX + i, PASSWORD);
		}
		
		// initialize the fields for each guid
		for (int i=0; i<numGuid; i++){
			client.fieldUpdate(entries[i], field, i);
		}
		
		// set the target guid to the second one and put it into the code
		targetGuid = entries[numGuid-1].getGuid();
		
		String codeFile = System.getProperty("activeCode");
		if(codeFile == null)
			codeFile = "scripts/activeCode/remoteQuery.js";
		
		String code = new String(Files.readAllBytes(Paths.get(codeFile)));
		code.replace("//substitute this line with the targetGuid", "var targetGuid="+targetGuid+";");
		System.out.println("The generated code is "+code);
		
	}
	
	
	/**
	 * @throws Exception 
	 * 
	 */
	@Test
	public void test_01_RemoteQuery() throws Exception{		
		// this is just used for a demo purpose, the performance should be tested next
		String response = client.fieldRead(entries[0], field);
		assertEquals(response, "1");
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args){
		Util.assertAssertionsEnabled();
		Result result = JUnitCore.runClasses(GNSClientCapacityTest.class);
		for (Failure failure : result.getFailures()) {
			System.out.println(failure.getMessage());
			failure.getException().printStackTrace();
		}
	}
}
