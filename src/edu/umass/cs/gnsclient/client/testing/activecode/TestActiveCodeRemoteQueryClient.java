package edu.umass.cs.gnsclient.client.testing.activecode;


import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.junit.Test;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;

import edu.umass.cs.gnsclient.client.GNSClientCommands;
import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.gnsclient.client.util.GuidUtils;
import edu.umass.cs.gnscommon.AclAccessType;
import edu.umass.cs.gnscommon.GNSProtocol;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.ActiveCode;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * @author gaozy
 *
 */
public class TestActiveCodeRemoteQueryClient {
	private final static int numGuid = 2;
	
	private static GNSClientCommands client = null;
	private static GuidEntry[] entries;	
	private static String ACCOUNT_GUID_PREFIX = "ACCOUNT_GUID";
	private static String PASSWORD = "";
	private static String targetGuid = "";
	private final static String someField = "someField";
	private final static String someValue = "someValue";
	private final static String depthField = "depthField";
	private final static String depthResult = "Depth test succeeds";
	
	
	
	private static void setupClientsAndGuids() throws Exception{
		client = new GNSClientCommands();
		entries = new GuidEntry[2];
		
		// initialize two GUID
		for (int i=0; i<numGuid; i++){
			entries[i] = GuidUtils.lookupOrCreateAccountGuid(
					client, ACCOUNT_GUID_PREFIX + i, PASSWORD);
		}
		
		// initialize the fields for each guid
		
		client.fieldUpdate(entries[0], someField, someValue);
		//client.fieldUpdate(entries[1], someField, depthResult);
		client.fieldUpdate(entries[1], depthField, depthResult);
		
		// set the target guid to the second one and put it into the code
		targetGuid = entries[numGuid-1].getGuid();		
		
	}
	
	
	/**
	 * @throws IOException 
	 * @throws InterruptedException 
	 */
	@Test
	public void test_01_RemoteQuery() throws IOException, InterruptedException{	
		
		int count = 0;
		try {
			setupClientsAndGuids();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		System.out.println(">>>>>>>>>> Testing >>>>>>>>>>");
		String codeFile = System.getProperty("activeReadCode");
		if(codeFile == null)
			codeFile = "scripts/activeCode/remoteReadQuery.js";
		
		String code = new String(Files.readAllBytes(Paths.get(codeFile)));
		String read_code = code.replace("//substitute this line with the targetGuid", "var targetGuid=\""+targetGuid+"\";");
		String noop_code = new String(Files.readAllBytes(Paths.get("scripts/activeCode/noop.js")));
		
		System.out.println("The new code is:\n"+read_code);
		
		String response = null;
		
		/** 
		 * Case I: test a read followed by a read		
		 */
		try {
			client.activeCodeSet(entries[0].getGuid(), ActiveCode.READ_ACTION, read_code.getBytes("UTF-8"), entries[0]);
			
			client.activeCodeSet(entries[1].getGuid(), ActiveCode.READ_ACTION, noop_code.getBytes("UTF-8"), entries[1]);
		} catch (ClientException e) {
			e.printStackTrace();
		}
		Thread.sleep(1000);
		
		
		try {
			response = client.fieldRead(entries[0], someField);
		} catch (Exception e) {
			e.printStackTrace();
		}
		assertEquals(depthResult, response);		
		System.out.println("Depth query test(a read followed by a read) succeeds!");		
		
		
		/**
		 * Case II: test a write followed by a read
		 */
		try {
			client.activeCodeClear(entries[0].getGuid(), ActiveCode.READ_ACTION, entries[0]);
			client.activeCodeSet(entries[0].getGuid(), ActiveCode.WRITE_ACTION, read_code.getBytes("UTF-8"), entries[0]);
			
		} catch (ClientException e) {
			e.printStackTrace();
		}
		Thread.sleep(1000);		
		
		try {
			client.fieldUpdate(entries[0], someField, someValue);
			
			count = 0;
			while(count < 10){
				try {
					response = client.fieldRead(entries[0], someField);
					if(response.equals(depthResult)){
						break;
					}else{					
						count++;
						System.out.println("The value hasn't been updated without a reason "+count);
						Thread.sleep(500);
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		assertEquals(depthResult, response);
		System.out.println("Depth query test(a write followed by a read) succeeds!");
		
		
		// reset the state		
		try {
			client.activeCodeClear(entries[0].getGuid(), ActiveCode.WRITE_ACTION, entries[0]);
			client.activeCodeClear(entries[1].getGuid(), ActiveCode.READ_ACTION, entries[1]);
			Thread.sleep(1000);
			client.fieldUpdate(entries[0], someField, someValue);
		} catch (ClientException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		
		/**
		 * Case III: test a read followed by a write 		
		 */
		codeFile = System.getProperty("activeWriteCode");
		if(codeFile == null)
			codeFile = "scripts/activeCode/remoteWriteQuery.js";		
		code = new String(Files.readAllBytes(Paths.get(codeFile)));
		String write_code = code.replace("//substitute this line with the targetGuid", "var targetGuid=\""+targetGuid+"\";");
		System.out.println("The new code is:\n"+write_code);
		
		try {
			// target guid must set acl to allow accessor to write
			client.aclAdd(AclAccessType.WRITE_WHITELIST, entries[1], GNSProtocol.ENTIRE_RECORD.toString(), entries[0].getGuid());
			client.activeCodeSet(entries[0].getGuid(), ActiveCode.READ_ACTION, write_code.getBytes("UTF-8"), entries[0]);					
			client.activeCodeSet(entries[1].getGuid(), ActiveCode.WRITE_ACTION, noop_code.getBytes("UTF-8"), entries[1]);		
		} catch ( Exception e ) {
			e.printStackTrace();
		}
		Thread.sleep(1000);
		
		try {
			response = client.fieldRead(entries[0], someField);
		} catch (Exception e2) {
			e2.printStackTrace();
		}
		assertEquals(someValue, response);
		
		
		try {
			response = client.fieldRead(entries[1], someField);
			assertEquals(someValue, response);
		} catch (Exception e2) {
			fail("Expect "+someValue+" but get "+response+", write after read failed.");
		}
		
		
		// This sleep is required as a requirement for eventual consistency semantics of gigapaxos
		Thread.sleep(1000);
		try {
			response = client.fieldRead(entries[1], someField);
		} catch (Exception e1) {
			e1.printStackTrace();
		}
		assertEquals(someValue, response);
		System.out.println("Depth query test(a write followed by a read) succeeds!");
		
		
		
		
		/**
		 *  test a write followed by a write
		 */
		try {
			client.activeCodeClear(entries[0].getGuid(), ActiveCode.READ_ACTION, entries[0]);
			client.activeCodeSet(entries[0].getGuid(), ActiveCode.WRITE_ACTION, write_code.getBytes("UTF-8"), entries[0]);
			
		} catch (ClientException e) {
			e.printStackTrace();
		}
		Thread.sleep(1000);
		try {
			client.fieldUpdate(entries[0], someField, someValue);
			fail("A write followed with a write operation should not succeed.");
		} catch (ClientException e) {
			System.out.println("Depth query test(a write followed by a write) succeeds!");
		}		
		 
		
		try {
			cleanup();
		} catch (Exception e) {
			e.printStackTrace();
		}		
	}
	
	private static void cleanup() throws Exception{
		try {
			client.activeCodeClear(entries[0].getGuid(), ActiveCode.READ_ACTION, entries[0]);
			client.activeCodeClear(entries[1].getGuid(), ActiveCode.READ_ACTION, entries[1]);
			client.activeCodeClear(entries[0].getGuid(), ActiveCode.WRITE_ACTION, entries[0]);
			client.activeCodeClear(entries[1].getGuid(), ActiveCode.WRITE_ACTION, entries[1]);
			
			
		} catch (ClientException e) {
			e.printStackTrace();
		}
		
		
		client.close();
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args){
		Result result = JUnitCore.runClasses(TestActiveCodeRemoteQueryClient.class);
		for (Failure failure : result.getFailures()) {
			System.out.println(failure.getMessage());
			failure.getException().printStackTrace();
		}
	}
}
