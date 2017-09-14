package edu.umass.cs.gnsclient.client.testing.activecode;


import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.json.JSONException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;
import org.junit.runners.MethodSorters;

import edu.umass.cs.gnsclient.client.GNSClient;
import edu.umass.cs.gnsclient.client.GNSCommand;
import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.gnsclient.client.util.GuidUtils;
import edu.umass.cs.gnscommon.AclAccessType;
import edu.umass.cs.gnscommon.GNSProtocol;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnscommon.exceptions.client.EncryptionException;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.ActiveCode;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * @author gaozy
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class TestActiveCodeRemoteQueryClient {
	private final static int numGuid = 2;
	
	private static GNSClient client = null;
	private static GuidEntry[] entries;	
	private static String ACCOUNT_GUID_PREFIX = "ACCOUNT_GUID";
	private static String PASSWORD = "";
	private static String targetGuid = "";
	private final static String someField = "someField";
	private final static String someValue = "someValue";
	private final static String depthField = "depthField";
	private final static String depthResult = "Depth test succeeds";
	
	
	
	private static String read_code;
	private static String noop_code;
	private static String write_code;
	
	/**
	 * @throws IOException
	 * @throws ClientException
	 */
	@BeforeClass
	public static void setupClientsAndGuids() throws IOException, ClientException {
		client = new GNSClient();
		entries = new GuidEntry[2];
		
		// initialize two GUID
		for (int i=0; i<numGuid; i++){
			try {
				entries[i] = GuidUtils.lookupOrCreateAccountGuid(
						client, ACCOUNT_GUID_PREFIX + i, PASSWORD);
				ObjectOutputStream output = new ObjectOutputStream(new FileOutputStream(new File("guid"+i)));
				entries[i].writeObject(output);
				output.flush();
				output.close();
			} catch (Exception e) {
				// the GUID has already been created, try to fetch it from a file
				try {
					ObjectInputStream input = new ObjectInputStream(new FileInputStream(new File("guid"+i)));
					entries[i] = new GuidEntry(input);
					input.close();
				} catch (IOException | EncryptionException ie) {
					ie.printStackTrace();
				}
			}
		}
		
		// set the target guid to the second one and put it into the code
		targetGuid = entries[numGuid-1].getGuid();	
		
		noop_code = new String(Files.readAllBytes(Paths.get("scripts/activeCode/noop.js")));
		String codeFile = System.getProperty("activeReadCode");
		if(codeFile == null)
			codeFile = "scripts/activeCode/remoteReadQuery.js";
		String code = new String(Files.readAllBytes(Paths.get(codeFile)));				
		read_code = code.replace("//substitute this line with the targetGuid", "var targetGuid=\""+targetGuid+"\";");	
		System.out.println("The read code is:\n"+read_code);
		
		codeFile = System.getProperty("activeWriteCode");
		if(codeFile == null)
			codeFile = "scripts/activeCode/remoteWriteQuery.js";		
		code = new String(Files.readAllBytes(Paths.get(codeFile)));
		write_code = code.replace("//substitute this line with the targetGuid", "var targetGuid=\""+targetGuid+"\";");
		System.out.println("The write code is:\n"+write_code);
		
		// initialize the fields for each guid
		client.execute(GNSCommand.fieldUpdate(entries[0], someField, someValue));
		client.execute(GNSCommand.fieldUpdate(entries[1], depthField, depthResult));			
		
		System.out.println(">>>>>>>>>> Testing >>>>>>>>>>");
	}
	
	
	/** 
	 * Case I: test a read followed by a read with ACL
	 * 
	 * @throws IOException 
	 * @throws InterruptedException 
	 * @throws JSONException 
	 * @throws ClientException 
	 */
	@Test
	public void test_00_RemoteQueryReadAfterRead() throws IOException, InterruptedException, ClientException, JSONException{
				
		String response = null;
		
		try {
			System.out.println("start setting up active code for the 1st guid on read op");
			client.execute(GNSCommand.activeCodeSet(entries[0].getGuid(), ActiveCode.READ_ACTION, read_code, entries[0]));
			
			System.out.println("start setting up active code for the 2nd guid on read op");
			client.execute(GNSCommand.activeCodeSet(entries[1].getGuid(), ActiveCode.READ_ACTION, noop_code, entries[1]));
		} catch (ClientException e) {
			e.printStackTrace();
		}
		Thread.sleep(100);		
		
		response = client.execute(GNSCommand.fieldRead(entries[0], someField)).getResultJSONObject().getString(someField);
		
		assertEquals(depthResult, response);		
		System.out.println("Depth query test(a read followed by a read) succeeds!");
		System.out.println("Cleaning up code for this test ...");
		
		client.execute(GNSCommand.activeCodeClear(entries[0].getGuid(), ActiveCode.READ_ACTION, entries[0]));
		client.execute(GNSCommand.activeCodeClear(entries[1].getGuid(), ActiveCode.READ_ACTION, entries[1]));
	}
		
	/**
	 * Case I: test a read after read without ACL
	 * FIXME: this test does not work because active code could bypass GNS ACL.
	 * 
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ClientException
	 * @throws JSONException
	 */
	//@Test
	public void test_01_RemoteQueryReadAfterReadWithoutACL() throws IOException, InterruptedException, ClientException, JSONException{
		String response = null;
		
		try {
			System.out.println("start setting up active code for the 1st guid on read op");
			client.execute(GNSCommand.activeCodeSet(entries[0].getGuid(), ActiveCode.READ_ACTION, read_code, entries[0]));
			
			System.out.println("start setting up active code for the 2nd guid on read op");
			client.execute(GNSCommand.activeCodeSet(entries[1].getGuid(), ActiveCode.READ_ACTION, noop_code, entries[1]));
			
			System.out.println("remove 1st guid from the 2nd guid's whitelist");			
			client.execute(GNSCommand.aclRemove(AclAccessType.READ_WHITELIST, entries[1], 
					GNSProtocol.ENTIRE_RECORD.toString(), GNSProtocol.ALL_GUIDS.toString()));
			
		} catch (ClientException e) {
			e.printStackTrace();
		}
		
		try{
			response = client.execute(GNSCommand.fieldRead(entries[1].getGuid(), depthField, entries[0])).getResultJSONObject().getString(depthField);
			fail("After removing +ALL+ from 2nd guid's whitelist, 1st guid should not be allowed to read 2nd guid's field.");
		}catch (ClientException e){
			// This operation should not be allowed
		}
		
		Thread.sleep(100);		
		try{
			response = client.execute(GNSCommand.fieldRead(entries[0], someField)).getResultJSONObject().getString(someField);
			fail("Remote query (read) should not be able to bypass the ACL check.");
		} catch(ClientException e){
			assertEquals(someValue, response);
		}
		
				
		System.out.println("Depth query test(a read followed by a read without ACL) succeeds!");
		System.out.println("Cleaning up code and reset acl for this test ...");
		
		client.execute(GNSCommand.activeCodeClear(entries[0].getGuid(), ActiveCode.READ_ACTION, entries[0]));
		client.execute(GNSCommand.activeCodeClear(entries[1].getGuid(), ActiveCode.READ_ACTION, entries[1]));
		client.execute(GNSCommand.aclAdd(AclAccessType.READ_WHITELIST, entries[1], 
				GNSProtocol.ENTIRE_RECORD.toString(), GNSProtocol.ALL_GUIDS.toString()));
	}
	
	/**
	 * Case II: test a write followed by a read
	 * @throws IOException 
	 * @throws ClientException 
	 * @throws InterruptedException 
	 */
	@Test
	public void test_02_RemoteQueryReadAfterWrite() throws IOException, ClientException, InterruptedException {
		int count = 0;
		String response = null;		
		
		System.out.println("start setting up active code for the 1st guid on write op");
		client.execute(GNSCommand.activeCodeSet(entries[0].getGuid(), ActiveCode.WRITE_ACTION, read_code, entries[0]));
		System.out.println("start setting up active code for the 2nd guid on read op");
		client.execute(GNSCommand.activeCodeSet(entries[1].getGuid(), ActiveCode.READ_ACTION, noop_code, entries[1]));
		Thread.sleep(1000);		
		
		try {
			client.execute(GNSCommand.fieldUpdate(entries[0], someField, someValue));
			count = 0;
			while(count < 10){
				try {
					response = client.execute(GNSCommand.fieldRead(entries[0], someField)).getResultJSONObject().getString(someField);
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
		
		client.execute(GNSCommand.activeCodeClear(entries[0].getGuid(), ActiveCode.WRITE_ACTION, entries[0]));
		client.execute(GNSCommand.activeCodeClear(entries[1].getGuid(), ActiveCode.READ_ACTION, entries[1]));
		System.out.println("Cleaning up code for this test ...");
		client.execute(GNSCommand.fieldUpdate(entries[0], someField, someValue));
		System.out.println("reset someField to someValue...");
	}
	
	/**
	 * Case III: test a read followed by a write without ACL
	 * FIXME: this test does not work because active code could bypass GNS ACL.
	 * 
	 * @throws IOException 
	 * @throws ClientException 
	 * @throws InterruptedException 
	 * @throws JSONException 
	 */
	//@Test
	public void test_03_RemoteQueryWriteAfterReadWithoutACL() throws IOException, ClientException, InterruptedException, JSONException	{	
		System.out.println("Write after read without ACL");
		// target guid must set acl to allow accessor to write
		System.out.println("start setting up active code for the 1st guid on read op");
		client.execute(GNSCommand.activeCodeSet(entries[0].getGuid(), ActiveCode.READ_ACTION, write_code, entries[0]));
		System.out.println("start setting up active code for the 2nd guid on write op");
		client.execute(GNSCommand.activeCodeSet(entries[1].getGuid(), ActiveCode.WRITE_ACTION, noop_code, entries[1]));
		client.execute(GNSCommand.fieldRead(entries[0], someField));
		
		try{
			// someField should not exist in this test
			client.execute(GNSCommand.fieldRead(entries[1], someField)).getResultJSONObject().getString(someField);
			fail("Depth query test(a write followed by a read without ACL setup) fails!");
		}catch(Exception e){
			System.out.println("Depth query test(a write followed by a read without ACL setup) succeeds!");
		}
		
		client.execute(GNSCommand.activeCodeClear(entries[0].getGuid(), ActiveCode.READ_ACTION, entries[0]));
		client.execute(GNSCommand.activeCodeClear(entries[1].getGuid(), ActiveCode.WRITE_ACTION, entries[1]));
		System.out.println("Cleaning up code for this test ...");		
	}
		
	/**
	 * Case IV: test a read followed by a write with ACL set up		
	 * @throws IOException 
	 * @throws ClientException 
	 * @throws InterruptedException 
	 * @throws JSONException 
	 * 
	 */
	@Test
	public void test_04_RemoteQueryWriteAfterReadWithACL() throws ClientException, IOException, InterruptedException, JSONException {	
		System.out.println("Write after read with ACL");
		System.out.println("start setting up active code for the 1st guid on read op");
		client.execute(GNSCommand.activeCodeSet(entries[0].getGuid(), ActiveCode.READ_ACTION, write_code, entries[0]));
		System.out.println("start setting up active code for the 2nd guid on write op");
		client.execute(GNSCommand.activeCodeSet(entries[1].getGuid(), ActiveCode.WRITE_ACTION, noop_code, entries[1]));
		
		
		System.out.println("Setup 2nd guid's write ACL for 1st guid");
		client.execute(GNSCommand.aclAdd(AclAccessType.WRITE_WHITELIST, entries[1], GNSProtocol.ENTIRE_RECORD.toString(), entries[0].getGuid()));
		Thread.sleep(1000);
		
		String response = null;
		
		response = client.execute(GNSCommand.fieldRead(entries[0], someField)).getResultJSONObject().getString(someField);
		
		assertEquals(someValue, response);
		
		/*
		int count = 0;
		try {
			//client.fieldUpdate(entries[0], someField, someValue);
			client.execute(GNSCommand.fieldUpdate(entries[0], someField, someValue));
			count = 0;
			while(count < 10){
				try {
					//response = client.fieldRead(entries[0], someField);
					response = client.execute(GNSCommand.fieldRead(entries[1], someField)).getResultJSONObject().getString(someField);
					if(response.equals(someValue)){
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
		assertEquals(someValue, response);
		*/
		System.out.println("Depth query test(a write followed by a read with ACL setup) succeeds!");
		
		client.execute(GNSCommand.activeCodeClear(entries[0].getGuid(), ActiveCode.READ_ACTION, entries[0]));
		client.execute(GNSCommand.activeCodeClear(entries[1].getGuid(), ActiveCode.WRITE_ACTION, entries[1]));
		System.out.println("Cleaning up code for this test ...");		
	}
		
		
	/**
	 * Case V: test a write followed by a write
	 * @throws IOException 
	 * @throws InterruptedException 
	 * @throws ClientException 
	 */
	@Test
	public void test_05_RemoteQueryWriteAfterWrite() throws IOException, InterruptedException, ClientException	{	
		
		System.out.println("start setting up active code for the 1st guid on write op");
		client.execute(GNSCommand.activeCodeSet(entries[0].getGuid(), ActiveCode.WRITE_ACTION, write_code, entries[0]));
		//client.activeCodeSet(entries[0].getGuid(), ActiveCode.WRITE_ACTION, write_code, entries[0]);
		System.out.println("start setting up active code for the 2nd guid on write op");
		client.execute(GNSCommand.activeCodeSet(entries[0].getGuid(), ActiveCode.WRITE_ACTION, write_code, entries[0]));
		
		try {
			//client.fieldUpdate(entries[0], someField, someValue);
			client.execute(GNSCommand.fieldUpdate(entries[0], someField, someValue));
			fail("A write followed with a write operation should not succeed.");
		} catch (ClientException e) {
			System.out.println("Depth query test(a write followed by a write) succeeds!");
		}	
		
		client.execute(GNSCommand.activeCodeClear(entries[0].getGuid(), ActiveCode.READ_ACTION, entries[0]));
		client.execute(GNSCommand.activeCodeClear(entries[1].getGuid(), ActiveCode.WRITE_ACTION, entries[1]));
		System.out.println("Cleaning up code for this test ...");		
	}
	
	/**
	 * Case VI: test a query 
	 * 
	 */
	@Test
	public void test_06_FullDepthQuery() {
		
	}
	
	/**
	 * @throws Exception
	 */
	@AfterClass
	public static void cleanup() throws Exception{
		
		try {			
			client.execute(GNSCommand.activeCodeClear(entries[0].getGuid(), ActiveCode.READ_ACTION, entries[0]));
			client.execute(GNSCommand.activeCodeClear(entries[1].getGuid(), ActiveCode.READ_ACTION, entries[1]));
			client.execute(GNSCommand.activeCodeClear(entries[0].getGuid(), ActiveCode.WRITE_ACTION, entries[0]));
			client.execute(GNSCommand.activeCodeClear(entries[1].getGuid(), ActiveCode.WRITE_ACTION, entries[1]));			
		} catch (ClientException e) {
			e.printStackTrace();
		}
		client.execute(GNSCommand.accountGuidRemove(entries[0]));
		client.execute(GNSCommand.accountGuidRemove(entries[1]));
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
