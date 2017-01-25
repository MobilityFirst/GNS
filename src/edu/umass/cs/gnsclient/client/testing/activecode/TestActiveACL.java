package edu.umass.cs.gnsclient.client.testing.activecode;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;

import edu.umass.cs.gnsclient.client.GNSClientCommands;
import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.gnsclient.client.util.GuidUtils;
import edu.umass.cs.gnscommon.AclAccessType;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.ActiveCode;
import edu.umass.cs.utils.DefaultTest;
import edu.umass.cs.utils.Util;

/**
 * This test checks when a GNS user does not remove his ALL_FIELD ACL,
 * what will happen.
 * 
 * @author gaozy
 */
@FixMethodOrder(org.junit.runners.MethodSorters.NAME_ASCENDING)
public class TestActiveACL extends DefaultTest {
	
	private final static int numGuid = 3;
	
	private static GNSClientCommands client = null;
	private static GuidEntry[] entries;	
	private static String ACCOUNT_GUID_PREFIX = "GUID";
	private static String PASSWORD = "";
	private final static String someField = "GUID_0_FIELD";
	private final static String someValue = "YOU_SHOULD_NOT_SEE_THIS_FIELD";
	
	
	/**
	 * @throws Exception
	 */
	@BeforeClass
	public static void setupClientsAndGuids() throws Exception {
		client = new GNSClientCommands();
		entries = new GuidEntry[numGuid];
		
		// initialize three GUID
		for (int i=0; i<numGuid; i++){
			try {
				entries[i] = GuidUtils.lookupOrCreateAccountGuid(
						client, ACCOUNT_GUID_PREFIX + i, PASSWORD);
			} catch (Exception e) {
			}
		}
		System.out.println("Create 3 GUIDs:GUID_0, GUID_1 and GUID_2");
		
		// initialize the fields for each guid
		client.fieldUpdate(entries[0], someField, someValue);
		client.aclAdd(AclAccessType.READ_WHITELIST, entries[0], someField, entries[2].getGuid());
		
		System.out.println("Update value of field '"+someField+"' for GUID_0 to "
				+someValue+", and add GUID_2 into "+someField+"'s ACL.");
		
	}
	
	
	/**
	 * This method tests the ACL check without ActiveACL, it needs to guarantee the following invariant.
	 * <p>The invariant of ACL is:
	 * <p>When the whitelist of a field Y.F exists, then a GUID X can read Y.F if and only if X belongs to the whitelist of Y.F
	 * <p>When the whitelist of a field Y.F does not exist, then any GUID X can read Y.F 
	 * @throws IOException 
	 * @throws InterruptedException 
	 */
	@Test
	public void test_01_checkWithoutActiveACL() throws IOException, InterruptedException{
		
		System.out.println(">>>>>>>>>> Test without ActiveACL >>>>>>>>>>");
		
		String response1 = null;
		try {
			response1 = client.fieldRead(entries[0].getGuid(), someField, entries[2]);
		} catch (Exception e1) {
			
		}
		assertEquals(response1, someValue);
		
		String response2 = null;
		System.out.println("GUID_1 reads the field GUID_0_FIELD of GUID_0");
		try {
			response2 = client.fieldRead(entries[0].getGuid(), someField, entries[1]);
			fail("GUID_1 should not be able to access to the field GUID_0_FIELD and see the response :\""+response2+"\"");
		} catch (Exception e) {
			
		}
	}
	
	/**
	 * This method tests the ACL check with ActiveACL, it needs to guarantee the following invariant.
	 * <p>The invariant of ACL is:
	 * <p>A GUID X can read Y.F if and only if X satisfies the condition defined by Y 
	 * 
	 * Let A be the access GUID, L is the whitelist of Y.F, C is the code of Y
	 * We are going to test the following cases:
	 * <p> A is in L, and C allows A to access F, then A should be able to access F
	 * <p> A is in L, and C does not allow A to access F, then A should not be able to access F
	 * <p> A is not in L, and C allow F to access F, then A should be able to access F
	 * @throws Exception 
	 */
	@Test
	public void test_02_checkWithActiveACL() throws Exception{
		System.out.println(">>>>>>>>>> Test with ActiveACL >>>>>>>>>>");
		
		/**
		 * Prepare code and set up whitelist
		 */
		client.aclAdd(AclAccessType.READ_WHITELIST, entries[0], someField, entries[1].getGuid());
		
		String allowed_code = new String(Files.readAllBytes(Paths.get("scripts/activeCode/aclAllowAccess.js")));
		String unallowed_code = new String(Files.readAllBytes(Paths.get("scripts/activeCode/aclNotAllowAccess.js")));
		
		allowed_code = allowed_code.replace("//replace with guid", "\""+entries[1].getGuid()+"\"").replace("//replace with public key", "\""+entries[1].getPublicKeyString()+"\"");
		unallowed_code = unallowed_code.replace("//replace with guid", "\""+entries[2].getGuid()+"\"");
		
		System.out.println("The allowed code is:\n"+allowed_code);
		System.out.println("The unallowed code is:\n"+unallowed_code);
		/*
		JSONArray list = client.aclGet(AclAccessType.READ_WHITELIST, entries[0], someField, entries[0].getGuid());
		System.out.println("The whitelist of the field contains the following guids:");
		for (int i=0; i<list.length(); i++){
			System.out.println(list.get(i));
		}
		
		System.out.println("The public key of GUID_1 is "+entries[1].getPublicKeyString());
		*/
		
		client.activeCodeSet(entries[0].getGuid(), ActiveCode.ON_READ, allowed_code.getBytes("UTF-8"), entries[0]);
		
		/**
		 * Test 1: A is in L, and C allows A to access F, then A should be able to access F
		 */
		String response1 = client.fieldRead(entries[0].getGuid(), someField, entries[1]);
		
		assertEquals(response1, someValue);
		
		/**
		 * Test 2: A is in L, and C does not allow A to access F, then A should not be able to access F
		 */
		// First, update the code		
		client.activeCodeSet(entries[0].getGuid(), ActiveCode.ON_READ, unallowed_code.getBytes("UTF-8"), entries[0]);
		try{
			String response2 = client.fieldRead(entries[0].getGuid(), someField, entries[2]);
			fail("GUID_1 should not be able to access to the field GUID_0_FIELD and see the response :\""+response2+"\"");
		} catch(Exception e){
			
		}
		
		
		
		/**
		 * Test 3:A is not in L, and C allow F to access F, then A should be able to access F
		 */
		
		// First, remove GUID_1 from the whitelist		
		client.activeCodeSet(entries[0].getGuid(), ActiveCode.ON_READ, allowed_code.getBytes("UTF-8"), entries[0]);
		client.aclRemove(AclAccessType.READ_WHITELIST, entries[0], someField, entries[1].getGuid());
		Thread.sleep(1000);
		
		String response3 = client.fieldRead(entries[0].getGuid(), someField, entries[1]);
		
		assertEquals(response3, someValue);
				
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args){
		Util.assertAssertionsEnabled();
		Result result = JUnitCore.runClasses(TestActiveACL.class);
		for (Failure failure : result.getFailures()) {
			System.out.println(failure.getMessage());
			failure.getException().printStackTrace();
		}
	}
}
