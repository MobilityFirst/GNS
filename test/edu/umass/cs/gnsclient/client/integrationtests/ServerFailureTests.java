package edu.umass.cs.gnsclient.client.bugreports;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.util.ArrayList;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.junit.runners.MethodSorters;

import edu.umass.cs.gnsclient.client.GNSClient;
import edu.umass.cs.gnsclient.client.GNSClientCommands;
import edu.umass.cs.gnsclient.client.GuidEntry;
import edu.umass.cs.gnsclient.client.util.GuidUtils;
import edu.umass.cs.gnscommon.utils.RandomString;
import edu.umass.cs.gnscommon.utils.ThreadUtils;
import edu.umass.cs.nio.JSONDelayEmulator;

/**
 * @author Brendan
 * 
 * Runs a number of tests while shutting down a minority of GNSApp servers at random intervals.
 *
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ServerFailureTests {

	private static final String DEFAULT_ACCOUNT_ALIAS = "support@gns.name";

	private static String accountAlias = DEFAULT_ACCOUNT_ALIAS; // REPLACE																// ALIAS
	private static final String PASSWORD = "password";
	private static GNSClientCommands client = null;
	private static GuidEntry masterGuid;
	private static ArrayList<String> serverNameArray;

	public static void causeRandomServerFailure(){
		//Shutdown a random server, update server count.
		
		//Wait a random time length
		
		//Restart the server to simulate recovery, update server count.
		
	}
	
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		//Testing.ServerList must be a comma separated list of server names that can be run using gpSever.sh
		String serverList = System.getProperty("Testing.ServerList");
		if (serverList.equals("")){
			System.out.println("Missing Testing.ServerList, which must be a comma separated list of server names!");
			System.exit(1);
		}
		while (serverList.length() > 0){
			int commaIdx = serverList.indexOf(',');
			if (commaIdx == -1){
				serverNameArray.add(serverList); //Add the one server still in the input list.
				break;
			}
			else{
				String server = serverList.substring(0,commaIdx);
				serverNameArray.add(server); //Add the next server to the ArrayList.
				serverList = serverList.substring(commaIdx+1,serverList.length()); //Remove that server from the input list.
			}
		}
		
		
		client = new GNSClientCommands();
		// Make all the reads be coordinated
		client.setForceCoordinatedReads(true);
		// arun: connectivity check embedded in GNSClient constructor
		boolean connected = client instanceof GNSClient;
		if (connected) {
			System.out.println("Client created and connected to server.");
		}
		//
		int tries = 5;
		boolean accountCreated = false;

		do {
			try {
				System.out.println("Creating account guid: " + (tries - 1)
						+ " attempt remaining.");
				masterGuid = GuidUtils.lookupOrCreateAccountGuid(client,
						accountAlias, PASSWORD, true);
				accountCreated = true;
			} catch (Exception e) {
				ThreadUtils.sleep((5 - tries) * 5000);
			}
		} while (!accountCreated && --tries > 0);
		if (accountCreated == false) {
			fail("Failure setting up account guid; aborting all tests.");
		}
		
		//Begin emulating transport delays
		JSONDelayEmulator.emulateDelays();
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		if (client != null) {
			client.close();
		}
	}
	@Rule
	public TestName testName = new TestName();

	@Rule
	public TestWatcher ruleExample = new TestWatcher() {
		@Override
		protected void failed(Throwable e, Description description) {
			System.out.println(" FAILED!!!!!!!!!!!!! " + e);
			e.printStackTrace();
			System.exit(1);
		}

		@Override
		protected void succeeded(Description description) {
			System.out.println(" succeeded");
		}
	};

	@Before
	public void beforeMethod() {
		System.out.print(testName.getMethodName() + " ");
	}

	@After
	public void afterMethod() {
	}

	@Test
	public void test_010_CreateEntity() {
		String runsString = System.getProperty("integrationTest.runs");
		int numRuns = 1000;
		if (runsString != null){
			numRuns=Integer.parseInt(runsString);
		}
		for (int i = 0; i < numRuns; i++){
			if (i % 100 == 0){
				System.out.println("Running test for the " + Integer.toString(i) + "th time.");
			}
			String alias = "testGUID" + RandomString.randomString(12);
			GuidEntry guidEntry = null;
			try {
				guidEntry = GuidUtils.registerGuidWithTestTag(client, masterGuid,
						alias);
			} catch (Exception e) {
				fail("Exception while creating guid on " + Integer.toString(i)+ "th run : " + e);
			}
			assertNotNull(guidEntry);
			assertEquals(alias, guidEntry.getEntityName());
		}
	}





}
