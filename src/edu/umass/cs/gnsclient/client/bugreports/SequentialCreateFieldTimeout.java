package edu.umass.cs.gnsclient.client.bugreports;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.io.IOException;

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
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;
import org.junit.runners.MethodSorters;

import edu.umass.cs.gnsclient.client.GNSClient;
import edu.umass.cs.gnsclient.client.GNSClientCommands;
import edu.umass.cs.gnsclient.client.testing.GNSClientCapacityTest;
import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.gnsclient.client.util.GuidUtils;
import edu.umass.cs.gnscommon.utils.RandomString;
import edu.umass.cs.gnscommon.utils.ThreadUtils;
import edu.umass.cs.utils.Util;

/**
 * @author Brendan
 * 
 * Repeats the Create_Entity test of ServerIntegrationTest a number of times sequentially to try and reproduce a timeout error.
 *
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class SequentialCreateFieldTimeout {

	private static final String DEFAULT_ACCOUNT_ALIAS = "support@gns.name";

	private static String accountAlias = DEFAULT_ACCOUNT_ALIAS; // REPLACE																// ALIAS
	private static final String PASSWORD = "password";
	private static GNSClientCommands client = null;
	private static GuidEntry masterGuid;

	public static void setAccountAlias(String alias) {
		accountAlias = alias;
	}

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
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
	public void test_010_CreateEntity() throws Exception {
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
				guidEntry = GuidUtils.registerGuidWithTestTag(client, masterGuid,
						alias);
			assertNotNull(guidEntry);
			assertEquals(alias, guidEntry.getEntityName());
		}
	}

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		Util.assertAssertionsEnabled();
		Result result = JUnitCore.runClasses(GNSClientCapacityTest.class);
		for (Failure failure : result.getFailures()) {
			System.out.println(failure.getMessage());
			failure.getException().printStackTrace();
		}
	}



}
