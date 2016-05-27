package edu.umass.cs.gnsclient.client.testing;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.json.JSONException;
import org.json.JSONObject;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;

import edu.umass.cs.gigapaxos.testing.TESTPaxosConfig;
import edu.umass.cs.gigapaxos.testing.TESTPaxosConfig.TC;
import edu.umass.cs.gigapaxos.testing.TESTPaxosMain;
import edu.umass.cs.gnsclient.client.GNSClientCommands;
import edu.umass.cs.gnsclient.client.GNSClientConfig;
import edu.umass.cs.gnsclient.client.GuidEntry;
import edu.umass.cs.gnsclient.client.testing.GNSTestingConfig.GNSTC;
import edu.umass.cs.gnsclient.client.util.GuidUtils;
import edu.umass.cs.gnsclient.client.util.KeyPairUtils;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnscommon.exceptions.client.DuplicateNameException;
import edu.umass.cs.reconfiguration.testing.TESTReconfigurationConfig;
import edu.umass.cs.reconfiguration.testing.TESTReconfigurationConfig.TRC;
import edu.umass.cs.utils.Config;
import edu.umass.cs.utils.DefaultTest;
import edu.umass.cs.utils.DelayProfiler;
import edu.umass.cs.utils.Util;

/**
 * @author Brendan
 * 			Based on GNSClientCapcityTest
 * 
 *         Tests the capacity of the GNS for sequential field additions and updates.
 *
 */
@FixMethodOrder(org.junit.runners.MethodSorters.NAME_ASCENDING)
public class GNSClientAddFieldTest extends DefaultTest {

	private static final String ACCOUNT_GUID_PREFIX = "ACCOUNT_GUID";
	private static final String PASSWORD = "some_password";

	// following can not be final if we want to initialize via command-line
	private static int numGuidsPerAccount;
	private static boolean accountGuidsOnly;
	private static int numClients;
	private static int numGuids;
	private static int numAccountGuids;
	private static GuidEntry[] accountGuidEntries;
	private static GuidEntry[] guidEntries;
	private static GNSClientCommands[] clients;
	private static ScheduledThreadPoolExecutor executor;

	private static Logger log = GNSClientConfig.getLogger();

	/**
	 * @throws Exception
	 */
	public GNSClientAddFieldTest() throws Exception {
	}

	/**
	 * @throws Exception
	 */
	@BeforeClass
	public static void setup() throws Exception {
		initStaticParams();
		setupClientsAndGuids();
	}

	private static void initStaticParams() {
		numGuidsPerAccount = Config.getGlobalInt(GNSTC.NUM_GUIDS_PER_ACCOUNT);
		accountGuidsOnly = Config.getGlobalBoolean(GNSTC.ACCOUNT_GUIDS_ONLY);
		numClients = Config.getGlobalInt(TC.NUM_CLIENTS);
		numGuids = Config.getGlobalInt(TC.NUM_GROUPS);
		numAccountGuids = accountGuidsOnly ? numGuids : Math.max(
				(int) Math.ceil(numGuids * 1.0 / numGuidsPerAccount), 1);
		accountGuidEntries = new GuidEntry[numAccountGuids];
		guidEntries = new GuidEntry[numGuids];
	}

	private static void setupClientsAndGuids() throws Exception {
		clients = new GNSClientCommands[numClients];
		executor = (ScheduledThreadPoolExecutor) Executors
				.newScheduledThreadPool(numClients);
		for (int i = 0; i < numClients; i++)
			clients[i] = new GNSClientCommands();
		String gnsInstance = clients[0].getGNSInstance();
		accountGuidEntries = new GuidEntry[numAccountGuids];

		int numPreExisting = 0;
		for (int i = 0; i < numAccountGuids; i++) {
			log.log(Level.FINE, "Creating account GUID {0}",
					new Object[] { ACCOUNT_GUID_PREFIX + i, });
			try {
				accountGuidEntries[i] = GuidUtils.lookupOrCreateAccountGuid(
						clients[0], ACCOUNT_GUID_PREFIX + i, PASSWORD);
				log.log(Level.FINE, "Created account {0}",
						new Object[] { accountGuidEntries[i] });
				assert (accountGuidEntries[i].getGuid().equals(KeyPairUtils
						.getGuidEntry(gnsInstance, ACCOUNT_GUID_PREFIX + i)
						.getGuid()));
			} catch (DuplicateNameException e) {
				numPreExisting++;
				accountGuidEntries[i] = KeyPairUtils.getGuidEntry(gnsInstance,
						ACCOUNT_GUID_PREFIX + i);
				log.log(Level.INFO, "Found that account {0} already exists",
						new Object[] { accountGuidEntries[i] });
			}
			// any other exceptions should be thrown up
		}

		System.out.println("Created (" + (numAccountGuids - numPreExisting)
				+ ") or found pre-existing (" + numPreExisting
				+ ") a total of " + numAccountGuids + " account GUIDs: "
				+ Arrays.asList(accountGuidEntries));

		if (accountGuidsOnly) {
			for (int i = 0; i < accountGuidEntries.length; i++)
				guidEntries[i] = accountGuidEntries[i];
			return;
		}

		guidEntries = new GuidEntry[numGuids];
		Set<String> subGuids = new HashSet<String>();
		for (int i = 0, j = 0; i < numGuids; i++) {
			subGuids.add(Config.getGlobalString(TC.TEST_GUID_PREFIX) + i);

			if (subGuids.size() == numGuidsPerAccount || i == numGuids - 1) {
				// because batch creation seems buggy
				if (subGuids.size() == 1) {
					String subGuid = subGuids.iterator().next();
					try {
						GuidEntry created = GuidUtils.lookupOrCreateGuid(
								clients[0], accountGuidEntries[i
										/ numGuidsPerAccount], subGuid);
						assert (created.getGuid().equals(KeyPairUtils
								.getGuidEntry(gnsInstance, subGuid).getGuid()));
					} catch (DuplicateNameException de) {
						// ignore, will retrieve it locally below
					}
					// any other exceptions should be thrown up
				} else
					try {
						// batch create
						clients[0].guidBatchCreate(accountGuidEntries[i
								/ numGuidsPerAccount], subGuids);
					} catch (Exception e) {
						for (String subGuid : subGuids) {
							try {
								clients[0].guidCreate(accountGuidEntries[i
										/ numGuidsPerAccount], subGuid);
							} catch (DuplicateNameException de) {
								// ignore, will retrieve it locally below
							}
							// any other exception should be throw up
						}
					}
				for (String subGuid : subGuids) {
					guidEntries[j++] = KeyPairUtils.getGuidEntry(gnsInstance,
							subGuid);
				}
				log.log(Level.FINE, "Created sub-guid(s) {0}",
						new Object[] { subGuids });
				subGuids.clear();
			}
		}
		for (GuidEntry guidEntry : accountGuidEntries)
			assert (guidEntry != null);
		for (GuidEntry guidEntry : guidEntries)
			assert (guidEntry != null);

		System.out.println("Created or found " + guidEntries.length
				+ " pre-existing sub-guids " + Arrays.asList(guidEntries));
	}

	private static final String someField = "ipv6";
	private static final String someValue = "2001:0db8:3c4d:0015::1a2f:1a2b";

	/**
	 * Verifies that a single field can be successfully added.
	 * @throws Exception 
	 */
	@Test
	public void test_01_SingleAddField() throws Exception {
		GuidEntry guid = guidEntries[0];
			clients[0].update(guid, new JSONObject("{\"" + someField + "\":\"" + someValue + "\"}"));
			// verify written value
			Assert.assertEquals(clients[0].fieldRead(guid, someField),
					(someValue));
			Assert.assertEquals(
					clients[numClients > 1 ? 1 : 0].fieldRead(guid, someField),
					(someValue));
	}
	
	/**
	 * Tests that NUM_REQUESTS different fields are successfully added by first adding all of the fields and then reading their values back.
	 * @throws Exception
	 * 
	 */
	@Test
	public void test_02_AddManyFieldsSequentially() throws Exception {
		GuidEntry guid = guidEntries[0];
		int numAdds = Config.getGlobalInt(TC.NUM_REQUESTS);
		long t = System.currentTimeMillis();
		for (int i = 0; i < numAdds; i++) {
			clients[0].update(guid, new JSONObject("{\"testField#"+ Integer.toString(i)+"\":\"" + Integer.toString(i) + "\"}"));
		}
		System.out.println("sequential_field_add_rate="
				+ Util.df(numAdds * 1.0 / (System.currentTimeMillis() - t))
				+ "K/s averaged over " + numAdds + " additions.");
		System.out.println("Checking field values...");
		for (int i = 0; i < numAdds; i++) {
			String response = clients[0].fieldRead(guid, "testField#"+ Integer.toString(i));
			assertTrue("Failed to read the correct value of testField#" + Integer.toString(i) +". Expected: " + Integer.toString(i) + ", but got: " + response, response.equals(Integer.toString(i)));;
		}
	}
	
	
	/**
	 * Performs NUM_REQUESTS sequential field updates to test write capacity.
	 * @throws Exception
	 * 
	 */
	@Test
	public void test_03_SequentialWrites() throws Exception {
		GuidEntry guid = guidEntries[0];
		int numAdds = Math.min(1000, Config.getGlobalInt(TC.NUM_REQUESTS));
		long t = System.currentTimeMillis();
		for (int i = 0; i < numAdds; i++) {
			clients[0].fieldUpdate(guid, someField, someValue);
		}
		System.out.print("sequential_write_rate="
				+ Util.df(numAdds * 1.0 / (System.currentTimeMillis() - t))
				+ "K/s averaged over " + numAdds + " additions.");
	}

	/**
	 * Removes all account and sub-guids created during the test.
	 * 
	 * @throws Exception
	 */
	@AfterClass
	public static void cleanup() throws Exception {
		Thread.sleep(2000);
		assert (clients != null && clients[0] != null);
		if (!accountGuidsOnly) {
			System.out.println("About to delete " + guidEntries.length
					+ " sub-guids: " + Arrays.asList(guidEntries));
			for (GuidEntry guidEntry : guidEntries) {
				try {
					log.log(Level.FINE, "About to delete sub-guid {0}",
							new Object[] { guidEntry });
					clients[0].guidRemove(guidEntry);
					log.log(Level.FINE, "Deleted sub-guid {0}",
							new Object[] { guidEntry });
				} catch (Exception e) {
					log.log(Level.WARNING, "Failed to delete sub-guid {0}",
							new Object[] { guidEntry });
					e.printStackTrace();
					// continue with rest
				}
			}
		}
		System.out.println("About to delete " + accountGuidEntries.length
				+ " account guids: " + Arrays.asList(accountGuidEntries));
		for (GuidEntry accGuidEntry : accountGuidEntries) {
			try {
				log.log(Level.FINE, "About to delete account guid {0}",
						new Object[] { accGuidEntry });
				clients[0].accountGuidRemove(accGuidEntry);
				log.log(Level.FINE, "Deleted account guid {0}",
						new Object[] { accGuidEntry });
			} catch (Exception e) {
				log.log(Level.WARNING, "Failed to delete account guid {0}",
						new Object[] { accGuidEntry });
				e.printStackTrace();
				// continue with rest
			}
		}
		for (GNSClientCommands client : clients)
			client.close();
		executor.shutdown();
		System.out.println(DelayProfiler.getStats());
	}

	private static void processArgs(String[] args) throws IOException {
		Config.register(args);
	}

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		Util.assertAssertionsEnabled();
		processArgs(args);
		Result result = JUnitCore.runClasses(GNSClientAddFieldTest.class);
		for (Failure failure : result.getFailures()) {
			System.out.println(failure.getMessage());
			failure.getException().printStackTrace();
		}
	}
}
