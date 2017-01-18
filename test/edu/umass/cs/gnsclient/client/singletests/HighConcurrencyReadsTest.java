package edu.umass.cs.gnsclient.client.singletests;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.json.JSONException;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;

import edu.umass.cs.gigapaxos.testing.TESTPaxosConfig.TC;
import edu.umass.cs.gnsclient.client.GNSClientCommands;
import edu.umass.cs.gnsclient.client.GNSClientConfig;
import edu.umass.cs.gnsclient.client.testing.GNSTestingConfig.GNSTC;
import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.gnsclient.client.util.GuidUtils;
import edu.umass.cs.gnsclient.client.util.KeyPairUtils;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnscommon.exceptions.client.DuplicateNameException;
import edu.umass.cs.utils.Config;
import edu.umass.cs.utils.DefaultTest;
import edu.umass.cs.utils.DelayProfiler;
import edu.umass.cs.utils.Util;

/**
 * @author arun
 * 
 *         Fixed. The fix involved two bugs in reconfiguration: (1) Fixed a bug
 *         in NIO related to SelectionKey.interestOps(.) that makes concurrent
 *         use of the method seemingly not thread-safe in contrast to the
 *         documentation. (2) Fixed a concurrency bug in
 *         ReconfigurableAppClientAsync that caused some requests to get stuck
 *         in the queue waiting for active replicas.
 * 
 *         Symptom: Some reads fail under high concurrency. Sometimes a single
 *         write fails as well.
 *
 */
@FixMethodOrder(org.junit.runners.MethodSorters.NAME_ASCENDING)
public class HighConcurrencyReadsTest extends DefaultTest {

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
	public HighConcurrencyReadsTest() throws Exception {
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
		for (int i = 0; i < numClients; i++) {
                  clients[i] = new GNSClientCommands();
                }
		String gnsInstance = GNSClientCommands.getGNSProvider();
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

	private static final String someField = "someField";
	private static final String someValue = "someValue";

	/**
	 * Verifies a single write is successful.
	 */
	@Test
	public void test_01_SingleWrite() {
		GuidEntry guid = guidEntries[0];
		try {
			clients[0].fieldUpdate(guid, someField, someValue);
			// verify written value
			Assert.assertEquals(clients[0].fieldRead(guid, someField),
					(someValue));
			Assert.assertEquals(
					clients[numClients > 1 ? 1 : 0].fieldRead(guid, someField),
					(someValue));
		} catch (IOException | ClientException | JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * @throws Exception
	 * 
	 */
	@Test
	public void test_02_SequentialSignedReadCapacity() throws Exception {
		int numReads = Math.min(1000, Config.getGlobalInt(TC.NUM_REQUESTS));
		long t = System.currentTimeMillis();
		for (int i = 0; i < numReads; i++) {
			clients[0].fieldRead(guidEntries[0], someField);
		}
		System.out.print("sequential_read_rate="
				+ Util.df(numReads * 1.0 / (System.currentTimeMillis() - t))
				+ "K/s averaged over " + numReads + " reads.");
	}

	/**
	 * @throws Exception
	 */
	@Test
	public void test_02_SequentialUnsignedReadCapacity() throws Exception {
		int numReads = Math.min(1000, Config.getGlobalInt(TC.NUM_REQUESTS));
		long t = System.currentTimeMillis();
		for (int i = 0; i < numReads; i++) {
			clients[0].fieldRead(guidEntries[0].getGuid(), someField, null);
		}
		System.out.print("sequential_read_rate="
				+ Util.df(numReads * 1.0 / (System.currentTimeMillis() - t))
				+ "K/s averaged over " + numReads + " reads.");
	}

	private static int numFinishedReads = 0;
	private static long lastReadFinishedTime = System.currentTimeMillis();

	synchronized static void incrFinishedReads() {
		numFinishedReads++;
		lastReadFinishedTime = System.currentTimeMillis();
	}

	private void blockingRead(int clientIndex, GuidEntry guid, boolean signed) {
		executor.submit(new Runnable() {
			public void run() {
				try {
					if (signed)
						assert (clients[clientIndex].fieldRead(guid, someField)
								.equals(someValue));
					else
						assert (clients[clientIndex].fieldRead(guid.getGuid(),
								someField, null).equals(someValue));

					incrFinishedReads();
				} catch (Exception e) {
					log.severe("Client " + clientIndex + " failed to read "
							+ guid);
					e.printStackTrace();
				}
			}
		});
	}

	/**
	 * @throws Exception
	 */
	// @Test
	public void test_03_ParallelSignedReadCapacity() throws Exception {
		int numReads = Math.min(10000, Config.getGlobalInt(TC.NUM_REQUESTS));
		long t = System.currentTimeMillis();
		for (int i = 0; i < numReads; i++) {
			blockingRead(numReads % numClients, guidEntries[0], true);
		}
		System.out.print("[total_reads=" + numReads + ": ");
		int lastCount = 0;
		while (numFinishedReads < numReads) {
			if (numFinishedReads > lastCount) {
				lastCount = numFinishedReads;
				System.out.print(numFinishedReads + " ");
			}
			Thread.sleep(1000);
		}
		System.out.print("] ");

		System.out.print("parallel_signed_read_rate="
				+ Util.df(numReads * 1.0 / (lastReadFinishedTime - t)) + "K/s");
	}

	private void reset() {
		numFinishedReads = 0;
		lastReadFinishedTime = System.currentTimeMillis();
	}

	/**
	 * @throws Exception
	 */
	@Test
	public void test_04_ParallelUnsignedReadCapacity() throws Exception {
		int numReads = Config.getGlobalInt(TC.NUM_REQUESTS);
		reset();
		long t = System.currentTimeMillis();
		for (int i = 0; i < numReads; i++) {
			blockingRead(numReads % numClients, guidEntries[0], false);
		}
		int j = 1;
		System.out.print("[total_reads=" + numReads + ": ");
		while (numFinishedReads < numReads) {
			if (numFinishedReads >= j) {
				j *= 2;
				System.out.print(numFinishedReads + " ");
			}
			Thread.sleep(500);
		}
		System.out.print("] ");
		System.out.print("parallel_unsigned_read_rate="
				+ Util.df(numReads * 1.0 / (lastReadFinishedTime - t)) + "K/s");
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
		Result result = JUnitCore.runClasses(HighConcurrencyReadsTest.class);
		for (Failure failure : result.getFailures()) {
			System.out.println(failure.getMessage());
			failure.getException().printStackTrace();
		}
	}
}
