package edu.umass.cs.gnsclient.client.testing;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
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
import edu.umass.cs.gnsclient.client.GNSClient;
import edu.umass.cs.gnsclient.client.GNSClientCommands;
import edu.umass.cs.gnsclient.client.GNSClientConfig;
import edu.umass.cs.gnsclient.client.GNSCommand;
import edu.umass.cs.gnsclient.client.testing.GNSTestingConfig.GNSTC;
import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.gnsclient.client.util.GuidUtils;
import edu.umass.cs.gnsclient.client.util.KeyPairUtils;
import edu.umass.cs.gnscommon.AclAccessType;
import edu.umass.cs.gnscommon.GNSProtocol;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnscommon.exceptions.client.DuplicateNameException;
import edu.umass.cs.gnscommon.packets.CommandPacket;
import edu.umass.cs.utils.Config;
import edu.umass.cs.utils.DefaultTest;
import edu.umass.cs.utils.DelayProfiler;
import edu.umass.cs.utils.Util;

/**
 * @author arun
 * 
 *         Tests the capacity of the GNS using a configurable number of async
 *         clients.
 *
 */
@FixMethodOrder(org.junit.runners.MethodSorters.NAME_ASCENDING)
public class GNSClientCapacityTest extends DefaultTest {

	private static String accountGUIDPrefix;
	private static final String PASSWORD = "some_password";

	// following can not be final if we want to initialize via command-line
	private static int numGuidsPerAccount;
	private static boolean accountGuidsOnly;
	private static int numClients;
	private static int numGuids;
	private static int numAccountGuids;
	private static GuidEntry[] accountGuidEntries;
	private static GuidEntry[] guidEntries;
	private static GuidEntry accessor;
	private static GNSClient[] clients;
	private static ThreadPoolExecutor executor;

	private static Logger log = GNSClientConfig.getLogger();

	/**
	 * @throws Exception
	 */
	public GNSClientCapacityTest() throws Exception {
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
		accountGUIDPrefix = Config.getGlobalString(GNSTC.ACCOUNT_GUID_PREFIX);
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
		clients = new GNSClient[numClients];
		executor = (ThreadPoolExecutor) Executors
				.newFixedThreadPool(50*numClients);
		for (int i = 0; i < numClients; i++)
			clients[i] = new GNSClientCommands();
		@SuppressWarnings("deprecation")
		String gnsInstance = GNSClient.getGNSProvider();
		accountGuidEntries = new GuidEntry[numAccountGuids];

		int numPreExisting = 0;
		for (int i = 0; i < numAccountGuids; i++) {
			log.log(Level.FINE, "Creating account GUID {0}",
					new Object[] { accountGUIDPrefix + i, });
			try {
				accountGuidEntries[i] = GuidUtils.lookupOrCreateAccountGuid(
						clients[0], accountGUIDPrefix + i, PASSWORD);
				log.log(Level.FINE, "Created account {0}",
						new Object[] { accountGuidEntries[i] });
				assert (accountGuidEntries[i].getGuid().equals(KeyPairUtils
						.getGuidEntry(gnsInstance, accountGUIDPrefix + i)
						.getGuid()));
			} catch (DuplicateNameException e) {
				numPreExisting++;
				accountGuidEntries[i] = KeyPairUtils.getGuidEntry(gnsInstance,
						accountGUIDPrefix + i);
				log.log(Level.INFO, "Found that account {0} already exists",
						new Object[] { accountGuidEntries[i] });
			}
			// any other exceptions should be thrown up
		}
		
		accessor = GuidUtils.lookupOrCreateAccountGuid(clients[0], accountGUIDPrefix+"_accessor", PASSWORD);
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
			subGuids.add(accountGUIDPrefix+Config.getGlobalString(TC.TEST_GUID_PREFIX) + i);

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
						clients[0].execute(GNSCommand.batchCreateGUIDs(accountGuidEntries[i
                              /numGuidsPerAccount], subGuids));
					} catch (Exception e) {
						for (String subGuid : subGuids) {
							try {								
								clients[0].execute(GNSCommand.guidCreate(accountGuidEntries[i
                                        / numGuidsPerAccount], subGuid));
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
			clients[0].execute(GNSCommand.fieldUpdate(guid, someField, someValue));
			// verify written value
			Assert.assertEquals(clients[0].execute(GNSCommand.fieldRead(guid, someField)).getResultJSONObject().getString(someField),
					(someValue));
			Assert.assertEquals(
					clients[numClients > 1 ? 1 : 0].execute(GNSCommand.fieldRead(guid, someField)).getResultJSONObject().getString(someField),
					(someValue));
		} catch (IOException | ClientException | JSONException e) {
			e.printStackTrace();
		} 
	}

	private static int numFinishedOps = 0;
	private static long lastOpFinishedTime = System.currentTimeMillis();

	synchronized static void incrFinishedOps() {
		numFinishedOps++;
		lastOpFinishedTime = System.currentTimeMillis();
	}

	private void blockingRead(int clientIndex, GuidEntry guid, boolean signed) {
		executor.submit(new Runnable() {
			public void run() {
				try {
					if (signed){
						assert(clients[clientIndex].execute(GNSCommand.fieldRead(guid, someField)).getResultJSONObject().getString(someField).equals(someValue));
					}
					else
						assert(clients[clientIndex].execute(GNSCommand.fieldRead(guid.getGuid(), 
								someField, null)).getResultJSONObject().getString(someField).equals(someValue));					
				} catch (ClientException | JSONException | IOException e) {
					log.severe("Client " + clientIndex + " failed to read "
							+ guid);
					e.printStackTrace();
				}
				incrFinishedOps();
			}
		});
	}
	
	private void blockingReadWithACL(int clientIndex, GuidEntry accessorGuid, String targetGuid) {
		executor.submit(new Runnable() {
			@Override
			public void run() {
				try {
					assert(clients[clientIndex].execute(GNSCommand.fieldRead(targetGuid, someField, accessorGuid)).getResultJSONObject().getString(someField).equals(someValue));
				} catch (ClientException | JSONException | IOException e) {
					log.severe("Client " + clientIndex + " failed to read "
							+ targetGuid+ " with accessor "+accessor.getGuid());
					e.printStackTrace();
				}
				incrFinishedOps();
			}			
		});
	}
	
	private void blockingWrite(int clientIndex, GuidEntry guid, int reqID) {
		executor.submit(new Runnable() {

			@Override
			public void run() {
				try {
					clients[clientIndex].execute(GNSCommand.fieldUpdate(guid, someField, someValue));					
				} catch (ClientException | IOException e) {
					log.severe("Client " + clientIndex + " failed to update "
							+ guid);
					e.printStackTrace();
				}
				incrFinishedOps();
			}
			
		});
	}
	
	
	private void blockingWriteWithACL(int clientIndex, GuidEntry accessorGuid, String targetGuid) {
		executor.submit(new Runnable() {
			@Override
			public void run() {
				try {
					clients[clientIndex].execute(GNSCommand.fieldUpdate(targetGuid, someField, someValue, accessorGuid));
				} catch (ClientException | IOException e) {
					log.severe("Client " + clientIndex + " failed to write "
							+ targetGuid+ " with accessor "+accessor.getGuid());
					e.printStackTrace();
				}
				incrFinishedOps();
			}			
		});
	}
	
	
	private void blockingRemove(int clientIndex, GuidEntry guid, int reqID) {
		executor.submit(new Runnable() {

			@Override
			public void run() {
				try {
					clients[clientIndex].execute(GNSCommand.fieldRemove(guid, someField+reqID));					
				} catch (ClientException | IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}	
				incrFinishedOps();
			}
			
		});
	}
	
	private static final int numWriteAndRemove = 50000;
	
	/**
	 * FIXME: we should update different fields, but it will trigger a derby exception.
	 * 
	 * @throws Exception
	 */
	@Test
	public void test_02_ParralelWriteCapacity() throws Exception {
		int numWrites = numWriteAndRemove;
		long t = System.currentTimeMillis();
		for (int i=0; i<numWrites; i++){
			blockingWrite(i % numClients, guidEntries[0], i);
		}
		System.out.print("[total_writes=" + numWrites+": ");
		int lastCount = 0;
		while (numFinishedOps < numWrites) {
			if(numFinishedOps>lastCount)  {
				lastCount = numFinishedOps;
				System.out.print(numFinishedOps + "@" + Util.df(numFinishedOps * 1.0 / (lastOpFinishedTime - t))+"K/s ");
			}
			Thread.sleep(1000);
		}
		System.out.print("] ");
		System.out.print("parallel_write_rate="
				+ Util.df(numWrites * 1.0 / (lastOpFinishedTime - t))
				+ "K/s");
	}
	
	/**
	 * Test write throughput with ACL 
	 * 
	 * @throws InterruptedException
	 * @throws ClientException
	 * @throws IOException
	 */
	@Test
	public void test_03_ParrallelWriteWithACLCapacity() throws InterruptedException, ClientException, IOException{
		reset();
		// The old GNS ACL implementation does not allow a GUID in ENTIRE_RECORD's ACL to write into a sub field.
		clients[0].execute(GNSCommand.aclAdd(AclAccessType.WRITE_WHITELIST, 
				guidEntries[0], someField, accessor.getGuid()));

		int numWrites = numWriteAndRemove;
		long t = System.currentTimeMillis();
		for (int i=0; i<numWrites; i++){
			blockingWriteWithACL(i%numClients, accessor, guidEntries[0].getGuid());
		}
		System.out.print("[total_writes_with_acl=" + numWrites+": ");
		int lastCount = 0;
		while (numFinishedOps < numWrites) {
			if(numFinishedOps>lastCount)  {
				lastCount = numFinishedOps;
				System.out.print(numFinishedOps + "@" + Util.df(numFinishedOps * 1.0 / (lastOpFinishedTime - t))+"K/s ");
			}
			Thread.sleep(1000);
		}
		System.out.print("] ");
		System.out.print("parallel_write_with_acl_rate="
				+ Util.df(numWrites * 1.0 / (lastOpFinishedTime - t))
				+ "K/s");
		
		clients[0].execute(GNSCommand.aclRemove(AclAccessType.WRITE_WHITELIST, 
				guidEntries[0], someField, accessor.getGuid()));
			
	}
	
	/**
	 * @throws Exception
	 */
	@Test
	public void test_11_ParallelSignedReadCapacity() throws Exception {
		reset();
		int numReads = Config.getGlobalInt(TC.NUM_REQUESTS);
		long t = System.currentTimeMillis();
		for (int i = 0; i < numReads; i++) {
			blockingRead(i % numClients, guidEntries[0], true);
		}
		System.out.print("[total_signed_reads=" + numReads+": ");
		int lastCount = 0;
		while (numFinishedOps < numReads) {
			if(numFinishedOps>lastCount)  {
				lastCount = numFinishedOps;
				System.out.print(numFinishedOps + "@" + Util.df(numFinishedOps * 1.0 / (lastOpFinishedTime - t))+"K/s ");
			}
			Thread.sleep(1000);
		}
		System.out.print("] ");

		System.out.print("parallel_signed_read_rate="
				+ Util.df(numReads * 1.0 / (lastOpFinishedTime - t))
				+ "K/s");
	}

	private void reset() {
		numFinishedOps = 0;
		lastOpFinishedTime = System.currentTimeMillis();
	}

	/**
	 * @throws Exception
	 */
	@Test
	public void test_12_ParallelUnsignedReadCapacity() throws Exception {
		
		int numReads = Config.getGlobalInt(TC.NUM_REQUESTS);
		reset();
		long t = System.currentTimeMillis();
		for (int i = 0; i < numReads; i++) {
			blockingRead(i % numClients, guidEntries[0], false);
		}
		int j = 1;
		System.out.print("[total_unsigned_reads=" + numReads+": ");
		while (numFinishedOps < numReads) {
			if (numFinishedOps >= j) {
				j *= 2;
				System.out.print(numFinishedOps + "@" + Util.df(numFinishedOps * 1.0 / (lastOpFinishedTime - t))+"K/s ");
			}
			Thread.sleep(500);
		}
		System.out.print("] ");
		System.out.print("parallel_unsigned_read_rate="
				+ Util.df(numReads * 1.0 / (lastOpFinishedTime - t))
				+ "K/s");
		
	}

	/**
	 * @throws ClientException
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@Test
	public void test_13_ParallelReadWithACLCapacity() throws ClientException, IOException, InterruptedException {
		reset();
		int numReads = Config.getGlobalInt(TC.NUM_REQUESTS);
		clients[0].execute(GNSCommand.aclAdd(AclAccessType.READ_WHITELIST, guidEntries[0], 
				GNSProtocol.ENTIRE_RECORD.toString(), accessor.getGuid()));
		
		long t = System.currentTimeMillis();
		for (int i = 0; i < numReads; i++) {
			blockingReadWithACL( i%numClients, accessor, guidEntries[0].getGuid());
		}
		int j = 1;
		System.out.print("[total_reads_with_acl=" + numReads+": ");
		while (numFinishedOps < numReads) {
			if (numFinishedOps >= j) {
				j *= 2;
				System.out.print(numFinishedOps + "@" + Util.df(numFinishedOps * 1.0 / (lastOpFinishedTime - t))+"K/s ");
			}
			Thread.sleep(500);
		}
		System.out.print("] ");
		System.out.print("parallel_read_with_acl_rate="
				+ Util.df(numReads * 1.0 / (lastOpFinishedTime - t))
				+ "K/s");
		
		clients[0].execute(GNSCommand.aclRemove(AclAccessType.READ_WHITELIST, guidEntries[0], 
				GNSProtocol.ENTIRE_RECORD.toString(), accessor.getGuid()));
	}
	
	/**
	 * FIXME: we should remove different fields, but it will trigger a derby exception.
	 * 
	 * @throws Exception
	 */
	@Test
	public void test_21_ParallelRemoveCapacity() throws Exception {
		reset();
		int numRemoves = numWriteAndRemove;
		long t = System.currentTimeMillis();
		for (int i=0; i<numRemoves; i++){
			blockingRemove(i % numClients, guidEntries[0], i);
		}
		System.out.print("[total_removes=" + numRemoves+": ");
		int lastCount = 0;
		while (numFinishedOps < numRemoves) {
			if(numFinishedOps>lastCount)  {
				lastCount = numFinishedOps;
				System.out.print(numFinishedOps + "@" + Util.df(numFinishedOps * 1.0 / (lastOpFinishedTime - t))+"K/s ");
			}
			Thread.sleep(1000);
		}
		System.out.print("] ");
		System.out.print("parallel_remove_rate="
				+ Util.df(numRemoves * 1.0 / (lastOpFinishedTime - t))
				+ "K/s");
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
					if(guidEntry!=null)
						clients[0].execute(GNSCommand.guidRemove(guidEntry));
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
				if(accGuidEntry != null)
					clients[0].execute(GNSCommand.accountGuidRemove(accGuidEntry));
				log.log(Level.FINE, "Deleted account guid {0}",
						new Object[] { accGuidEntry });
			} catch (Exception e) {
				log.log(Level.WARNING, "Failed to delete account guid {0}",
						new Object[] { accGuidEntry });
				e.printStackTrace();
				// continue with rest
			}
		}
		for (GNSClient client : clients)
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
		Result result = JUnitCore.runClasses(GNSClientCapacityTest.class);
		for (Failure failure : result.getFailures()) {
			System.out.println(failure.getMessage());
			failure.getException().printStackTrace();
		}
	}
}
