package edu.umass.cs.gnsclient.client.bugreports;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.junit.Assert;
import org.json.JSONException;
import org.junit.AfterClass;
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
import edu.umass.cs.gnsclient.client.util.KeyPairUtils;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnscommon.exceptions.client.DuplicateNameException;
import edu.umass.cs.utils.Config;
import edu.umass.cs.utils.DefaultTest;
import edu.umass.cs.utils.Util;

/**
 * @author arun
 *
 * Fixed. The bug was a concurrency bug in using signature instances at
 * the client as well as the server. The fix was to pre-create multiple
 * signature instances (for parallelism) and synchronize upon the
 * instance when using it.
 *
 * Tests the capacity of the GNS using a configurable number of async
 * clients.
 *
 */
@FixMethodOrder(org.junit.runners.MethodSorters.NAME_ASCENDING)
public class SomeReadsEncryptionFails extends DefaultTest {

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
  public SomeReadsEncryptionFails() throws Exception {
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
    String gnsInstance = clients[0].getGNSProvider();
    accountGuidEntries = new GuidEntry[numAccountGuids];

    int numPreExisting = 0;
    for (int i = 0; i < numAccountGuids; i++) {
      log.log(Level.FINE, "Creating account GUID {0}",
              new Object[]{ACCOUNT_GUID_PREFIX + i,});
      try {
        accountGuidEntries[i] = clients[0].accountGuidCreate(
                ACCOUNT_GUID_PREFIX + i, PASSWORD);
        log.log(Level.FINE, "Created account {0}",
                new Object[]{accountGuidEntries[i]});
        assert (accountGuidEntries[i].getGuid().equals(KeyPairUtils
                .getGuidEntry(gnsInstance, ACCOUNT_GUID_PREFIX + i)
                .getGuid()));
      } catch (DuplicateNameException e) {
        numPreExisting++;
        accountGuidEntries[i] = KeyPairUtils.getGuidEntry(gnsInstance,
                ACCOUNT_GUID_PREFIX + i);
        log.log(Level.INFO, "Found that account {0} already exists",
                new Object[]{accountGuidEntries[i]});
      }
      // any other exception should be throw up
    }

    System.out.println("Created (" + (numAccountGuids - numPreExisting)
            + ") or found pre-existing (" + numPreExisting
            + ") a total of " + numAccountGuids + " account GUIDs: "
            + Arrays.asList(accountGuidEntries));

    if (accountGuidsOnly) {
      for (int i = 0; i < accountGuidEntries.length; i++) {
        guidEntries[i] = accountGuidEntries[i];
      }
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
            GuidEntry created = clients[0].guidCreate(
                    accountGuidEntries[i / numGuidsPerAccount],
                    subGuid);
            assert (created.getGuid().equals(KeyPairUtils
                    .getGuidEntry(gnsInstance, subGuid).getGuid()));
          } catch (DuplicateNameException de) {
            // ignore, will retrieve it locally below
          }
          // any other exception should be throw up
        } else {
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
        }
        for (String subGuid : subGuids) {
          guidEntries[j++] = KeyPairUtils.getGuidEntry(gnsInstance,
                  subGuid);
        }
        log.log(Level.FINE, "Created sub-guid(s) {0}",
                new Object[]{subGuids});
        subGuids.clear();
      }
    }
    for (GuidEntry guidEntry : accountGuidEntries) {
      assert (guidEntry != null);
    }
    for (GuidEntry guidEntry : guidEntries) {
      assert (guidEntry != null);
    }

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
    } catch (IOException | ClientException e) {
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
  public void test_02_SequentialReadCapacity() throws Exception {
    int numReads = Math.min(1000, Config.getGlobalInt(TC.NUM_REQUESTS));
    long t = System.currentTimeMillis();
    for (int i = 0; i < numReads; i++) {
      clients[(int) (Math.random() * numClients)].fieldRead(
              guidEntries[0], someField);
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

  private void blockingRead(int clientIndex, GuidEntry guid) {
    executor.submit(new Runnable() {
      public void run() {
        try {
          clients[clientIndex].fieldRead(guid, someField);
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
  @Test
  public void test_03_ParallelReadCapacity() throws Exception {
    int numReads = Config.getGlobalInt(TC.NUM_REQUESTS);
    long t = System.currentTimeMillis();
    for (int i = 0; i < numReads; i++) {
      blockingRead(0, guidEntries[0]);
    }
    int j = 1;
    while (numFinishedReads < numReads) {
      if (numFinishedReads >= j) {
        j *= 2;
        System.out.print(numFinishedReads + " ");
      }
      Thread.sleep(500);
    }
    System.out.print("sequential_read_rate="
            + Util.df(numReads * 1.0 / (lastReadFinishedTime - t))
            + "K/s averaged over " + numReads + " reads.");
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
                  new Object[]{guidEntry});
          clients[0].guidRemove(guidEntry);
          log.log(Level.FINE, "Deleted sub-guid {0}",
                  new Object[]{guidEntry});
        } catch (Exception e) {
          log.log(Level.WARNING, "Failed to delete sub-guid {0}",
                  new Object[]{guidEntry});
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
                new Object[]{accGuidEntry});
        clients[0].accountGuidRemove(accGuidEntry);
        log.log(Level.FINE, "Deleted account guid {0}",
                new Object[]{accGuidEntry});
      } catch (Exception e) {
        log.log(Level.WARNING, "Failed to delete account guid {0}",
                new Object[]{accGuidEntry});
        e.printStackTrace();
        // continue with rest
      }
    }
    for (GNSClientCommands client : clients) {
      client.close();
    }
    executor.shutdown();
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
    Result result = JUnitCore.runClasses(SomeReadsEncryptionFails.class);
    for (Failure failure : result.getFailures()) {
      System.out.println(failure.getMessage());
      failure.getException().printStackTrace();
    }
  }
}
