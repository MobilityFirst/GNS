package edu.umass.cs.gnsclient.client.bugreports;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.After;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;

import edu.umass.cs.gigapaxos.testing.TESTPaxosConfig;
import edu.umass.cs.gigapaxos.testing.TESTPaxosConfig.TC;
import edu.umass.cs.gnsclient.client.GNSClient;
import edu.umass.cs.gnsclient.client.GNSClientCommands;
import edu.umass.cs.gnsclient.client.GNSClientConfig;
import edu.umass.cs.gnsclient.client.testing.GNSTestingConfig.GNSTC;
import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.gnsclient.client.util.KeyPairUtils;
import edu.umass.cs.gnscommon.exceptions.client.DuplicateNameException;
import edu.umass.cs.utils.Config;
import edu.umass.cs.utils.DefaultTest;
import edu.umass.cs.utils.Util;

/**
 *
 * @author westy
 */
public class BatchCreateOpsFail extends DefaultTest {

  private static int numGuidsPerAccount;
  private static boolean accountGuidsOnly;

  private static final String ACCOUNT_GUID_PREFIX = "ACCOUNT_GUID";
  private static final String PASSWORD = "some_password";

  private static int numClients;
  private static int numGuids;
  private static int numAccountGuids;
  private static GuidEntry[] accountGuidEntries;
  private static GuidEntry[] guidEntries;
  private static GNSClientCommands[] clients;

  private static Logger log = GNSClientConfig.getLogger();

  /**
   *
   * @throws Exception
   */
  public BatchCreateOpsFail() throws Exception {
    initParameters();
    setupClientsAndGuids();
  }

  private static void initParameters() {
    numGuidsPerAccount = Config.getGlobalInt(GNSTC.NUM_GUIDS_PER_ACCOUNT);
    accountGuidsOnly = Config.getGlobalBoolean(GNSTC.ACCOUNT_GUIDS_ONLY);
    numClients = Config.getGlobalInt(TC.NUM_CLIENTS);
    numGuids = Config.getGlobalInt(TC.NUM_GROUPS);
    numAccountGuids = accountGuidsOnly ? numGuids : Math.max((int) Math.ceil(numGuids * 1.0
            / numGuidsPerAccount), 1);
  }

  private void setupClientsAndGuids() throws Exception {
    clients = new GNSClientCommands[numClients];
    for (int i = 0; i < numClients; i++) {
      clients[i] = new GNSClientCommands(new GNSClient());
    }
    String gnsInstance = clients[0].getGNSProvider();
    accountGuidEntries = new GuidEntry[numAccountGuids];

    for (int i = 0; i < numAccountGuids; i++) {
      log.log(Level.FINE, "{0} creating account GUID {1}", new Object[]{
        this, ACCOUNT_GUID_PREFIX + i,});
      try {
        accountGuidEntries[i] = clients[0].accountGuidCreate(
                ACCOUNT_GUID_PREFIX + i, PASSWORD);
        log.log(Level.FINE, "{0} created account ", new Object[]{
          this, accountGuidEntries[i]});
        assert (accountGuidEntries[i].getGuid().equals(KeyPairUtils
                .getGuidEntry(gnsInstance, ACCOUNT_GUID_PREFIX + i)
                .getGuid()));
      } catch (DuplicateNameException e) {
        accountGuidEntries[i] = KeyPairUtils.getGuidEntry(gnsInstance,
                ACCOUNT_GUID_PREFIX + i);
        log.log(Level.INFO,
                "{0} found that account {1} already exists",
                new Object[]{this, accountGuidEntries[i]});
      }
      // any other exception should be throw up
    }

    System.out.println("Created or found pre-existing " + numAccountGuids
            + " account GUIDs: " + Arrays.asList(accountGuidEntries));

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
        log.log(Level.FINE, "{0} created sub-guid(s) {1}",
                new Object[]{this, subGuids});
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

  /**
   *
   * @throws Exception
   */
  @After
  public void cleanup() throws Exception {
    Thread.sleep(2000);
    assert (clients != null && clients[0] != null);
    System.out.println("About to delete " + guidEntries.length
            + " sub-guids: " + Arrays.asList(guidEntries));
    for (GuidEntry guidEntry : guidEntries) {
      try {
        log.log(Level.INFO, "{0} about to delete sub-guid {1}",
                new Object[]{this, guidEntry});
        clients[0].guidRemove(guidEntry);
        log.log(Level.INFO, "{0} deleted sub-guid {1}", new Object[]{
          this, guidEntry});
      } catch (Exception e) {
        log.log(Level.WARNING, "{0} failed to delete sub-guid {1}",
                new Object[]{this, guidEntry});
        e.printStackTrace();
        // continue with rest
      }
    }
    System.out.println("About to delete " + accountGuidEntries.length
            + " account guids: " + Arrays.asList(accountGuidEntries));
    for (GuidEntry accGuidEntry : accountGuidEntries) {
      try {
        clients[0].accountGuidRemove(accGuidEntry);
        log.log(Level.FINE, "{0} deleted account guid {1}",
                new Object[]{this, accGuidEntry});
      } catch (Exception e) {
        log.log(Level.WARNING, "{0} failed to delete account guid {1}",
                new Object[]{this, accGuidEntry});
        e.printStackTrace();
        // continue with rest
      }
    }
    for (GNSClientCommands client : clients) {
      client.close();
    }
  }

  private static void processArgs(String[] args) throws IOException {
    // TODO: add command-line args support to Config
    Config.register(TC.class, TESTPaxosConfig.TESTING_CONFIG_FILE_KEY,
            TESTPaxosConfig.DEFAULT_TESTING_CONFIG_FILE);
  }

  /**
   *
   * @param args
   * @throws IOException
   */
  public static void main(String[] args) throws IOException {
    Util.assertAssertionsEnabled();
    processArgs(args);
    Result result = JUnitCore.runClasses(BatchCreateOpsFail.class);
    for (Failure failure : result.getFailures()) {
      System.out.println(failure.getMessage());
      failure.getException().printStackTrace();
    }
  }
}
