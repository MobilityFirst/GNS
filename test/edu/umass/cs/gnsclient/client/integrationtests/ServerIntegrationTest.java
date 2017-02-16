/* Copyright (c) 2015 University of Massachusetts
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 * 
 * Initial developer(s): Westy, arun */
package edu.umass.cs.gnsclient.client.integrationtests;

import edu.umass.cs.gnsserver.utils.RunCommand;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;

import edu.umass.cs.contextservice.client.ContextServiceClient;
import edu.umass.cs.gigapaxos.PaxosConfig;
import edu.umass.cs.gigapaxos.paxosutil.RequestInstrumenter;
import edu.umass.cs.gnsclient.client.GNSClient;
import edu.umass.cs.gnsclient.client.GNSClientCommands;
import edu.umass.cs.gnsclient.client.GNSCommand;
import edu.umass.cs.gnsclient.client.util.BasicGuidEntry;
import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.gnsclient.client.util.GuidUtils;
import edu.umass.cs.gnsclient.client.util.JSONUtils;
import edu.umass.cs.gnsclient.client.util.SHA1HashFunction;
import edu.umass.cs.gnsclient.jsonassert.JSONAssert;
import edu.umass.cs.gnsclient.jsonassert.JSONCompareMode;
import edu.umass.cs.gnscommon.AclAccessType;
import edu.umass.cs.gnscommon.GNSProtocol;
import edu.umass.cs.gnscommon.ResponseCode;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnscommon.exceptions.client.EncryptionException;
import edu.umass.cs.gnscommon.exceptions.client.FieldNotFoundException;
import edu.umass.cs.gnscommon.utils.Base64;
import edu.umass.cs.gnscommon.utils.RandomString;
import edu.umass.cs.gnscommon.utils.ThreadUtils;
import edu.umass.cs.gnsserver.database.MongoRecords;
import edu.umass.cs.gnsserver.main.GNSConfig;
import edu.umass.cs.gnsserver.utils.DefaultGNSTest;
import edu.umass.cs.reconfiguration.ReconfigurableNode;
import edu.umass.cs.reconfiguration.ReconfigurationConfig;
import edu.umass.cs.reconfiguration.reconfigurationutils.DefaultNodeConfig;
import edu.umass.cs.utils.Config;
import edu.umass.cs.utils.Repeat;
import edu.umass.cs.utils.Util;

import edu.umass.cs.utils.Utils;
import java.awt.geom.Point2D;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.security.NoSuchAlgorithmException;
import java.util.Set;

import org.apache.commons.lang3.RandomUtils;
import org.hamcrest.Matchers;
import org.json.JSONException;
import org.junit.Assert;

/**
 * Functionality test for core elements in the client using the
 * GNSClientCommands.
 *
 */
//@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ServerIntegrationTest extends DefaultGNSTest {

  private static final String DEFAULT_ACCOUNT_ALIAS = "support@gns.name";

  private static String accountAlias = DEFAULT_ACCOUNT_ALIAS; // REPLACE //
  // ALIAS
  private static final String PASSWORD = "password";
  private static GNSClientCommands clientCommands = null;
  private static GuidEntry masterGuid;

  private static final int REPEAT = 10;

  /**
   *
   * @param alias
   */
  public static void setAccountAlias(String alias) {
    accountAlias = alias;
  }

  private static final String HOME = System.getProperty("user.home");
  private static final String GNS_DIR = "GNS";
  private static final String GNS_HOME = HOME + "/" + GNS_DIR + "/";

  private static String getPath(String filename) {
    if (new File(filename).exists()) {
      return filename;
    }
    if (new File(GNS_HOME + filename).exists()) {
      return GNS_HOME + filename;
    } else {
      Util.suicide("Can not find server startup script: " + filename);
    }
    return null;
  }

  private static enum DefaultProps {
    SERVER_COMMAND("server.command", USE_GP_SCRIPTS ? GP_SERVER
            : SCRIPTS_SERVER, true),
    GIGAPAXOS_CONFIG("gigapaxosConfig", "conf/gnsserver.3local.unittest.properties",
            true),
    KEYSTORE("javax.net.ssl.keyStore", "conf/keyStore.jks", true),
    KEYSTORE_PASSWORD("javax.net.ssl.keyStorePassword", "qwerty"),
    TRUSTSTORE("javax.net.ssl.trustStore", "conf/trustStore.jks",
            true),
    TRUSTSTORE_PASSWORD("javax.net.ssl.trustStorePassword", "qwerty"),
    LOGGING_PROPERTIES("java.util.logging.config.file",
            "conf/logging.gns.unittest.properties", true),
    START_SERVER("startServer", "true"),;

    final String key;
    final String value;
    final boolean isFile;

    DefaultProps(String key, String value, boolean isFile) {
      this.key = key;
      this.value = value;
      this.isFile = isFile;
    }

    DefaultProps(String key, String value) {
      this(key, value, false);
    }
  }

  private static void setProperties() {
    for (DefaultProps prop : DefaultProps.values()) {
      if (System.getProperty(prop.key) == null) {
        System.setProperty(prop.key, prop.isFile ? getPath(prop.value)
                : prop.value);
      }
    }
  }

  // this static block must be above GP_OPTIONS
  static {
    setProperties();
  }

  private static final String SCRIPTS_SERVER = "scripts/3nodeslocal/reset_and_restart.sh";
  private static final String SCRIPTS_OPTIONS = " "; // can't give any options

  private static final String GP_SERVER = "bin/gpServer.sh";
  private static final String GP_OPTIONS = getGigaPaxosOptions();
  private static final boolean USE_GP_SCRIPTS = true;
  private static String options = USE_GP_SCRIPTS ? GP_OPTIONS
          : SCRIPTS_OPTIONS;

  private static String getGigaPaxosOptions() {
    String gpOptions = "";
    for (DefaultProps prop : DefaultProps.values()) {
      gpOptions += " -D" + prop.key + "=" + System.getProperty(prop.key);
    }
    return gpOptions + " -ea";
  }

  private static boolean useGPScript() {
    return System.getProperty(DefaultProps.SERVER_COMMAND.key).contains(
            GP_SERVER);
  }

  private static void failWithStackTrace(String message, Exception... e) {
    System.out.println("\n--" + RequestInstrumenter.getLog() + "--");
    if (e != null && e.length > 0) {
      e[0].printStackTrace();
    }
    org.junit.Assert.fail(message);
  }

  /* We need this below even though a majority being up suffices and account GNSProtocol.GUID.toString() 
	 * creation success (with retransmission) auto-detects whether a majority is up,
	 * it can happen that one server is not yet ready, which sometimes leads to 
	 * some tests like lookupPrimaryGuid failing because the request goes to a 
	 * still-not-up server and simply times out.
	 * 
	 * The clean way to obviate this wait is to build fault-tolerance into the 
	 * tests, i.e., every request should be retransmitted until success
	 * assuming that any server can fail at any time.
   */
  private static long WAIT_TILL_ALL_SERVERS_READY = 000;

  private static boolean singleJVM() {
    return (System.getProperty("singleJVM") != null && System.getProperty("singleJVM").trim().toLowerCase().equals("true"));

  }

  /**
   * @throws IOException
   * @throws FileNotFoundException
   * @throws InterruptedException
   *
   */
  @BeforeClass
  public static void setUpBeforeClass() throws FileNotFoundException,
          IOException, InterruptedException {
    DefaultGNSTest.setUpBeforeClass();
    masterGuid = GuidUtils.getGUIDKeys(accountAlias = globalAccountName);
    clientCommands = (GNSClientCommands) new GNSClientCommands()
            .setNumRetriesUponTimeout(2).setForceCoordinatedReads(true);
    client = new GNSClient()
            .setNumRetriesUponTimeout(2)
            .setForceCoordinatedReads(true)
            .setForcedTimeout(8000);

  }

  /**
   * @throws FileNotFoundException
   * @throws IOException
   * @throws InterruptedException
   */
  @Deprecated
  public static void setUpBeforeClassOld() throws FileNotFoundException, IOException, InterruptedException {
    /* The waitTillAllServersReady parameter is not needed for
		 * single-machine tests as we check the logs explicitly below. It is
		 * still useful for distributed tests as there is no intentionally
		 * support in gigapaxos' async client to detect if all servrs are up. */
    String waitString = System.getProperty("waitTillAllServersReady");
    if (waitString != null) {
      WAIT_TILL_ALL_SERVERS_READY = Integer.parseInt(waitString);
    }

    // get pattern for log files
    Properties logProps = new Properties();
    logProps.load(new FileInputStream(System.getProperty(DefaultProps.LOGGING_PROPERTIES.key)));
    String logFiles = logProps.getProperty("java.util.logging.FileHandler.pattern");

    if (logFiles != null) {
      logFiles = logFiles.replaceAll("%.*", "").trim() + "*";
    }
    new File(logFiles.replaceFirst("/[^/]*$", "")).mkdirs();

    if (logFiles != null) {
      System.out.print("Deleting log files " + logFiles);
      RunCommand.command("rm -f " + logFiles, ".", false);
      System.out.print(" ...done" + logFiles);
    }

    // start server
    if (System.getProperty("startServer") != null
            && System.getProperty("startServer").equals("true")) {

      // clear explicitly if gigapaxos
      if (useGPScript()) {

        // forceclear
        String forceClearCmd = System
                .getProperty(DefaultProps.SERVER_COMMAND.key)
                + " "
                + getGigaPaxosOptions() + " forceclear all";
        System.out.println(forceClearCmd);
        RunCommand.command(
                forceClearCmd, ".");

        /* We need to do this to limit the number of files used by mongo.
         * Otherwise failed runs quickly lead to more failed runs because
         * index files created in previous runs are not removed.
         */
        dropAllDatabases();

        options = getGigaPaxosOptions() + " restart all";
      } else {
        options = SCRIPTS_OPTIONS;
      }

      String startServerCmd = System
              .getProperty(DefaultProps.SERVER_COMMAND.key)
              + " "
              + options;
      System.out.println(startServerCmd);

      // servers are being started here
      if (singleJVM()) {
        startServersSingleJVM();
      } else {
        ArrayList<String> output = RunCommand.command(startServerCmd, ".");

        if (output != null) {
          for (String line : output) {
            System.out.println(line);
          }
        } else {
          failWithStackTrace("Server command failure: ; aborting all tests.");
        }
      }
    }

    int numServers = PaxosConfig.getActives().size() + ReconfigurationConfig.getReconfigurators().size();

    ArrayList<String> output;
    int numServersUp = 0;
    // a little sleep ensures that there is time for at least one log file to get created
    Thread.sleep(500);
    if (!singleJVM()) {
      do {
        output = RunCommand.command("cat " + logFiles + " | grep -a \"server ready\" | wc -l ", ".", false);
        String temp = output.get(0);
        temp = temp.replaceAll("\\s", "");
        try {
          numServersUp = Integer.parseInt(temp);
        } catch (NumberFormatException e) {
          // can happen if no files have yet gotten created
          System.out.println(e);
        }
        System.out.println(Integer.toString(numServersUp) + " out of " + Integer.toString(numServers) + " servers are ready.");
        Thread.sleep(1000);
      } while (numServersUp < numServers);
    }

    System.out.println("Starting client");

    int numRetries = 2;
    boolean forceCoordinated = true;

    clientCommands = (GNSClientCommands) new GNSClientCommands()
            .setNumRetriesUponTimeout(numRetries)
            .setForceCoordinatedReads(forceCoordinated);

    client = new GNSClient()
            .setNumRetriesUponTimeout(numRetries)
            .setForceCoordinatedReads(forceCoordinated)
            .setForcedTimeout(8000);

    System.out.println("Client created and connected to server.");
    //
    int tries = 5;
    boolean accountCreated = false;

    Thread.sleep(WAIT_TILL_ALL_SERVERS_READY);

    do {
      try {
        System.out.println("Creating account guid: " + (tries - 1)
                + " attempt remaining.");
        String createdGUID = client.execute(GNSCommand.createAccount(accountAlias)).getResultString();
        Assert.assertEquals(createdGUID, GuidUtils.getGUIDKeys(accountAlias).guid);

        // older code; okay to leave it hanging or to remove
        masterGuid = GuidUtils.lookupOrCreateAccountGuid(clientCommands,
                accountAlias, PASSWORD, true);
        accountCreated = true;
      } catch (Exception e) {
        e.printStackTrace();
        ThreadUtils.sleep((5 - tries) * 4000);
      }
    } while (!accountCreated && --tries > 0);
    if (accountCreated == false) {
      failWithStackTrace("Failure setting up account guid; aborting all tests.");
    }

  }

  private static void startServersSingleJVM() throws IOException {
    // all JVM properties should be already set above
    for (String server : ReconfigurationConfig.getReconfiguratorIDs()) {
      ReconfigurableNode.main(new String[]{server, ReconfigurationConfig.CommandArgs.start.toString(), server});
    }
    for (String server : PaxosConfig.getActives().keySet()) {
      ReconfigurableNode.main(new String[]{server, ReconfigurationConfig.CommandArgs.start.toString(), server});
    }
  }

  public static void tearDownAfterClass() throws ClientException, IOException {
    // to allow more time for remove
    client.setForcedTimeout(TIMEOUT * 2);
    DefaultGNSTest.tearDownAfterClass();
  }

  /**
   *
   */
//  @AfterClass
  @Deprecated
  public static void tearDownAfterClassOld() {
    if (clientCommands != null) {
      clientCommands.close();
    }
    System.out.println("--" + RequestInstrumenter.getLog() + "--");
    /* arun: need a more efficient, parallel implementation of removal of
		 * sub-guids, otherwise this times out. */
    //client.accountGuidRemove(masterGuid);
    if (System.getProperty("startServer") != null
            && System.getProperty("startServer").equals("true")) {
      if (singleJVM()) {
        for (String server : PaxosConfig.getActives().keySet()) {
          ReconfigurableNode.forceClear(server);
        }
        for (String server : ReconfigurationConfig.getReconfiguratorIDs()) {
          ReconfigurableNode.forceClear(server);
        }
      } else if (useGPScript()) {
        String stopCmd = System
                .getProperty(DefaultProps.SERVER_COMMAND.key)
                + " "
                + getGigaPaxosOptions() + " stop all";
        System.out
                .print("Stopping all servers in "
                        + System.getProperty(DefaultProps.GIGAPAXOS_CONFIG.key) + " with " + stopCmd);

        try {
          RunCommand.command(stopCmd, ".");
        } catch (Exception e) {
          System.out.println(" failed to stop all servers with [" + stopCmd + "]");
          e.printStackTrace();
          throw e;
        }
        System.out.println(" stopped all servers.");
      } else {
        ArrayList<String> output = RunCommand.command(
                new File(System
                        .getProperty(DefaultProps.SERVER_COMMAND.key))
                .getParent()
                + "/shutdown.sh", ".");
        if (output != null) {
          for (String line : output) {
            System.out.println(line);
          }
        } else {
          System.out.println("SHUTDOWN SERVER COMMAND FAILED!");
        }
      }
    }

    dropAllDatabases();
  }

  private static void dropAllDatabases() {
    for (String server : new DefaultNodeConfig<>(
            PaxosConfig.getActives(),
            ReconfigurationConfig.getReconfigurators()).getNodeIDs()) {
      MongoRecords.dropNodeDatabase(server);
    }
  }

  /**
   * A test that pulls together a bunch of end--to-end tests.
   */
  public ServerIntegrationTest() {

  }

  /**
   * arun: Coordinated operations generally need some settling time before
   * they can be tested at "any" replica. That is, read-your-writes
   * consistency is not ensured if a read following a write happens to go to a
   * different replica. Thus, we either need to wait for a long enough
   * duration and/or retransmit upon failure.
   *
   * I have inserted waitSettle() haphazardly at places. These tests need to
   * be systematically fixed by retrying if the expected answer is not found.
   * Simply using the async client to resend the request should suffice as
   * ReconfigurableAppClientAsync is designed to automatically pick "good"
   * active replicas, i.e., it will forget crashed ones for some time; clear
   * its cache, re-query, and pick randomly upon an active replica error; and
   * pick the replica closest by distance and load otherwise.
   */
  @SuppressWarnings("javadoc")
  private static void waitSettle(long wait) {
    try {
      if (wait > 0) {
        Thread.sleep(wait);
      }
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  /* TODO:
   * Brendan: I've begun checking tests to make sure that logically
   * they should pass every time in a distributed setting.
   * I will be marking the tests I've already checked with //CHECKED FOR VALIDITY
   * in the first line of the test.
   * 
   * For now I'm assuming no loss (if tests are run locally hopefully this won't be a problem)
   * and bounded delays (of less than the timeout).  I am also assuming the tests are executed
   * in order as some do depend on the actions of previous tests.
   * 
   * TODO: Increase the timeout for these test commands so that they almost never fail due to timeout.
   * 
   */
  /**
   * Creates a guid.
   *
   * @throws Exception
   */
  @Test
  @Repeat(times = REPEAT)
  public void test_010_CreateEntity() throws Exception {
    // CHECKED FOR VALIDITY
    String alias = "testGUID" + RandomString.randomString(12);
    String createdGUID = client.execute(GNSCommand.createGUID(masterGuid, alias))
            .getResultString();
    Assert.assertEquals(alias, GuidUtils.getGUIDKeys(alias).entityName);
    Assert.assertEquals(createdGUID, GuidUtils.getGUIDKeys(alias).guid);
    // deprecated client test
    // GuidEntry guidEntry = clientCommands.guidCreate(masterGuid, alias);
    // Assert.assertNotNull(guidEntry);
    // Assert.assertEquals(alias, guidEntry.getEntityName());
  }

  /**
   * @throws Exception
   */
  @Test
  @Repeat(times = REPEAT * 10)
  public void test_001_CreateAndUpdate() throws Exception {
    // CHECKED FOR VALIDITY
    String alias = "testGUID" + RandomString.randomString(12);
    String createdGUID = client.execute(
            GNSCommand.createGUID(masterGuid, alias)).getResultString();
    GuidEntry createdGUIDEntry = GuidUtils.getGUIDKeys(alias);
    String key = "key1", value = "value1";
    client.execute(GNSCommand.update(createdGUID,
            new JSONObject().put(key, value), createdGUIDEntry));
    Assert.assertEquals(value,
            client.execute(GNSCommand.fieldRead(createdGUIDEntry, key)).getResultMap().get(key));
  }

  /**
   * Removes a guid.
   *
   * @throws IOException
   * @throws ClientException
   * @throws NoSuchAlgorithmException
   */
  @Test
  public void test_020_RemoveCreated() throws NoSuchAlgorithmException,
          ClientException, IOException {
    // CHECKED FOR VALIDITY
    String testGuidName = "testGUID" + RandomString.randomString(12);
    GuidEntry testGuid;

    testGuid = clientCommands.guidCreate(masterGuid, testGuidName);
    clientCommands.guidRemove(masterGuid, testGuid.getGuid());

    try {
      clientCommands.lookupGuidRecord(testGuid.getGuid());
      failWithStackTrace("Lookup testGuid should have throw an exception.");
    } catch (ClientException e) {
      // expected
    }
  }

  /**
   * Removes a guid not using an account guid.
   *
   * @throws IOException
   * @throws ClientException
   * @throws NoSuchAlgorithmException
   */
  @Test
  public void test_030_RemoveCreatedSansAccountInfo() throws NoSuchAlgorithmException, ClientException, IOException {
    //CHECKED FOR VALIDITY
    String testGuidName = "testGUID" + RandomString.randomString(12);

    String testGUID = client.execute(GNSCommand.createGUID(masterGuid, testGuidName)).getResultString();
    client.execute(GNSCommand.removeGUID(GuidUtils.getGUIDKeys(testGuidName)));
//    GuidEntry testGuid = clientCommands.guidCreate(masterGuid, testGuidName);
//    clientCommands.guidRemove(testGuid);

    try {
//      clientCommands.lookupGuidRecord(testGuid.getGuid());
      client.execute(GNSCommand.lookupGUID(testGUID));
      failWithStackTrace("Lookup testGuid should have throw an exception.");
    } catch (ClientException e) {
      // expected
    }
  }

  private static final String REMOVE_ACCOUNT_PASSWORD = "removalPassword";

  /**
   * Runs the RemoveAccountWithPassword tests as one independent unit.
   *
   * @throws Exception
   */
  @Test
  @Repeat(times = 0) //Disabled temporarily
  public void test_031_RemoveAccountWithPasswordTest() throws Exception {
    String accountToRemoveWithPassword = RandomString.randomString(12) + "-" + "passwordremovetest@gns.name";
    GuidEntry accountToRemoveGuid = test_035_RemoveAccountWithPasswordCreateAccount(accountToRemoveWithPassword);
    test_036_RemoveAccountWithPasswordCheckAccount(accountToRemoveGuid);
    test_037_RemoveAccountWithPasswordRemoveAccount(accountToRemoveWithPassword);
    test_038_RemoveAccountWithPasswordCheckAccountAfterRemove(accountToRemoveWithPassword);
  }

  /**
   * Create an account to remove using the password.
   *
   * @throws Exception
   */
  @SuppressWarnings("javadoc")
  private GuidEntry test_035_RemoveAccountWithPasswordCreateAccount(String accountToRemoveWithPassword) throws Exception {
    /* FIXED: GuidUtils.lookupOrCreateAccountGuid() is safe 
	 * since the account verification step is coordinated later on in its chain.
	 * TODO: Make sure that gigapaxos guaruntees UPDATE your CREATES for servers with 
	 * greater than 3 replicas.
     */

    client.execute(
            GNSCommand.createAccount(accountToRemoveWithPassword,
                    REMOVE_ACCOUNT_PASSWORD));
    return GuidUtils.getGUIDKeys(accountToRemoveWithPassword);
    //    return GuidUtils.lookupOrCreateAccountGuid(clientCommands, accountToRemoveWithPassword, REMOVE_ACCOUNT_PASSWORD, true);
  }

  /**
   * Check the account to remove using the password.
   *
   * @throws IOException
   * @throws ClientException
   */
  @SuppressWarnings("javadoc")
  private void test_036_RemoveAccountWithPasswordCheckAccount(GuidEntry accountToRemoveGuid) throws ClientException, IOException {
    //CHECKED FOR VALIDITY
    client.execute(GNSCommand.lookupAccountRecord(accountToRemoveGuid.getGuid()));
//    clientCommands.lookupAccountRecord(accountToRemoveGuid.getGuid());
  }

  /**
   * Remove the account using the password.
   *
   * @throws Exception
   */
  @SuppressWarnings("javadoc")
  private void test_037_RemoveAccountWithPasswordRemoveAccount(String accountToRemoveWithPassword) throws Exception {
    //CHECKED FOR VALIDITY
    client.execute(GNSCommand.accountGuidRemoveWithPassword(accountToRemoveWithPassword, REMOVE_ACCOUNT_PASSWORD));
//    clientCommands.accountGuidRemoveWithPassword(accountToRemoveWithPassword, REMOVE_ACCOUNT_PASSWORD);
  }

  /**
   * Check the account removed using the password.
   *
   * @throws IOException
   */
  @SuppressWarnings("javadoc")
  private void test_038_RemoveAccountWithPasswordCheckAccountAfterRemove(String accountToRemoveWithPassword) throws IOException {
    //CHECKED FOR VALIDITY
    try {
      clientCommands.lookupGuid(accountToRemoveWithPassword);
      failWithStackTrace("lookupGuid for " + accountToRemoveWithPassword + " should have throw an exception.");
    } catch (ClientException e) {
      //This exception is expected.
    }
  }

  /**
   * Look up a primary guid.
   *
   * @throws IOException
   * @throws ClientException
   * @throws NoSuchAlgorithmException
   */
  @Test
  public void test_040_LookupPrimaryGuid() throws NoSuchAlgorithmException,
          ClientException, IOException {
    // CHECKED FOR VALIDITY
    String testGuidName = "testGUID" + RandomString.randomString(12);
    GuidEntry testGuid;
    testGuid = clientCommands.guidCreate(masterGuid, testGuidName);

    Assert.assertEquals(masterGuid.getGuid(),
            clientCommands.lookupPrimaryGuid(testGuid.getGuid()));
  }

  /**
   * Runs the Field tests as one independent unit.
   *
   * @throws Exception
   */
  @Test
  public void test_041_FieldTests() throws Exception {
    GuidEntry subGuidEntry = test_050_CreateSubGuid();
    test_060_FieldNotFoundException(subGuidEntry);
    test_070_FieldExistsFalse(subGuidEntry);
    test_080_CreateFieldForFieldExists(subGuidEntry);
    test_090_FieldExistsTrue(subGuidEntry);

  }

  /**
   * Create a sub guid.
   *
   * @throws Exception
   */
  @SuppressWarnings("javadoc")
  private GuidEntry test_050_CreateSubGuid() throws Exception {
    //CHECKED FOR VALIDITY
    GuidEntry subGuidEntry = clientCommands.guidCreate(masterGuid, "subGuid"
            + RandomString.randomString(12));
    System.out.print("Created: " + subGuidEntry);
    return subGuidEntry;
  }

  /**
   * Check field not found exception.
   *
   * @throws Exception
   */
  @SuppressWarnings("javadoc")
  private void test_060_FieldNotFoundException(GuidEntry subGuidEntry) throws Exception {
    //CHECKED FOR VALIDITY
    try {
      clientCommands.fieldReadArrayFirstElement(subGuidEntry.getGuid(),
              "environment", subGuidEntry);
      failWithStackTrace("Should have thrown an exception.");
    } catch (FieldNotFoundException e) {
      //This is expected.
    }
  }

  /**
   * Test fieldExists.
   *
   * @throws Exception
   */
  @SuppressWarnings("javadoc")
  private void test_070_FieldExistsFalse(GuidEntry subGuidEntry) throws Exception {
    //CHECKED FOR VALIDITY
    try {
      Assert.assertFalse(clientCommands.fieldExists(subGuidEntry.getGuid(),
              "environment", subGuidEntry));
    } catch (ClientException e) {
      // System.out.println("This was expected: " , e);
    }
  }

  /**
   * Create a field for fieldExists.
   *
   * @throws IOException
   * @throws ClientException
   */
  @SuppressWarnings("javadoc")
  private void test_080_CreateFieldForFieldExists(GuidEntry subGuidEntry) throws ClientException, IOException {
    //CHECKED FOR VALIDITY
    clientCommands.fieldCreateOneElementList(subGuidEntry.getGuid(),
            "environment", "work", subGuidEntry);
  }

  /**
   * Create a field for fieldExists true.
   *
   * @throws Exception
   */
  @SuppressWarnings("javadoc")
  private void test_090_FieldExistsTrue(GuidEntry subGuidEntry) throws Exception {
    //CHECKED FOR VALIDITY
    Assert.assertTrue(clientCommands.fieldExists(subGuidEntry.getGuid(),
            "environment", subGuidEntry));
  }

  /**
   * Runs the set of ACL tests for checking All Fields Access as an independent unit.
   *
   * @throws Exception
   * @throws JSONException
   */
  @Test
  public void test_100_ACLTest_All_Fields() throws JSONException, Exception {
    final String TEST_FIELD_NAME = "testField";
//    GuidEntry accountGuid = GuidUtils.lookupOrCreateAccountGuid(clientCommands,
//    		RandomString.randomString(6) + "@gns.name", PASSWORD, true);
    String name = RandomString.randomString(6) + "@gns.name";
    client.execute(GNSCommand.createAccount(name, PASSWORD));
    GuidEntry accountGuid = GuidUtils.getGUIDKeys(name);

    String testFieldName = TEST_FIELD_NAME + RandomString.randomString(6);
    test_101_ACLCreateField(accountGuid, testFieldName);
    test_110_ACLMaybeAddAllFields(accountGuid);
    test_111_ACLCheckForAllFieldsPass(accountGuid);
    test_112_ACLRemoveAllFields(accountGuid);
    test_113_ACLCheckForAllFieldsMissing(accountGuid);
    test_114_CheckAllFieldsAcl(accountGuid);
    test_115_DeleteAllFieldsAcl(accountGuid);
    test_116_CheckAllFieldsAclGone(accountGuid);
  }

  // shorthand
  private static void p(String s) {
    System.out.print(s + " ");
  }

  /*
   * @throws IOException
   * @throws ClientException
   *
   */
  private void test_101_ACLCreateField(GuidEntry masterGuid, String testFieldName) throws ClientException, IOException {
    //CHECKED FOR VALIDITY
    //p("test_101_ACLCreateField:" + testFieldName);
    clientCommands.fieldCreateOneElementList(masterGuid.getGuid(), testFieldName, "testValue", masterGuid);
  }

  //
  // Start with some simple tests to insure that basic ACL mechanics work
  //
  /*
   * Add the ALL_GUID to GNSProtocol.ENTIRE_RECORD.toString() if it's not there already.
   *
   * @throws Exception
   * @throws JSONException
   */
  private void test_110_ACLMaybeAddAllFields(GuidEntry masterGuid) throws JSONException, Exception {
    //CHECKED FOR VALIDITY
    p("test_110_ACLMaybeAddAllFields");
    if (!JSONUtils.JSONArrayToArrayList(clientCommands.aclGet(AclAccessType.READ_WHITELIST, masterGuid,
            GNSProtocol.ENTIRE_RECORD.toString(), masterGuid.getGuid()))
            .contains(GNSProtocol.ALL_GUIDS.toString())) {
      clientCommands.aclAdd(AclAccessType.READ_WHITELIST, masterGuid,
              GNSProtocol.ENTIRE_RECORD.toString(),
              GNSProtocol.ALL_GUIDS.toString());
    }
  }

  /*
   * @throws Exception
   * @throws JSONException
   *
   */
  private void test_111_ACLCheckForAllFieldsPass(GuidEntry masterGuid) throws JSONException, Exception {
    //CHECKED FOR VALIDITY
    p("test_111_ACLCheckForAllFieldsPass");
    JSONArray expected = new JSONArray(Arrays.asList(GNSProtocol.ALL_GUIDS.toString()));
    JSONAssert.assertEquals(expected,
            clientCommands.aclGet(AclAccessType.READ_WHITELIST, masterGuid,
                    GNSProtocol.ENTIRE_RECORD.toString(), masterGuid.getGuid()), true);
  }

  private void test_112_ACLRemoveAllFields(GuidEntry masterGuid) throws Exception {
    //CHECKED FOR VALIDITY
    p("test_112_ACLRemoveAllFields");
    // remove default read access for this test
    clientCommands.aclRemove(AclAccessType.READ_WHITELIST, masterGuid,
            GNSProtocol.ENTIRE_RECORD.toString(), GNSProtocol.ALL_GUIDS.toString());
  }

  private void test_113_ACLCheckForAllFieldsMissing(GuidEntry masterGuid) throws JSONException, Exception {
    //CHECKED FOR VALIDITY
    p("test_113_ACLCheckForAllFieldsMissing");
    JSONArray expected = new JSONArray();
    JSONAssert.assertEquals(expected,
            clientCommands.aclGet(AclAccessType.READ_WHITELIST, masterGuid,
                    GNSProtocol.ENTIRE_RECORD.toString(), masterGuid.getGuid()), true);
  }

  private void test_114_CheckAllFieldsAcl(GuidEntry masterGuid) throws Exception {
    //CHECKED FOR VALIDITY
    p("test_114_CheckAllFieldsAcl");
    Assert.assertTrue(clientCommands.fieldAclExists(AclAccessType.READ_WHITELIST, masterGuid,
            GNSProtocol.ENTIRE_RECORD.toString()));
  }

  private void test_115_DeleteAllFieldsAcl(GuidEntry masterGuid) throws Exception {
    //CHECKED FOR VALIDITY
    p("test_115_DeleteAllFieldsAcl");
    clientCommands.fieldDeleteAcl(AclAccessType.READ_WHITELIST, masterGuid, GNSProtocol.ENTIRE_RECORD.toString());
  }

  private void test_116_CheckAllFieldsAclGone(GuidEntry masterGuid) throws Exception {
    //CHECKED FOR VALIDITY
    p("test_116_CheckAllFieldsAclGone");
    Assert.assertFalse(clientCommands.fieldAclExists(AclAccessType.READ_WHITELIST, masterGuid, GNSProtocol.ENTIRE_RECORD.toString()));
  }

  /**
   * Runs the set of ACL tests for checking single field access as an independent unit.
   *
   * @throws Exception
   * @throws JSONException
   */
  @Test
  @Repeat(times = REPEAT * 10)
  public void test_117_ACLTest_Single_Field() throws JSONException, Exception {
    final String TEST_FIELD_NAME = "testField";
    String testFieldName = TEST_FIELD_NAME + RandomString.randomString(6);
    test_101_ACLCreateField(masterGuid, testFieldName);
    test_120_CreateAcl(testFieldName);
    test_121_CheckAcl(testFieldName);
    test_122_DeleteAcl(testFieldName);
    test_123_CheckAclGone(testFieldName);
  }

  private void test_120_CreateAcl(String testFieldName) throws Exception {
    //CHECKED FOR VALIDITY
    clientCommands.fieldCreateAcl(AclAccessType.READ_WHITELIST, masterGuid, testFieldName);
  }

  private void test_121_CheckAcl(String testFieldName) throws Exception {
    //CHECKED FOR VALIDITY
    Assert.assertTrue(clientCommands.fieldAclExists(AclAccessType.READ_WHITELIST, masterGuid, testFieldName));
  }

  private void test_122_DeleteAcl(String testFieldName) throws Exception {
    //CHECKED FOR VALIDITY
    clientCommands.fieldDeleteAcl(AclAccessType.READ_WHITELIST, masterGuid, testFieldName);
  }

  private void test_123_CheckAclGone(String testFieldName) throws Exception {
    //CHECKED FOR VALIDITY
    Assert.assertFalse(clientCommands.fieldAclExists(AclAccessType.READ_WHITELIST, masterGuid, testFieldName));
  }

  /**
   * Runs a set of ACL tests checking if others can read or not ACLS set by another.
   *
   * @throws Exception
   * @throws JSONException
   */
  @Test
  public void test_124_ACLTests_OtherUser() throws JSONException, Exception {
    GuidEntry westyEntry = GuidUtils.lookupOrCreateGuid(clientCommands, masterGuid, "westy124" + RandomString.randomString(6));
    GuidEntry samEntry = GuidUtils.lookupOrCreateGuid(clientCommands, masterGuid, "sam124" + RandomString.randomString(6));
    test_131_ACLRemoveAllFields(westyEntry, samEntry);
    test_132_ACLCreateFields(westyEntry);
    test_135_ACLMaybeAddAllFieldsForMaster(westyEntry);
    test_136_ACLMasterReadAllFields(westyEntry);
    test_137_ACLReadMyFields(westyEntry);
    test_138_ACLNotReadOtherGuidAllFieldsTest(westyEntry, samEntry);
    test_139_ACLNotReadOtherGuidFieldTest(westyEntry, samEntry);
    test_140_AddACLTest(westyEntry, samEntry);
    test_141_CheckACLTest(westyEntry, samEntry);

    test_150_ACLCreateDeeperField(westyEntry);
    test_151_ACLAddDeeperFieldACL(westyEntry);
    test_152_ACLCheckDeeperFieldACLExists(westyEntry);
    test_153_ACLReadDeeperFieldSelf(westyEntry);
    test_154_ACLReadDeeperFieldOtherFail(westyEntry, samEntry);
    test_156_ACLReadShallowFieldOtherFail(westyEntry, samEntry);
    test_157_AddAllRecordACL(westyEntry);
    test_158_ACLReadDeeperFieldOtherFail(westyEntry, samEntry);

  }

  /**
   * Runs a second set of ACL tests checking if others can read or not ACLS set by another.
   *
   * @throws Exception
   * @throws JSONException
   */
  @Test
  public void test_125_ACLTests_OtherUser2() throws JSONException, Exception {
    GuidEntry westyEntry = GuidUtils.lookupOrCreateGuid(clientCommands, masterGuid, "westy125" + RandomString.randomString(6));
    GuidEntry samEntry = GuidUtils.lookupOrCreateGuid(clientCommands, masterGuid, "sam125" + RandomString.randomString(6));
    GuidEntry barneyEntry = GuidUtils.lookupOrCreateGuid(clientCommands, masterGuid, "barney125" + RandomString.randomString(6));

    test_143_ACLAdjustACL(barneyEntry);
    test_144_ACLCreateFields(barneyEntry);
    test_145_ACLUpdateACL(barneyEntry);
    test_146_ACLTestReadsOne(barneyEntry, samEntry);
    test_147_ACLTestReadsTwo(barneyEntry, westyEntry);
    test_148_ACLTestReadsThree(barneyEntry, samEntry);
    test_149_ACLALLFields(barneyEntry);

  }

  /**
   * Runs a set of ACL tests checking nested fields.
   *
   * @throws Exception
   * @throws JSONException
   */
  @Test
  public void test_126_ACLTests_DeeperFields() throws JSONException, Exception {
    GuidEntry westyEntry = GuidUtils.lookupOrCreateGuid(clientCommands, masterGuid, "westy126" + RandomString.randomString(6));
    GuidEntry samEntry = GuidUtils.lookupOrCreateGuid(clientCommands, masterGuid, "sam126" + RandomString.randomString(6));

    test_131_ACLRemoveAllFields(westyEntry, samEntry);

    test_150_ACLCreateDeeperField(westyEntry);
    test_151_ACLAddDeeperFieldACL(westyEntry);
    test_152_ACLCheckDeeperFieldACLExists(westyEntry);
    test_153_ACLReadDeeperFieldSelf(westyEntry);
    test_154_ACLReadDeeperFieldOtherFail(westyEntry, samEntry);
    test_156_ACLReadShallowFieldOtherFail(westyEntry, samEntry);
    test_157_AddAllRecordACL(westyEntry);
    test_158_ACLReadDeeperFieldOtherFail(westyEntry, samEntry);

  }

  /**
   *
   * @param westyEntry
   * @param samEntry
   * @throws Exception
   */
  public void test_131_ACLRemoveAllFields(GuidEntry westyEntry, GuidEntry samEntry) throws Exception {
    //CHECKED FOR VALIDITY
    // remove default read access for this test
    clientCommands.aclRemove(AclAccessType.READ_WHITELIST, westyEntry,
            GNSProtocol.ENTIRE_RECORD.toString(), GNSProtocol.ALL_GUIDS.toString());
    clientCommands.aclRemove(AclAccessType.READ_WHITELIST, samEntry,
            GNSProtocol.ENTIRE_RECORD.toString(), GNSProtocol.ALL_GUIDS.toString());
  }

  /**
   *
   * @param westyEntry
   * @throws IOException
   * @throws ClientException
   */
  public void test_132_ACLCreateFields(GuidEntry westyEntry) throws ClientException, IOException {
    //CHECKED FOR VALIDITY
    clientCommands.fieldUpdate(westyEntry.getGuid(), "environment", "work", westyEntry);
    clientCommands.fieldUpdate(westyEntry.getGuid(), "ssn", "000-00-0000", westyEntry);
    clientCommands.fieldUpdate(westyEntry.getGuid(), "password", "666flapJack", westyEntry);
    clientCommands.fieldUpdate(westyEntry.getGuid(), "address", "100 Hinkledinkle Drive", westyEntry);
  }

  /**
   *
   * @param westyEntry
   * @throws IOException
   * @throws JSONException
   * @throws ClientException
   */
  public void test_135_ACLMaybeAddAllFieldsForMaster(GuidEntry westyEntry)
          throws ClientException, JSONException, IOException {
    // CHECKED FOR VALIDITY
    if (!JSONUtils.JSONArrayToArrayList(
            clientCommands.aclGet(AclAccessType.READ_WHITELIST, westyEntry,
                    GNSProtocol.ENTIRE_RECORD.toString(),
                    westyEntry.getGuid())).contains(masterGuid.getGuid())) {
      clientCommands.aclAdd(AclAccessType.READ_WHITELIST, westyEntry,
              GNSProtocol.ENTIRE_RECORD.toString(), masterGuid.getGuid());
    }
  }

  /**
   *
   * @param westyEntry
   */
  public void test_136_ACLMasterReadAllFields(GuidEntry westyEntry) {
    //CHECKED FOR VALIDITY
    try {
      JSONObject expected = new JSONObject();
      expected.put("environment", "work");
      expected.put("password", "666flapJack");
      expected.put("ssn", "000-00-0000");
      expected.put("address", "100 Hinkledinkle Drive");
      JSONObject actual = new JSONObject(clientCommands.fieldRead(westyEntry.getGuid(),
              GNSProtocol.ENTIRE_RECORD.toString(), masterGuid));
      JSONAssert.assertEquals(expected, actual, true);
    } catch (Exception e) {
      failWithStackTrace("Exception while reading all fields in ACLReadAllFields: ", e);
    }
  }

  /**
   *
   * @param westyEntry
   */
  public void test_137_ACLReadMyFields(GuidEntry westyEntry) {
    //CHECKED FOR VALIDITY
    try {
      // read my own field
      Assert.assertEquals("work",
              clientCommands.fieldRead(westyEntry.getGuid(), "environment", westyEntry));
      // read another one of my fields field
      Assert.assertEquals("000-00-0000",
              clientCommands.fieldRead(westyEntry.getGuid(), "ssn", westyEntry));

    } catch (Exception e) {
      failWithStackTrace("Exception while reading fields in ACLReadMyFields: ", e);
    }
  }

  /**
   *
   * @param westyEntry
   * @param samEntry
   */
  public void test_138_ACLNotReadOtherGuidAllFieldsTest(GuidEntry westyEntry, GuidEntry samEntry) {
    //CHECKED FOR VALIDITY
    try {
      try {
        String result = clientCommands.fieldRead(westyEntry.getGuid(), GNSProtocol.ENTIRE_RECORD.toString(), samEntry);
        failWithStackTrace("Result of read of all of westy's fields by sam is " + result
                + " which is wrong because it should have been rejected.");
      } catch (ClientException e) {
      }
    } catch (Exception e) {
      failWithStackTrace("Exception while reading fields in ACLNotReadOtherGuidAllFieldsTest: ", e);
    }
  }

  /**
   *
   * @param westyEntry
   * @param samEntry
   */
  public void test_139_ACLNotReadOtherGuidFieldTest(GuidEntry westyEntry, GuidEntry samEntry) {
    //CHECKED FOR VALIDITY
    try {
      try {
        String result = clientCommands.fieldRead(westyEntry.getGuid(), "environment",
                samEntry);
        failWithStackTrace("Result of read of westy's environment by sam is " + result
                + " which is wrong because it should have been rejected.");
      } catch (ClientException e) {
      }
    } catch (Exception e) {
      failWithStackTrace("Exception while reading fields in ACLNotReadOtherGuidFieldTest: ", e);

    }
  }

  /**
   *
   * @param westyEntry
   * @param samEntry
   */
  public void test_140_AddACLTest(GuidEntry westyEntry, GuidEntry samEntry) {
    //CHECKED FOR VALIDITY
    try {
      try {
        clientCommands.aclAdd(AclAccessType.READ_WHITELIST, westyEntry, "environment", samEntry.getGuid());
      } catch (Exception e) {
        failWithStackTrace("Exception adding Sam to Westy's readlist: ", e);
      }
    } catch (Exception e) {
      failWithStackTrace("Exception when we were not expecting it in ACLPartOne: ", e);
    }
  }

  /**
   *
   * @param westyEntry
   * @param samEntry
   */
  public void test_141_CheckACLTest(GuidEntry westyEntry, GuidEntry samEntry) {
    //CHECKED FOR VALIDITY
    try {
      try {
        Assert.assertEquals("work", clientCommands.fieldRead(westyEntry.getGuid(), "environment", samEntry));
      } catch (Exception e) {
        failWithStackTrace("Exception while Sam reading Westy's field: ", e);
      }
    } catch (Exception e) {
      failWithStackTrace("Exception when we were not expecting it in ACLPartOne: ", e);
    }
  }

  /**
   *
   * @param barneyEntry
   */
  public void test_143_ACLAdjustACL(GuidEntry barneyEntry) {
    //CHECKED FOR VALIDITY
    try {
      // remove default read access for this test
      clientCommands.aclRemove(AclAccessType.READ_WHITELIST, barneyEntry,
              GNSProtocol.ENTIRE_RECORD.toString(), GNSProtocol.ALL_GUIDS.toString());
    } catch (Exception e) {
      failWithStackTrace("Exception when we were not expecting it in ACLPartTwo: ", e);
    }
  }

  /**
   *
   * @param barneyEntry
   */
  public void test_144_ACLCreateFields(GuidEntry barneyEntry) {
    //CHECKED FOR VALIDITY
    try {
      // remove default read access for this test
      clientCommands.fieldUpdate(barneyEntry.getGuid(), "cell", "413-555-1234", barneyEntry);
      clientCommands.fieldUpdate(barneyEntry.getGuid(), "address", "100 Main Street", barneyEntry);
    } catch (IOException | ClientException e) {
      failWithStackTrace("Exception when we were not expecting it in ACLPartTwo: ", e);
    }
  }

  /**
   *
   * @param barneyEntry
   */
  public void test_145_ACLUpdateACL(GuidEntry barneyEntry) {
    //CHECKED FOR VALIDITY
    try {
      try {
        // let anybody read barney's cell field
        clientCommands.aclAdd(AclAccessType.READ_WHITELIST, barneyEntry, "cell",
                GNSProtocol.ALL_GUIDS.toString());
      } catch (Exception e) {
        failWithStackTrace("Exception creating ALL_GUIDS access for Barney's cell: ", e);
      }
    } catch (Exception e) {
      failWithStackTrace("Exception when we were not expecting it in ACLPartTwo: ", e);
    }
  }

  /**
   *
   * @param barneyEntry
   * @param samEntry
   */
  public void test_146_ACLTestReadsOne(GuidEntry barneyEntry, GuidEntry samEntry) {
    //CHECKED FOR VALIDITY
    try {
      try {
        Assert.assertEquals("413-555-1234",
                clientCommands.fieldRead(barneyEntry.getGuid(), "cell", samEntry));
      } catch (Exception e) {
        failWithStackTrace("Exception while Sam reading Barney' cell: ", e);
      }
    } catch (Exception e) {
      failWithStackTrace("Exception when we were not expecting it in ACLPartOne: ", e);
    }
  }

  /**
   *
   * @param barneyEntry
   * @param westyEntry
   */
  public void test_147_ACLTestReadsTwo(GuidEntry barneyEntry, GuidEntry westyEntry) {
    //CHECKED FOR VALIDITY
    try {
      try {
        Assert.assertEquals("413-555-1234",
                clientCommands.fieldRead(barneyEntry.getGuid(), "cell", westyEntry));
      } catch (Exception e) {
        failWithStackTrace("Exception while Westy reading Barney' cell: ", e);
      }
    } catch (Exception e) {
      failWithStackTrace("Exception when we were not expecting it in ACLTestReadsTwo: ", e);
    }
  }

  /**
   *
   * @param barneyEntry
   * @param samEntry
   */
  public void test_148_ACLTestReadsThree(GuidEntry barneyEntry, GuidEntry samEntry) {
    //CHECKED FOR VALIDITY
    try {
      try {
        String result = clientCommands.fieldRead(barneyEntry.getGuid(), "address",
                samEntry);
        failWithStackTrace("Result of read of barney's address by sam is " + result
                + " which is wrong because it should have been rejected.");
      } catch (ClientException e) {
        if (e.getCode() == ResponseCode.ACCESS_ERROR) {
          System.out.print("This was expected for null querier trying to ReadUnsigned "
                  + barneyEntry.getGuid()
                  + "'s address: "
                  + e);
        }
      } catch (Exception e) {
        failWithStackTrace("Exception while Sam reading Barney' address: ", e);
      }

    } catch (Exception e) {
      failWithStackTrace("Exception when we were not expecting it in ACLTestReadsThree: ", e);
    }
  }

  /**
   *
   * @param barneyEntry
   */
  public void test_149_ACLALLFields(GuidEntry barneyEntry) {
    //CHECKED FOR VALIDITY
    String superUserName = "superuser" + RandomString.randomString(6);
    try {
      try {
        clientCommands.lookupGuid(superUserName);
        failWithStackTrace(superUserName + " entity should not exist");
      } catch (ClientException e) {
      }

      GuidEntry superuserEntry = clientCommands.guidCreate(masterGuid, superUserName);

      // let superuser read any of barney's fields
      clientCommands.aclAdd(AclAccessType.READ_WHITELIST, barneyEntry,
              GNSProtocol.ENTIRE_RECORD.toString(), superuserEntry.getGuid());

      Assert.assertEquals("413-555-1234",
              clientCommands.fieldRead(barneyEntry.getGuid(), "cell", superuserEntry));
      Assert.assertEquals("100 Main Street",
              clientCommands.fieldRead(barneyEntry.getGuid(), "address", superuserEntry));

    } catch (Exception e) {
      failWithStackTrace("Exception when we were not expecting it in ACLALLFields: ", e);
    }
  }

  /**
   *
   * @param westyEntry
   */
  public void test_150_ACLCreateDeeperField(GuidEntry westyEntry) {
    //CHECKED FOR VALIDITY
    try {
      try {
        clientCommands.fieldUpdate(westyEntry.getGuid(), "test.deeper.field", "fieldValue", westyEntry);
      } catch (IOException | ClientException e) {
        failWithStackTrace("Problem updating field: ", e);
      }
    } catch (Exception e) {
      failWithStackTrace("Exception when we were not expecting it ACLCreateDeeperField: ", e);
    }
  }

  /**
   *
   * @param westyEntry
   */
  public void test_151_ACLAddDeeperFieldACL(GuidEntry westyEntry) {
    //CHECKED FOR VALIDITY
    try {
      try {
        // Create an empty ACL, effectively disabling access except by the guid itself.
        clientCommands.fieldCreateAcl(AclAccessType.READ_WHITELIST, westyEntry, "test.deeper.field");
      } catch (Exception e) {
        failWithStackTrace("Problem adding acl: ", e);
      }
    } catch (Exception e) {
      failWithStackTrace("Exception when we were not expecting it ACLCreateDeeperField: ", e);
    }
  }

  /**
   *
   * @param westyEntry
   */
  public void test_152_ACLCheckDeeperFieldACLExists(GuidEntry westyEntry) {
    //CHECKED FOR VALIDITY
    try {
      try {
        Assert.assertTrue(clientCommands.fieldAclExists(AclAccessType.READ_WHITELIST, westyEntry, "test.deeper.field"));
      } catch (Exception e) {
        failWithStackTrace("Problem reading acl: ", e);
      }
    } catch (Exception e) {
      failWithStackTrace("Exception when we were not expecting it ACLCreateDeeperField: ", e);
    }
  }

  // This should pass even though the ACL for test.deeper.field is empty because you
  // can always read your own fields.
  /**
   *
   * @param westyEntry
   */
  public void test_153_ACLReadDeeperFieldSelf(GuidEntry westyEntry) {
    //CHECKED FOR VALIDITY
    try {
      try {
        Assert.assertEquals("fieldValue", clientCommands.fieldRead(westyEntry.getGuid(), "test.deeper.field", westyEntry));
      } catch (Exception e) {
        failWithStackTrace("Problem adding read field: ", e);
      }
    } catch (Exception e) {
      failWithStackTrace("Exception when we were not expecting it ACLCreateDeeperField: ", e);
    }
  }

  // This should fail because the ACL for test.deeper.field is empty.
  /**
   *
   * @param westyEntry
   * @param samEntry
   */
  public void test_154_ACLReadDeeperFieldOtherFail(GuidEntry westyEntry, GuidEntry samEntry) {
    //CHECKED FOR VALIDITY
    try {
      try {
        Assert.assertEquals("fieldValue", clientCommands.fieldRead(westyEntry.getGuid(), "test.deeper.field", samEntry));
        failWithStackTrace("This read should have failed.");
      } catch (Exception e) {
      }
    } catch (Exception e) {
      failWithStackTrace("Exception when we were not expecting it ACLCreateDeeperField: ", e);
    }
  }

  // This should fail because the ACL for test.deeper.field is empty.
  /**
   *
   * @param westyEntry
   * @param samEntry
   */
  public void test_156_ACLReadShallowFieldOtherFail(GuidEntry westyEntry, GuidEntry samEntry) {
    //CHECKED FOR VALIDITY
    try {
      try {
        Assert.assertEquals("fieldValue", clientCommands.fieldRead(westyEntry.getGuid(), "test.deeper", samEntry));
        failWithStackTrace("This read should have failed.");
      } catch (Exception e) {
      }
    } catch (Exception e) {
      failWithStackTrace("Exception when we were not expecting it ACLCreateDeeperField: ", e);
    }
  }

  /**
   *
   * @param westyEntry
   */
  public void test_157_AddAllRecordACL(GuidEntry westyEntry) {
    //CHECKED FOR VALIDITY
    try {
      clientCommands.aclAdd(AclAccessType.READ_WHITELIST, westyEntry, "test", GNSProtocol.ALL_GUIDS.toString());
    } catch (Exception e) {
      failWithStackTrace("Exception when we were not expecting it ACLCreateDeeperField: ", e);
    }
  }

  // This should still fail because the ACL for test.deeper.field is empty even though test 
  // now has an GNSProtocol.ALL_GUIDS.toString() at the root (this is different than the old model).
  /**
   *
   * @param westyEntry
   * @param samEntry
   */
  public void test_158_ACLReadDeeperFieldOtherFail(GuidEntry westyEntry, GuidEntry samEntry) {
    //CHECKED FOR VALIDITY
    try {
      try {
        Assert.assertEquals("fieldValue", clientCommands.fieldRead(westyEntry.getGuid(), "test.deeper.field", samEntry));
        failWithStackTrace("This read should have failed.");
      } catch (Exception e) {
      }
    } catch (Exception e) {
      failWithStackTrace("Exception when we were not expecting it ACLCreateDeeperField: ", e);
    }
  }

  /**
   * Runs a set of DB tests.
   *
   * @throws Exception
   * @throws JSONException
   */
  @Test
  public void test_160_DBTests() throws JSONException, Exception {
    GuidEntry westyEntry = GuidUtils.lookupOrCreateGuid(clientCommands, masterGuid, "westy160" + RandomString.randomString(6));

    test_170_DB(westyEntry);
    test_180_DBUpserts(westyEntry);

  }

  /**
   * Tests a bunch of different access methods.
   *
   * @param westyEntry
   */
  public void test_170_DB(GuidEntry westyEntry) {
    //CHECKED FOR VALIDITY
    try {

      clientCommands.fieldCreateOneElementList(westyEntry.getGuid(), "cats",
              "whacky", westyEntry);

      Assert.assertEquals("whacky", clientCommands.fieldReadArrayFirstElement(
              westyEntry.getGuid(), "cats", westyEntry));

      clientCommands.fieldAppendWithSetSemantics(
              westyEntry.getGuid(),
              "cats",
              new JSONArray(Arrays.asList("hooch", "maya", "red", "sox",
                      "toby")), westyEntry);

      HashSet<String> expected = new HashSet<>(Arrays.asList(
              "hooch", "maya", "red", "sox", "toby", "whacky"));
      HashSet<String> actual = JSONUtils.JSONArrayToHashSet(clientCommands
              .fieldReadArray(westyEntry.getGuid(), "cats", westyEntry));
      Assert.assertEquals(expected, actual);

      clientCommands.fieldClear(westyEntry.getGuid(), "cats", new JSONArray(
              Arrays.asList("maya", "toby")), westyEntry);

      expected = new HashSet<>(Arrays.asList("hooch", "red", "sox",
              "whacky"));
      actual = JSONUtils.JSONArrayToHashSet(clientCommands.fieldReadArray(
              westyEntry.getGuid(), "cats", westyEntry));
      Assert.assertEquals(expected, actual);

      clientCommands.fieldReplaceFirstElement(westyEntry.getGuid(), "cats",
              "maya", westyEntry);

      Assert.assertEquals("maya", clientCommands.fieldReadArrayFirstElement(
              westyEntry.getGuid(), "cats", westyEntry));
      clientCommands.fieldAppendWithSetSemantics(westyEntry.getGuid(), "cats",
              "fred", westyEntry);

      expected = new HashSet<>(Arrays.asList("maya", "fred"));
      actual = JSONUtils.JSONArrayToHashSet(clientCommands.fieldReadArray(
              westyEntry.getGuid(), "cats", westyEntry));
      Assert.assertEquals(expected, actual);

      clientCommands.fieldAppendWithSetSemantics(westyEntry.getGuid(), "cats",
              "fred", westyEntry);

      expected = new HashSet<>(Arrays.asList("maya", "fred"));
      actual = JSONUtils.JSONArrayToHashSet(clientCommands.fieldReadArray(
              westyEntry.getGuid(), "cats", westyEntry));
      Assert.assertEquals(expected, actual);
    } catch (Exception e) {
      failWithStackTrace("Exception when we were not expecting testing DB: ", e);
    }
  }

  /**
   * Tests a bunch of different DB upsert methods.
   *
   * @param westyEntry
   */
  public void test_180_DBUpserts(GuidEntry westyEntry) {
    //CHECKED FOR VALIDITY
    HashSet<String> expected;
    HashSet<String> actual;
    try {

      clientCommands.fieldAppendOrCreate(westyEntry.getGuid(), "dogs", "bear",
              westyEntry);

      expected = new HashSet<>(Arrays.asList("bear"));
      actual = JSONUtils.JSONArrayToHashSet(clientCommands.fieldReadArray(
              westyEntry.getGuid(), "dogs", westyEntry));
      Assert.assertEquals(expected, actual);

    } catch (Exception e) {
      failWithStackTrace("1) Looking for bear: ", e);
    }
    try {
      clientCommands.fieldAppendOrCreateList(westyEntry.getGuid(), "dogs",
              new JSONArray(Arrays.asList("wags", "tucker")), westyEntry);

      expected = new HashSet<>(Arrays.asList("bear", "wags",
              "tucker"));
      actual = JSONUtils.JSONArrayToHashSet(clientCommands.fieldReadArray(
              westyEntry.getGuid(), "dogs", westyEntry));
      Assert.assertEquals(expected, actual);
    } catch (Exception e) {
      failWithStackTrace("2) Looking for bear, wags, tucker: ", e);
    }
    try {
      clientCommands.fieldReplaceOrCreate(westyEntry.getGuid(), "goats", "sue",
              westyEntry);

      expected = new HashSet<>(Arrays.asList("sue"));
      actual = JSONUtils.JSONArrayToHashSet(clientCommands.fieldReadArray(
              westyEntry.getGuid(), "goats", westyEntry));
      Assert.assertEquals(expected, actual);
    } catch (Exception e) {
      failWithStackTrace("3) Looking for sue: ", e);
    }
    try {
      clientCommands.fieldReplaceOrCreate(westyEntry.getGuid(), "goats",
              "william", westyEntry);

      expected = new HashSet<>(Arrays.asList("william"));
      actual = JSONUtils.JSONArrayToHashSet(clientCommands.fieldReadArray(
              westyEntry.getGuid(), "goats", westyEntry));
      Assert.assertEquals(expected, actual);
    } catch (Exception e) {
      failWithStackTrace("4) Looking for william: ", e);
    }
    try {
      clientCommands.fieldReplaceOrCreateList(westyEntry.getGuid(), "goats",
              new JSONArray(Arrays.asList("dink", "tink")), westyEntry);

      expected = new HashSet<>(Arrays.asList("dink", "tink"));
      actual = JSONUtils.JSONArrayToHashSet(clientCommands.fieldReadArray(
              westyEntry.getGuid(), "goats", westyEntry));
      Assert.assertEquals(expected, actual);
    } catch (Exception e) {
      failWithStackTrace("5) Looking for dink, tink: ", e);
    }
  }

  /**
   * Tests different DB substitute methods.
   */
  @Test
  public void test_190_Substitute() {
    //CHECKED FOR VALIDITY
    String testSubstituteGuid = "testSubstituteGUID"
            + RandomString.randomString(12);
    String field = "people";
    GuidEntry testEntry = null;
    try {
      // Utils.clearTestGuids(client);
      // System.out.println("cleared old GUIDs");
      testEntry = clientCommands.guidCreate(masterGuid, testSubstituteGuid);
    } catch (Exception e) {
      failWithStackTrace("Exception during init: ", e);
    }
    try {
      clientCommands.fieldAppendOrCreateList(
              testEntry.getGuid(),
              field,
              new JSONArray(Arrays
                      .asList("Frank", "Joe", "Sally", "Rita")),
              testEntry);
    } catch (IOException | ClientException e) {
      failWithStackTrace("Exception during create: ", e);
    }

    try {
      HashSet<String> expected = new HashSet<>(Arrays.asList(
              "Frank", "Joe", "Sally", "Rita"));
      HashSet<String> actual = JSONUtils.JSONArrayToHashSet(clientCommands
              .fieldReadArray(testEntry.getGuid(), field, testEntry));
      Assert.assertEquals(expected, actual);
    } catch (Exception e) {
      failWithStackTrace("Exception when we were not expecting it: ", e);
    }

    try {
      clientCommands.fieldSubstitute(testEntry.getGuid(), field, "Christy",
              "Sally", testEntry);
    } catch (Exception e) {
      failWithStackTrace("Exception during substitute: ", e);
    }

    try {
      HashSet<String> expected = new HashSet<>(Arrays.asList(
              "Frank", "Joe", "Christy", "Rita"));
      HashSet<String> actual = JSONUtils.JSONArrayToHashSet(clientCommands
              .fieldReadArray(testEntry.getGuid(), field, testEntry));
      Assert.assertEquals(expected, actual);
    } catch (Exception e) {
      failWithStackTrace("Exception when we were not expecting it: ", e);
    }
  }

  /**
   * Tests different DB substitute list methods.
   */
  @Test
  @Repeat(times = REPEAT)
  public void test_200_SubstituteList() {
    //CHECKED FOR VALIDITY
    String testSubstituteListGuid = "testSubstituteListGUID"
            + RandomString.randomString(12);
    String field = "people";
    GuidEntry testEntry = null;
    try {
      // Utils.clearTestGuids(client);
      // System.out.println("cleared old GUIDs");
      testEntry = clientCommands.guidCreate(masterGuid, testSubstituteListGuid);
      System.out.print(testEntry + " ");
    } catch (Exception e) {
      failWithStackTrace("Exception during init: ", e);
    }
    try {
      clientCommands.fieldAppendOrCreateList(
              testEntry.getGuid(),
              field,
              new JSONArray(Arrays
                      .asList("Frank", "Joe", "Sally", "Rita")),
              testEntry);
    } catch (IOException | ClientException e) {
      failWithStackTrace("Exception during create: ", e);
    }

    try {
      HashSet<String> expected = new HashSet<>(Arrays.asList(
              "Frank", "Joe", "Sally", "Rita"));
      HashSet<String> actual = JSONUtils.JSONArrayToHashSet(clientCommands
              .fieldReadArray(testEntry.getGuid(), field, testEntry));
      Assert.assertEquals(expected, actual);
    } catch (Exception e) {
      failWithStackTrace("Exception when we were not expecting it: ", e);
    }

    try {
      clientCommands.fieldSubstitute(testEntry.getGuid(), field, new JSONArray(
              Arrays.asList("BillyBob", "Hank")),
              new JSONArray(Arrays.asList("Frank", "Joe")), testEntry);
    } catch (IOException | ClientException e) {
      failWithStackTrace("Exception during substitute: ", e);
    }

    try {
      HashSet<String> expected = new HashSet<>(Arrays.asList(
              "BillyBob", "Hank", "Sally", "Rita"));
      HashSet<String> actual = JSONUtils.JSONArrayToHashSet(clientCommands
              .fieldReadArray(testEntry.getGuid(), field, testEntry));
      Assert.assertEquals(expected, actual);
    } catch (Exception e) {
      failWithStackTrace("Exception when we were not expecting it: ", e);
    }
  }

  /**
   * A set of tests for checking Group functionality.
   *
   * @throws Exception
   */
  @Test
  public void test_210_GroupTests() throws Exception {
    GuidEntry westyEntry = GuidUtils.lookupOrCreateGuid(clientCommands, masterGuid, "westy210" + RandomString.randomString(6));
    GuidEntry samEntry = GuidUtils.lookupOrCreateGuid(clientCommands, masterGuid, "sam210" + RandomString.randomString(6));

    List<GuidEntry> entries = test_210_GroupCreate();
    GuidEntry guidToDeleteEntry = entries.get(0);
    GuidEntry mygroupEntry = entries.get(1);
    test_211_GroupAdd(westyEntry, samEntry, mygroupEntry, guidToDeleteEntry);
    test_212_GroupRemoveGuid(guidToDeleteEntry, mygroupEntry);

    GuidEntry groupAccessUserEntry = test_220_GroupAndACLCreateGuids(mygroupEntry);
    test_221_GroupAndACLTestBadAccess(groupAccessUserEntry, westyEntry);
    test_222_GroupAndACLTestGoodAccess(groupAccessUserEntry, westyEntry);
    test_223_GroupAndACLTestRemoveGuid(westyEntry, mygroupEntry);
    test_224_GroupAndACLTestRemoveGuidCheck(westyEntry, mygroupEntry);
  }

  /**
   * Create a group to test.
   *
   * @return a list of GuidEntry
   * @throws Exception
   */
  public List<GuidEntry> test_210_GroupCreate() throws Exception {
    //CHECKED FOR VALIDITY
    String mygroupName = "mygroup" + RandomString.randomString(12);
    try {
      clientCommands.lookupGuid(mygroupName);
      failWithStackTrace(mygroupName + " entity should not exist");
    } catch (ClientException e) {
      //Expected
    }
    GuidEntry guidToDeleteEntry = clientCommands.guidCreate(masterGuid, "deleteMe" + RandomString.randomString(12));
    GuidEntry mygroupEntry = clientCommands.guidCreate(masterGuid, mygroupName);

    List<GuidEntry> entries = new ArrayList<>();
    entries.add(guidToDeleteEntry);
    entries.add(mygroupEntry);
    return entries;
  }

  /**
   * Add guids to a group.
   *
   * @param westyEntry
   * @param samEntry
   * @param mygroupEntry
   * @param guidToDeleteEntry
   */
  public void test_211_GroupAdd(GuidEntry westyEntry, GuidEntry samEntry, GuidEntry mygroupEntry, GuidEntry guidToDeleteEntry) {
    //CHECKED FOR VALIDITY
    try {
      clientCommands.groupAddGuid(mygroupEntry.getGuid(), westyEntry.getGuid(), mygroupEntry);
      clientCommands.groupAddGuid(mygroupEntry.getGuid(), samEntry.getGuid(), mygroupEntry);
      clientCommands.groupAddGuid(mygroupEntry.getGuid(), guidToDeleteEntry.getGuid(), mygroupEntry);
    } catch (IOException | ClientException e) {
      failWithStackTrace("Exception while adding to groups: ", e);
    }
    try {
      HashSet<String> expected = new HashSet<>(Arrays.asList(
              westyEntry.getGuid(), samEntry.getGuid(),
              guidToDeleteEntry.getGuid()));
      HashSet<String> actual = JSONUtils.JSONArrayToHashSet(clientCommands
              .groupGetMembers(mygroupEntry.getGuid(), mygroupEntry));
      Assert.assertEquals(expected, actual);

      expected = new HashSet<>(
              Arrays.asList(mygroupEntry.getGuid()));
      actual = JSONUtils.JSONArrayToHashSet(clientCommands.guidGetGroups(
              westyEntry.getGuid(), westyEntry));
      Assert.assertEquals(expected, actual);

    } catch (IOException | ClientException | JSONException e) {
      failWithStackTrace("Exception while getting members and groups: ", e);
    }
  }

  /**
   * Remove a guid from a group.
   *
   * @param guidToDeleteEntry
   * @param mygroupEntry
   */
  public void test_212_GroupRemoveGuid(GuidEntry guidToDeleteEntry, GuidEntry mygroupEntry) {
    //CHECKED FOR VALIDITY
    // now remove a guid and check for group updates
    try {
      clientCommands.guidRemove(masterGuid, guidToDeleteEntry.getGuid());
    } catch (ClientException | IOException e) {
      failWithStackTrace("Exception while removing testGuid: ", e);
    }
    try {
      clientCommands.lookupGuidRecord(guidToDeleteEntry.getGuid());
      failWithStackTrace("Lookup testGuid should have throw an exception.");
    } catch (ClientException e) {

    } catch (IOException e) {
      failWithStackTrace("Exception while doing Lookup testGuid: ", e);
    }
    try {
      HashSet<String> actual = JSONUtils.JSONArrayToHashSet(
              clientCommands.groupGetMembers(mygroupEntry.getGuid(), mygroupEntry));
      Assert.assertThat(actual, Matchers.not(Matchers.hasItem(guidToDeleteEntry.getGuid())));

    } catch (ClientException | IOException | JSONException e) {
      Utils.failWithStackTrace("Exception during remove guid group update test: " + e);
    }
  }

  /**
   * Update some ACLs for group tests.
   *
   * @param mygroupEntry
   * @return a GuidEntry
   * @throws Exception
   */
  public GuidEntry test_220_GroupAndACLCreateGuids(GuidEntry mygroupEntry) throws Exception {
    //CHECKED FOR VALIDITY
    // testGroup();
    String groupAccessUserName = "groupAccessUser"
            + RandomString.randomString(12);
    GuidEntry groupAccessUserEntry;
    try {
      clientCommands.lookupGuid(groupAccessUserName);
      failWithStackTrace(groupAccessUserName + " entity should not exist");
    } catch (ClientException e) {
      //Expected
    }
    groupAccessUserEntry = clientCommands.guidCreate(masterGuid,
            groupAccessUserName);
    // remove all fields read by all
    clientCommands.aclRemove(AclAccessType.READ_WHITELIST,
            groupAccessUserEntry, GNSProtocol.ENTIRE_RECORD.toString(),
            GNSProtocol.ALL_GUIDS.toString());

    // test of remove all fields read by all
    JSONAssert.assertEquals(new JSONArray(Arrays.asList(masterGuid.getGuid())),
            clientCommands.aclGet(AclAccessType.READ_WHITELIST, groupAccessUserEntry,
                    GNSProtocol.ENTIRE_RECORD.toString(), groupAccessUserEntry.getGuid()),
            JSONCompareMode.STRICT);

    clientCommands.fieldCreateOneElementList(groupAccessUserEntry.getGuid(),
            "address", "23 Jumper Road", groupAccessUserEntry);
    clientCommands.fieldCreateOneElementList(groupAccessUserEntry.getGuid(),
            "age", "43", groupAccessUserEntry);
    clientCommands.fieldCreateOneElementList(groupAccessUserEntry.getGuid(),
            "hometown", "whoville", groupAccessUserEntry);

    clientCommands.aclAdd(AclAccessType.READ_WHITELIST, groupAccessUserEntry,
            "hometown", mygroupEntry.getGuid());

    return groupAccessUserEntry;
  }

  /**
   * Test a group access that should be denied.
   *
   * @param groupAccessUserEntry
   * @param westyEntry
   */
  public void test_221_GroupAndACLTestBadAccess(GuidEntry groupAccessUserEntry, GuidEntry westyEntry) {
    //CHECKED FOR VALIDITY
    try {
      try {
        String result = clientCommands.fieldReadArrayFirstElement(
                groupAccessUserEntry.getGuid(), "address", westyEntry);
        failWithStackTrace("Result of read of groupAccessUser's age by sam is "
                + result
                + " which is wrong because it should have been rejected.");
      } catch (ClientException e) {
      }
    } catch (Exception e) {
      failWithStackTrace("Exception while attempting a failing read of groupAccessUser's age by sam: ",
              e);
    }
  }

  /**
   * Test an group access that should be allowed.
   *
   * @param groupAccessUserEntry
   * @param westyEntry
   */
  public void test_222_GroupAndACLTestGoodAccess(GuidEntry groupAccessUserEntry, GuidEntry westyEntry) {
    //CHECKED FOR VALIDITY
    try {
      Assert.assertEquals("whoville", clientCommands.fieldReadArrayFirstElement(
              groupAccessUserEntry.getGuid(), "hometown", westyEntry));
    } catch (Exception e) {
      failWithStackTrace("Exception while attempting read of groupAccessUser's hometown by westy: ",
              e);
    }
  }

  /**
   * Remove a guid and test that it was.
   *
   * @param westyEntry
   * @param mygroupEntry
   */
  public void test_223_GroupAndACLTestRemoveGuid(GuidEntry westyEntry, GuidEntry mygroupEntry) {
    //CHECKED FOR VALIDITY
    try {
      try {
        clientCommands.groupRemoveGuid(mygroupEntry.getGuid(), westyEntry.getGuid(), mygroupEntry);
      } catch (IOException | ClientException e) {
        failWithStackTrace("Exception removing westy from mygroup: ", e);
      }
    } catch (Exception e) {
      failWithStackTrace("Exception when we were not expecting it in GroupAndACLTestRemoveGuid: ", e);
    }

  }

  /**
   *
   * @param westyEntry
   * @param mygroupEntry
   */
  public void test_224_GroupAndACLTestRemoveGuidCheck(GuidEntry westyEntry, GuidEntry mygroupEntry) {
    try {
      HashSet<String> actual = JSONUtils.JSONArrayToHashSet(
              clientCommands.groupGetMembers(mygroupEntry.getGuid(),
                      mygroupEntry));
      Assert.assertThat(actual, Matchers.not(Matchers.hasItem(westyEntry.getGuid())));
    } catch (ClientException | IOException | JSONException e) {
      failWithStackTrace("Exception while getting group members in GroupAndACLTestRemoveGuidCheck: ", e);
    }
  }

  /**
   * Runs a set of Alias tests.
   */
  @Test
  public void test_229_AliasTests() {
    String alias = "ALIAS-" + RandomString.randomString(6) + "@blah.org";
    test_230_AliasAdd(alias);
    test_231_AliasIsPresent(alias);
    test_232_AliasCheckRemove(alias);
  }

  /**
   * Add an alias.
   *
   * @param alias
   */
  public void test_230_AliasAdd(String alias) {
    //CHECKED FOR VALIDITY
    try {
      // KEEP IN MIND THAT CURRENTLY ONLY ACCOUNT GUIDS HAVE ALIASES
      // add an alias to the masterGuid
      clientCommands.addAlias(masterGuid, alias);
      // lookup the guid using the alias
      Assert.assertEquals(masterGuid.getGuid(), clientCommands.lookupGuid(alias));
    } catch (Exception e) {
      failWithStackTrace("Exception while adding alias: ", e);
    }
  }

  /**
   * Test that recently added alias is present.
   *
   * @param alias
   */
  public void test_231_AliasIsPresent(String alias) {
    //CHECKED FOR VALIDITY
    try {
      // grab all the alias from the guid
      HashSet<String> actual = JSONUtils.JSONArrayToHashSet(clientCommands
              .getAliases(masterGuid));

      /* arun: This test has no reason to succeed because getAliases is
			 * not coordinated or forceCoordinateable.
       */
      // make sure our new one is in there
      Assert.assertThat(actual, Matchers.hasItem(alias));
      // now remove it
      clientCommands.removeAlias(masterGuid, alias);
    } catch (Exception e) {
      failWithStackTrace("Exception removing alias: ", e);
    }
  }

  /**
   * Check that removed alias is gone.
   *
   * @param alias
   */
  public void test_232_AliasCheckRemove(String alias) {
    //CHECKED FOR VALIDITY
    try {
      // and make sure it is gone
      try {
        clientCommands.lookupGuid(alias);
        failWithStackTrace(alias + " should not exist");
      } catch (ClientException e) {
      }
    } catch (Exception e) {
      failWithStackTrace("Exception while checking alias: ", e);
    }
  }

  /**
   * Write access checks.
   *
   * @throws Exception
   */
  @Test
  public void test_240_WriteAccess() throws Exception {
    GuidEntry westyEntry = GuidUtils.lookupOrCreateGuid(clientCommands, masterGuid, "westy240" + RandomString.randomString(6));
    GuidEntry samEntry = GuidUtils.lookupOrCreateGuid(clientCommands, masterGuid, "sam240" + RandomString.randomString(6));
    GuidEntry barneyEntry = GuidUtils.lookupOrCreateGuid(clientCommands, masterGuid, "barney240" + RandomString.randomString(6));
    //CHECKED FOR VALIDITY
    String fieldName = "whereAmI";
    try {
      clientCommands.aclAdd(AclAccessType.WRITE_WHITELIST, westyEntry,
              fieldName, samEntry.getGuid());
    } catch (Exception e) {
      failWithStackTrace("Exception adding Sam to Westy's writelist: ", e);
    }
    // write my own field
    try {
      clientCommands.fieldReplaceFirstElement(westyEntry.getGuid(),
              fieldName, "shopping", westyEntry);
    } catch (IOException | ClientException e) {
      failWithStackTrace("Exception while Westy's writing own field: ", e);
    }
    // now check the value
    Assert.assertEquals("shopping", clientCommands.fieldReadArrayFirstElement(
            westyEntry.getGuid(), fieldName, westyEntry));
    // someone else write my field
    try {
      clientCommands.fieldReplaceFirstElement(westyEntry.getGuid(),
              fieldName, "driving", samEntry);
    } catch (IOException | ClientException e) {
      failWithStackTrace("Exception while Sam writing Westy's field: ", e);
    }
    // now check the value
    Assert.assertEquals("driving", clientCommands.fieldReadArrayFirstElement(
            westyEntry.getGuid(), fieldName, westyEntry));
    // do one that should fail
    try {
      clientCommands.fieldReplaceFirstElement(westyEntry.getGuid(),
              fieldName, "driving", barneyEntry);
      failWithStackTrace("Write by barney should have failed!");
    } catch (ClientException e) {
      //expected
    }
  }

  /**
   * Runs a set of UnsignedRead tests.
   *
   * @throws Exception
   */
  @Test
  public void test_245_UnsignedReadTests() throws Exception {
    GuidEntry unsignedReadAccountGuid = test_249_UnsignedReadDefaultWriteCreateAccountGuid();
    p("test_249_UnsignedReadDefaultWriteCreateAccountGuid");
    test_250_UnsignedReadDefaultAccountGuidWrite(unsignedReadAccountGuid);
    p("test_250_UnsignedReadDefaultAccountGuidWrite");
    test_251_UnsignedReadDefaultAccountGuidRead(unsignedReadAccountGuid);
    p("test_251_UnsignedReadDefaultAccountGuidRead");

    String unsignedReadFieldName = "allreadaccess";
    String unreadAbleReadFieldName = "cannotreadreadaccess";
    GuidEntry unsignedReadTestGuid = test_252_UnsignedReadCreateGuids(unsignedReadAccountGuid);
    test_253_UnsignedReadCheckACL(unsignedReadAccountGuid, unsignedReadTestGuid);
    p("test_253_UnsignedReadCheckACL");

    test_254_UnsignedReadDefaultWrite(unsignedReadTestGuid, unsignedReadFieldName);
    p("test_254_UnsignedReadDefaultWrite");
    test_255_UnsignedReadDefaultRead(unsignedReadTestGuid, unsignedReadFieldName);
    test_256_UnsignedReadFailRemoveDefaultReadAccess(unsignedReadTestGuid);
    test_257_UnsignedReadCheckACLForRecord(unsignedReadAccountGuid, unsignedReadTestGuid);
    test_258_UnsignedReadFailWriteField(unsignedReadTestGuid, unreadAbleReadFieldName);
    test_259_UnsignedReadFailRead(unsignedReadTestGuid, unreadAbleReadFieldName);
    test_260_UnsignedReadAddFieldAccess(unsignedReadTestGuid, unsignedReadFieldName);
    test_261_UnsignedReadWithFieldAccess(unsignedReadTestGuid, unsignedReadFieldName);
    test_262_UnsignedReadFailAgain(unsignedReadTestGuid, unreadAbleReadFieldName);
    test_263_UnsignedReadFailMissingField(unsignedReadTestGuid);

  }

  /**
   *
   * @return GuidEntry
   * @throws Exception
   */
  public GuidEntry test_249_UnsignedReadDefaultWriteCreateAccountGuid() throws Exception {
//    GuidEntry unsignedReadAccountGuid = GuidUtils.lookupOrCreateAccountGuid(clientCommands,
//            "unsignedReadAccountGuid249" + RandomString.randomString(12), PASSWORD, true);
    String name = "unsignedReadAccountGuid249" + RandomString.randomString(12);
    client.execute(GNSCommand.createAccount(name, PASSWORD));
    return GuidUtils.getGUIDKeys(name);
  }

  /**
   * Tests for the default ACL list working with unsigned read.
   * Sets up the field in the account guid.
   *
   * @param unsignedReadAccountGuid
   * @throws IOException
   * @throws ClientException
   */
  public void test_250_UnsignedReadDefaultAccountGuidWrite(GuidEntry unsignedReadAccountGuid) throws ClientException, IOException {
    clientCommands.fieldUpdate(unsignedReadAccountGuid, "aRandomFieldForUnsignedRead", "aRandomValue");
  }

  /**
   * Tests for the default ACL list working with unsigned read.
   * Attempts to read the field.
   *
   * @param unsignedReadAccountGuid
   */
  public void test_251_UnsignedReadDefaultAccountGuidRead(GuidEntry unsignedReadAccountGuid) {
    try {
      String response = clientCommands.fieldRead(unsignedReadAccountGuid.getGuid(), "aRandomFieldForUnsignedRead", null);
      Assert.assertEquals("aRandomValue", response);
    } catch (Exception e) {
      failWithStackTrace("Exception writing field UnsignedReadDefaultMasterWrite: ", e);
    }
  }

  // Creating 4 fields
  /**
   * Create the subguid for the unsigned read tests.
   *
   * @param unsignedReadAccountGuid
   * @return a GuidEntry
   * @throws Exception
   */
  public GuidEntry test_252_UnsignedReadCreateGuids(GuidEntry unsignedReadAccountGuid) throws Exception {
    GuidEntry unsignedReadTestGuid = clientCommands.guidCreate(unsignedReadAccountGuid, "unsignedReadTestGuid" + RandomString.randomString(12));
    System.out.println("Created: " + unsignedReadTestGuid);
    return unsignedReadTestGuid;
  }

  /**
   * Check the default ACL for the unsigned read tests.
   * The account guid and EVERYONE should be in the ENTIRE_RECORD ACL.
   *
   * @param unsignedReadAccountGuid
   * @param unsignedReadTestGuid
   */
  public void test_253_UnsignedReadCheckACL(GuidEntry unsignedReadAccountGuid, GuidEntry unsignedReadTestGuid) {
    try {
      JSONArray expected = new JSONArray(new ArrayList<>(Arrays.asList(unsignedReadAccountGuid.getGuid(),
              GNSProtocol.EVERYONE.toString())));
      JSONArray actual = clientCommands.aclGet(AclAccessType.READ_WHITELIST, unsignedReadTestGuid,
              GNSProtocol.ENTIRE_RECORD.toString(), unsignedReadTestGuid.getGuid());
      JSONAssert.assertEquals(expected, actual, false);
    } catch (Exception e) {
      failWithStackTrace("Exception while retrieving ACL in UnsignedReadCheckACL: ", e);
    }
  }

  /**
   * Write the value the unsigned read tests.
   *
   * @param unsignedReadTestGuid
   * @param unsignedReadFieldName
   */
  public void test_254_UnsignedReadDefaultWrite(GuidEntry unsignedReadTestGuid, String unsignedReadFieldName) {
    try {
      clientCommands.fieldUpdate(unsignedReadTestGuid.getGuid(),
              unsignedReadFieldName, "funkadelicread", unsignedReadTestGuid);
    } catch (IOException | ClientException e) {
      failWithStackTrace("Exception while writing value UnsignedReadDefaultWrite: ", e);
    }
  }

  /**
   * Check the value the unsigned read tests.
   *
   * @param unsignedReadTestGuid
   * @param unsignedReadFieldName
   */
  public void test_255_UnsignedReadDefaultRead(GuidEntry unsignedReadTestGuid, String unsignedReadFieldName) {
    try {
      Assert.assertEquals("funkadelicread",
              clientCommands.fieldRead(unsignedReadTestGuid.getGuid(), unsignedReadFieldName, null));
    } catch (Exception e) {
      failWithStackTrace("Exception reading value in UnsignedReadDefaultRead: ", e);
    }
  }

  /**
   * Remove the default ENTIRE_RECORD read access.
   *
   * @param unsignedReadTestGuid
   */
  public void test_256_UnsignedReadFailRemoveDefaultReadAccess(GuidEntry unsignedReadTestGuid) {
    try {
      clientCommands.aclRemove(AclAccessType.READ_WHITELIST, unsignedReadTestGuid,
              GNSProtocol.ENTIRE_RECORD.toString(), GNSProtocol.ALL_GUIDS.toString());
    } catch (Exception e) {
      failWithStackTrace("Exception removing defa in UnsignedReadDefaultRead: ", e);
    }
  }

  /**
   * Ensure that only the account guid is in the ACL.
   *
   * @param unsignedReadAccountGuid
   * @param unsignedReadTestGuid
   */
  public void test_257_UnsignedReadCheckACLForRecord(GuidEntry unsignedReadAccountGuid, GuidEntry unsignedReadTestGuid) {
    try {
      JSONArray expected = new JSONArray(new ArrayList<>(Arrays.asList(unsignedReadAccountGuid.getGuid())));
      JSONArray actual = clientCommands.aclGet(AclAccessType.READ_WHITELIST, unsignedReadTestGuid,
              GNSProtocol.ENTIRE_RECORD.toString(), unsignedReadTestGuid.getGuid());
      JSONAssert.assertEquals(expected, actual, false);
    } catch (Exception e) {
      failWithStackTrace("Exception while retrieving ACL in UnsignedReadCheckACL: ", e);
    }
  }

  /**
   * Write a value to the field we're going to try to read.
   *
   * @param unsignedReadTestGuid
   * @param unreadAbleReadFieldName
   */
  public void test_258_UnsignedReadFailWriteField(GuidEntry unsignedReadTestGuid, String unreadAbleReadFieldName) {

    try {
      clientCommands.fieldUpdate(unsignedReadTestGuid.getGuid(), unreadAbleReadFieldName, "bummer", unsignedReadTestGuid);
    } catch (IOException | ClientException e) {
      failWithStackTrace("Exception while testing for denied unsigned access in UnsignedRead: ", e);
    }
  }

  /**
   * Attempt a read that should fail because ENTIRE_RECORD was removed.
   *
   * @param unsignedReadTestGuid
   * @param unreadAbleReadFieldName
   */
  public void test_259_UnsignedReadFailRead(GuidEntry unsignedReadTestGuid, String unreadAbleReadFieldName) {
    try {
      try {
        String result = clientCommands.fieldRead(unsignedReadTestGuid.getGuid(), unreadAbleReadFieldName, null);
        failWithStackTrace("Result of read of test guid's "
                + unreadAbleReadFieldName
                + " in "
                + unsignedReadTestGuid.entityName
                + " as world readable was "
                + result
                + " which is wrong because it should have been rejected in UnsignedRead.");
      } catch (ClientException e) {
      }
    } catch (Exception e) {
      failWithStackTrace("Exception while testing for denied unsigned access in UnsignedRead: ", e);
    }
  }

  /**
   * Adds access for just the field we are trying to read.
   *
   * @param unsignedReadTestGuid
   * @param unsignedReadFieldName
   */
  public void test_260_UnsignedReadAddFieldAccess(GuidEntry unsignedReadTestGuid, String unsignedReadFieldName) {
    try {
      clientCommands.aclAdd(AclAccessType.READ_WHITELIST, unsignedReadTestGuid, unsignedReadFieldName,
              GNSProtocol.ALL_GUIDS.toString());
    } catch (Exception e) {
      failWithStackTrace("Exception adding unsigned access in UnsignedReadAddFieldAccess: ", e);
    }
  }

  /**
   * Insures that we can read a world readable field without a guid.
   * This one has an ALL_GUIDS ACL just for this field.
   *
   * @param unsignedReadTestGuid
   * @param unsignedReadFieldName
   */
  public void test_261_UnsignedReadWithFieldAccess(GuidEntry unsignedReadTestGuid, String unsignedReadFieldName) {
    try {
      Assert.assertEquals("funkadelicread", clientCommands.fieldRead(unsignedReadTestGuid.getGuid(),
              unsignedReadFieldName, null));
    } catch (Exception e) {
      failWithStackTrace("Exception while testing for unsigned access in UnsignedReadAddReadWithFieldAccess: ", e);
    }
  }

  /**
   * Insures that we still can't read the non-world-readable field without a guid.
   *
   * @param unsignedReadTestGuid
   * @param unreadAbleReadFieldName
   */
  public void test_262_UnsignedReadFailAgain(GuidEntry unsignedReadTestGuid, String unreadAbleReadFieldName) {
    try {
      try {
        String result = clientCommands.fieldRead(unsignedReadTestGuid.getGuid(), unreadAbleReadFieldName, null);
        failWithStackTrace("Result of read of test guid's "
                + unreadAbleReadFieldName
                + " in "
                + unsignedReadTestGuid.entityName
                + " as world readable was "
                + result
                + " which is wrong because it should have been rejected in UnsignedRead.");
      } catch (ClientException e) {
      }
    } catch (Exception e) {
      failWithStackTrace("Exception while testing for denied unsigned access in UnsignedRead: ", e);
    }
  }

  /**
   *
   * @param unsignedReadTestGuid
   */
  public void test_263_UnsignedReadFailMissingField(GuidEntry unsignedReadTestGuid) {
    String missingFieldName = "missingField" + RandomString.randomString(12);
    try {
      try {
        String result = clientCommands.fieldRead(unsignedReadTestGuid.getGuid(), missingFieldName, null);
        failWithStackTrace("Result of read of test guid's nonexistant field "
                + missingFieldName
                + " in "
                + unsignedReadTestGuid.entityName
                + " as world readable was "
                + result
                + " which is wrong because it should have failed.");
      } catch (ClientException e) {
        // The normal result
      }
    } catch (Exception e) {
      failWithStackTrace("Exception while testing for denied unsigned access in UnsignedRead: ", e);
    }
  }

  /**
   * Runs a set of UnsignedRead tests.
   *
   * @throws Exception
   */
  @Test
  public void test_264_UnsignedWriteTests() throws Exception {
    GuidEntry westyEntry = GuidUtils.lookupOrCreateGuid(clientCommands, masterGuid, "westy264" + RandomString.randomString(6));
    test_265_UnsignedWrite(westyEntry);
    test_270_RemoveField(westyEntry);
  }

  /**
   * Unsigned write tests.
   *
   * @param westyEntry
   * @throws Exception
   */
  public void test_265_UnsignedWrite(GuidEntry westyEntry) throws Exception {
    //CHECKED FOR VALIDITY

    String unsignedWriteFieldName = "allwriteaccess";
    String standardWriteFieldName = "standardwriteaccess";
    try {
      clientCommands.fieldCreateOneElementList(westyEntry.getGuid(),
              unsignedWriteFieldName, "default", westyEntry);
      // make it writeable by everyone
      clientCommands.aclAdd(AclAccessType.WRITE_WHITELIST, westyEntry,
              unsignedWriteFieldName, GNSProtocol.ALL_GUIDS.toString());
      clientCommands.fieldReplaceFirstElement(westyEntry.getGuid(),
              unsignedWriteFieldName, "funkadelicwrite", westyEntry);
      Assert.assertEquals("funkadelicwrite", clientCommands.fieldReadArrayFirstElement(
              westyEntry.getGuid(), unsignedWriteFieldName, westyEntry));

      clientCommands.fieldCreateOneElementList(westyEntry.getGuid(),
              standardWriteFieldName, "bummer", westyEntry);
      try {
        clientCommands.fieldReplaceFirstElement(westyEntry.getGuid(),
                standardWriteFieldName, "funkadelicwrite", null);
        failWithStackTrace("Write of westy's field " + standardWriteFieldName
                + " as world readable should have been rejected.");
      } catch (ClientException e) {
      }
    } catch (Exception e) {
      failWithStackTrace("Exception when we were not expecting it: ", e);
    }
  }

  /**
   * Test removing a field.
   *
   * @param westyEntry
   */
  public void test_270_RemoveField(GuidEntry westyEntry) {
    //CHECKED FOR VALIDITY
    String fieldToDelete = "fieldToDelete";
    try {
      clientCommands.fieldCreateOneElementList(westyEntry.getGuid(),
              fieldToDelete, "work", westyEntry);
    } catch (IOException | ClientException e) {
      failWithStackTrace("Exception while creating the field: ", e);
    }
    try {
      // read my own field
      Assert.assertEquals("work", clientCommands.fieldReadArrayFirstElement(
              westyEntry.getGuid(), fieldToDelete, westyEntry));
    } catch (Exception e) {
      failWithStackTrace("Exception while reading the field " + fieldToDelete + ": ", e);
    }
    try {
      clientCommands.fieldRemove(westyEntry.getGuid(), fieldToDelete, westyEntry);
    } catch (IOException | ClientException e) {
      failWithStackTrace("Exception while removing field: ", e);
    }

    try {
      String result = clientCommands.fieldReadArrayFirstElement(
              westyEntry.getGuid(), fieldToDelete, westyEntry);

      failWithStackTrace("Result of read of westy's " + fieldToDelete + " is " + result
              + " which is wrong because it should have been deleted.");
    } catch (ClientException e) {
    } catch (Exception e) {
      failWithStackTrace("Exception while removing field: ", e);
    }

  }

  /**
   * Tests that lists work correctly.
   *
   * @param westyEntry
   */
  public void test_280_ListOrderAndSetElement(GuidEntry westyEntry) {
    //CHECKED FOR VALIDITY
    try {
      //FIXME:: Why is the parameter being set?
      westyEntry = clientCommands.guidCreate(masterGuid,
              "westy" + RandomString.randomString(12));
    } catch (Exception e) {
      failWithStackTrace("Exception during creation of westyEntry: ", e);
    }
    try {

      clientCommands.fieldCreateOneElementList(westyEntry.getGuid(), "numbers",
              "one", westyEntry);

      Assert.assertEquals("one", clientCommands.fieldReadArrayFirstElement(
              westyEntry.getGuid(), "numbers", westyEntry));

      clientCommands.fieldAppend(westyEntry.getGuid(), "numbers", "two",
              westyEntry);
      clientCommands.fieldAppend(westyEntry.getGuid(), "numbers", "three",
              westyEntry);
      clientCommands.fieldAppend(westyEntry.getGuid(), "numbers", "four",
              westyEntry);
      clientCommands.fieldAppend(westyEntry.getGuid(), "numbers", "five",
              westyEntry);

      List<String> expected = new ArrayList<>(Arrays.asList("one",
              "two", "three", "four", "five"));
      ArrayList<String> actual = JSONUtils
              .JSONArrayToArrayList(clientCommands.fieldReadArray(
                      westyEntry.getGuid(), "numbers", westyEntry));
      Assert.assertEquals(expected, actual);

      clientCommands.fieldSetElement(westyEntry.getGuid(), "numbers", "frank", 2,
              westyEntry);

      expected = new ArrayList<String>(Arrays.asList("one", "two",
              "frank", "four", "five"));
      actual = JSONUtils.JSONArrayToArrayList(clientCommands.fieldReadArray(
              westyEntry.getGuid(), "numbers", westyEntry));
      Assert.assertEquals(expected, actual);

    } catch (Exception e) {
      failWithStackTrace("Unexpected exception during test: ", e);
    }
  }

  private static final Set<GuidEntry> CREATED_GUIDS = new HashSet<>();

  // for use in SELECT test below.
  private static final long SELECT_WAIT = 500;

  /**
   * Tests that selectNear and selectWithin work.
   */
  @Test
  public void test_320_GeoSpatialSelect() {
    /* The SELECT tests will need extra long timeouts and some waitSettle commands
	   * between writes and the SELECTs since the SELECTs are not force-coordinatable
	   * and implementing force-coordination would be very complicated, and may not
	   * worth the trouble since it would only be used for testing purposes.
	   * 
	   * DONE: Add in waitSettle() between client.setLocation and the select call
	   * for this test and any other similar ones.
     */
    try {
      for (int cnt = 0; cnt < 5; cnt++) {
        GuidEntry testEntry = clientCommands.guidCreate(masterGuid, "geoTest-"
                + RandomString.randomString(12));
        CREATED_GUIDS.add(testEntry); // save them so we can delete them later
        clientCommands.setLocation(testEntry, 0.0, 0.0);

        waitSettle(SELECT_WAIT); //See comment under the method header.

        // arun: added this but unclear why we should need this at all
        JSONArray location = clientCommands.getLocation(testEntry.getGuid(),
                testEntry);
        assert (location.getDouble(0) == 0.0 && location.getDouble(1) == 0.0);
      }
    } catch (ClientException | IOException | JSONException e) {
      failWithStackTrace("Exception while writing fields for GeoSpatialSelect: ", e);
    }

    // select near
    try {
      JSONArray loc = new JSONArray();
      loc.put(1.0);
      loc.put(1.0);
      JSONArray result = clientCommands.selectNear(GNSProtocol.LOCATION_FIELD_NAME.toString(), loc, 2000000.0);
      // best we can do should be at least 5, but possibly more objects in
      // results
      Assert.assertThat(result.length(), Matchers.greaterThanOrEqualTo(5));
    } catch (JSONException | ClientException | IOException e) {
      failWithStackTrace("Exception executing selectNear: ", e);
    }

    // select within
    try {
      JSONArray rect = new JSONArray();
      JSONArray upperLeft = new JSONArray();
      upperLeft.put(1.0);
      upperLeft.put(1.0);
      JSONArray lowerRight = new JSONArray();
      lowerRight.put(-1.0);
      lowerRight.put(-1.0);
      rect.put(upperLeft);
      rect.put(lowerRight);
      JSONArray result = clientCommands.selectWithin(GNSProtocol.LOCATION_FIELD_NAME.toString(), rect);
      // best we can do should be at least 5, but possibly more objects in
      // results
      Assert.assertThat(result.length(), Matchers.greaterThanOrEqualTo(5));
    } catch (JSONException | ClientException | IOException e) {
      failWithStackTrace("Exception executing selectWithin: ", e);
    }

    try {
      for (GuidEntry guid : CREATED_GUIDS) {
        clientCommands.guidRemove(masterGuid, guid.getGuid());
      }
      CREATED_GUIDS.clear();
    } catch (ClientException | IOException e) {
      failWithStackTrace("Exception during cleanup: " + e);
    }
  }

  /**
   * Tests that selectQuery works with a reader.
   */
  @Test
  public void test_330_QuerySelectWithReader() {
    String fieldName = "testQuery";
    try {
      for (int cnt = 0; cnt < 5; cnt++) {
        GuidEntry testEntry = clientCommands.guidCreate(masterGuid,
                "queryTest-" + RandomString.randomString(12));
        // Remove default all fields / all guids ACL;
        clientCommands.aclRemove(AclAccessType.READ_WHITELIST, testEntry,
                GNSProtocol.ENTIRE_RECORD.toString(), GNSProtocol.ALL_GUIDS.toString());
        CREATED_GUIDS.add(testEntry); // save them so we can delete them later
        JSONArray array = new JSONArray(Arrays.asList(25));
        clientCommands.fieldReplaceOrCreateList(testEntry.getGuid(), fieldName,
                array, testEntry);
      }
    } catch (ClientException | IOException e) {
      failWithStackTrace("Exception while trying to create the guids: ", e);
    }

    try {
      waitSettle(SELECT_WAIT); //See comment under the method header for test_320_GeoSpatialSelect
      String query = "~" + fieldName + " : ($gt: 0)";
      JSONArray result = clientCommands.selectQuery(masterGuid, query);
//      for (int i = 0; i < result.length(); i++) {
//        System.out.print("guid: " + result.get(i).toString() + "  ");
//      }
      // best we can do should be at least 5, but possibly more objects in
      // results
      Assert.assertThat(result.length(), Matchers.greaterThanOrEqualTo(5));
    } catch (ClientException | IOException e) {
      failWithStackTrace("Exception executing selectQuery: ", e);
    }

    try {
      for (GuidEntry guid : CREATED_GUIDS) {
        clientCommands.guidRemove(masterGuid, guid.getGuid());
      }
      CREATED_GUIDS.clear();
    } catch (ClientException | IOException e) {
      failWithStackTrace("Exception during cleanup: " + e);
    }
  }

  /**
   * Tests that selectQuery without a reader will return results from world readable fields.
   */
  @Test
  public void test_331_QuerySelectWorldReadable() {
    String fieldName = "testQueryWorldReadable";
    try {
      for (int cnt = 0; cnt < 5; cnt++) {
        GuidEntry testEntry = clientCommands.guidCreate(masterGuid,
                "queryTest-" + RandomString.randomString(12));
        CREATED_GUIDS.add(testEntry); // save them so we can delete them later
        JSONArray array = new JSONArray(Arrays.asList(25));
        clientCommands.fieldReplaceOrCreateList(testEntry.getGuid(), fieldName,
                array, testEntry);
      }
    } catch (ClientException | IOException e) {
      failWithStackTrace("Exception while trying to create the guids: ", e);
    }

    try {
      waitSettle(SELECT_WAIT); //See comment under the method header for test_320_GeoSpatialSelect
      String query = "~" + fieldName + " : ($gt: 0)";
      JSONArray result = clientCommands.selectQuery(query);
//      for (int i = 0; i < result.length(); i++) {
//        System.out.print("guid: " + result.get(i).toString() + "  ");
//      }
      // best we can do should be at least 5, but possibly more objects in
      // results
      Assert.assertThat(result.length(), Matchers.greaterThanOrEqualTo(5));
    } catch (ClientException | IOException e) {
      failWithStackTrace("Exception executing selectQuery: ", e);
    }

    try {
      for (GuidEntry guid : CREATED_GUIDS) {
        clientCommands.guidRemove(masterGuid, guid.getGuid());
      }
      CREATED_GUIDS.clear();
    } catch (ClientException | IOException e) {
      failWithStackTrace("Exception during cleanup: " + e);
    }
  }

  /**
   * Tests that selectQuery without a reader will not return results from non-world readable fields.
   */
  @Test
  public void test_332_QuerySelectWorldNotReadable() {
    String fieldName = "testQueryWorldNotReadable";
    try {
      for (int cnt = 0; cnt < 5; cnt++) {
        GuidEntry testEntry = clientCommands.guidCreate(masterGuid,
                "queryTest-" + RandomString.randomString(12));
        // Remove default all fields / all guids ACL;
        clientCommands.aclRemove(AclAccessType.READ_WHITELIST, testEntry,
                GNSProtocol.ENTIRE_RECORD.toString(), GNSProtocol.ALL_GUIDS.toString());
        CREATED_GUIDS.add(testEntry); // save them so we can delete them later
        JSONArray array = new JSONArray(Arrays.asList(25));
        clientCommands.fieldReplaceOrCreateList(testEntry.getGuid(), fieldName,
                array, testEntry);
      }
    } catch (ClientException | IOException e) {
      failWithStackTrace("Exception while trying to create the guids: ", e);
    }

    try {
      waitSettle(SELECT_WAIT); //See comment under the method header for test_320_GeoSpatialSelect
      String query = "~" + fieldName + " : ($gt: 0)";
      JSONArray result = clientCommands.selectQuery(query);
      Assert.assertThat(result.length(), Matchers.equalTo(0));
    } catch (ClientException | IOException e) {
      failWithStackTrace("Exception executing selectQuery: ", e);
    }

    try {
      for (GuidEntry guid : CREATED_GUIDS) {
        clientCommands.guidRemove(masterGuid, guid.getGuid());
      }
      CREATED_GUIDS.clear();
    } catch (ClientException | IOException e) {
      failWithStackTrace("Exception during cleanup: " + e);
    }
  }

  /**
   * Tests fieldSetNull.
   *
   * @throws Exception
   */
  @Test
  public void test_400_SetFieldNull() throws Exception {
    //CHECKED FOR VALIDITY
    String field = "fieldToSetToNull";
    GuidEntry westyEntry = clientCommands.guidCreate(masterGuid,
            "westy400" + RandomString.randomString(12));
    System.out.print("Created: " + westyEntry);
    try {
      clientCommands.fieldCreateOneElementList(westyEntry.getGuid(), field,
              "work", westyEntry);
    } catch (IOException | ClientException e) {
      failWithStackTrace("Exception while creating the field: ", e);
    }
    try {
      // read my own field
      Assert.assertEquals("work", clientCommands.fieldReadArrayFirstElement(
              westyEntry.getGuid(), field, westyEntry));
    } catch (Exception e) {
      failWithStackTrace("Exception while reading the field " + field + ": ", e);
    }
    try {
      clientCommands.fieldSetNull(westyEntry.getGuid(), field, westyEntry);
    } catch (IOException | ClientException e) {
      failWithStackTrace("Exception while setting field to null field: ", e);
    }
    try {
      Assert.assertEquals(null, clientCommands.fieldReadArrayFirstElement(
              westyEntry.getGuid(), field, westyEntry));
    } catch (Exception e) {
      failWithStackTrace("Exception while reading the field " + field + ": ", e);
    }
  }

  /**
   * A set of tests for testing JSONUpdate.
   *
   * @throws Exception
   */
  @Test
  public void test_405_JSONUpdate_Tests() throws Exception {
    GuidEntry westyEntry = test_410_JSONUpdate();
    test_420_NewRead(westyEntry);
    test_430_NewUpdate(westyEntry);

  }

  /**
   * Tests update using JSON.
   *
   * @return GuidEntry created
   * @throws Exception
   */
  public GuidEntry test_410_JSONUpdate() throws Exception {
    //CHECKED FOR VALIDITY
    GuidEntry westyEntry = clientCommands.guidCreate(masterGuid,
            "westy410" + RandomString.randomString(12));
    //System.out.print("Created: " + westyEntry);
    try {
      JSONObject json = new JSONObject();
      json.put("name", "frank");
      json.put("occupation", "busboy");
      json.put("location", "work");
      json.put("friends",
              new ArrayList<>(Arrays.asList("Joe", "Sam", "Billy")));
      JSONObject subJson = new JSONObject();
      subJson.put("einy", "floop");
      subJson.put("meiny", "bloop");
      json.put("gibberish", subJson);
      clientCommands.update(westyEntry, json);
    } catch (JSONException | IOException | ClientException e) {
      failWithStackTrace("Exception while updating JSON: ", e);
    }

    try {
      JSONObject expected = new JSONObject();
      expected.put("name", "frank");
      expected.put("occupation", "busboy");
      expected.put("location", "work");
      expected.put("friends",
              new ArrayList<>(Arrays.asList("Joe", "Sam", "Billy")));
      JSONObject subJson = new JSONObject();
      subJson.put("einy", "floop");
      subJson.put("meiny", "bloop");
      expected.put("gibberish", subJson);
      JSONObject actual = clientCommands.read(westyEntry);
      JSONAssert.assertEquals(expected, actual,
              JSONCompareMode.NON_EXTENSIBLE);
      //System.out.println(actual);
    } catch (Exception e) {
      failWithStackTrace("Exception while reading JSON: ", e);
    }

    try {
      JSONObject json = new JSONObject();
      json.put("occupation", "rocket scientist");
      clientCommands.update(westyEntry, json);
    } catch (JSONException | IOException | ClientException e) {
      failWithStackTrace("Exception while changing \"occupation\" to \"rocket scientist\": ",
              e);
    }

    try {
      JSONObject expected = new JSONObject();
      expected.put("name", "frank");
      expected.put("occupation", "rocket scientist");
      expected.put("location", "work");
      expected.put("friends",
              new ArrayList<>(Arrays.asList("Joe", "Sam", "Billy")));
      JSONObject subJson = new JSONObject();
      subJson.put("einy", "floop");
      subJson.put("meiny", "bloop");
      expected.put("gibberish", subJson);
      JSONObject actual = clientCommands.read(westyEntry);
      JSONAssert.assertEquals(expected, actual,
              JSONCompareMode.NON_EXTENSIBLE);
      //System.out.println(actual);
    } catch (Exception e) {
      failWithStackTrace("Exception while reading change of \"occupation\" to \"rocket scientist\": ",
              e);
    }

    try {
      JSONObject json = new JSONObject();
      json.put("ip address", "127.0.0.1");
      clientCommands.update(westyEntry, json);
    } catch (JSONException | IOException | ClientException e) {
      failWithStackTrace("Exception while adding field \"ip address\" with value \"127.0.0.1\": ",
              e);
    }

    try {
      JSONObject expected = new JSONObject();
      expected.put("name", "frank");
      expected.put("occupation", "rocket scientist");
      expected.put("location", "work");
      expected.put("ip address", "127.0.0.1");
      expected.put("friends",
              new ArrayList<>(Arrays.asList("Joe", "Sam", "Billy")));
      JSONObject subJson = new JSONObject();
      subJson.put("einy", "floop");
      subJson.put("meiny", "bloop");
      expected.put("gibberish", subJson);
      JSONObject actual = clientCommands.read(westyEntry);
      JSONAssert.assertEquals(expected, actual,
              JSONCompareMode.NON_EXTENSIBLE);
      //System.out.println(actual);
    } catch (Exception e) {
      failWithStackTrace("Exception while reading JSON: ", e);
    }

    try {
      clientCommands.fieldRemove(westyEntry.getGuid(), "gibberish", westyEntry);
    } catch (IOException | ClientException e) {
      failWithStackTrace("Exception during remove field \"gibberish\": ", e);
    }

    try {
      JSONObject expected = new JSONObject();
      expected.put("name", "frank");
      expected.put("occupation", "rocket scientist");
      expected.put("location", "work");
      expected.put("ip address", "127.0.0.1");
      expected.put("friends",
              new ArrayList<>(Arrays.asList("Joe", "Sam", "Billy")));
      JSONObject actual = clientCommands.read(westyEntry);
      JSONAssert.assertEquals(expected, actual,
              JSONCompareMode.NON_EXTENSIBLE);
      //System.out.println(actual);
    } catch (Exception e) {
      failWithStackTrace("Exception while reading JSON: ", e);
    }
    return westyEntry;
  }

  /**
   * Tests that dotted field reads work.
   *
   * @param westyEntry
   */
  public void test_420_NewRead(GuidEntry westyEntry) {
    //CHECKED FOR VALIDITY
    try {
      JSONObject json = new JSONObject();
      JSONObject subJson = new JSONObject();
      subJson.put("sally", "red");
      subJson.put("sammy", "green");
      JSONObject subsubJson = new JSONObject();
      subsubJson.put("right", "seven");
      subsubJson.put("left", "eight");
      subJson.put("sally", subsubJson);
      json.put("flapjack", subJson);
      clientCommands.update(westyEntry, json);
    } catch (JSONException | IOException | ClientException e) {
      failWithStackTrace("Exception while adding field \"flapjack\": ", e);
    }

    try {
      JSONObject expected = new JSONObject();
      expected.put("name", "frank");
      expected.put("occupation", "rocket scientist");
      expected.put("location", "work");
      expected.put("ip address", "127.0.0.1");
      expected.put("friends",
              new ArrayList<>(Arrays.asList("Joe", "Sam", "Billy")));
      JSONObject subJson = new JSONObject();
      subJson.put("sammy", "green");
      JSONObject subsubJson = new JSONObject();
      subsubJson.put("right", "seven");
      subsubJson.put("left", "eight");
      subJson.put("sally", subsubJson);
      expected.put("flapjack", subJson);
      JSONObject actual = clientCommands.read(westyEntry);
      JSONAssert.assertEquals(expected, actual,
              JSONCompareMode.NON_EXTENSIBLE);
      //System.out.println(actual);
    } catch (Exception e) {
      failWithStackTrace("Exception while reading JSON: ", e);
    }
    try {
      String actual = clientCommands.fieldRead(westyEntry.getGuid(),
              "flapjack.sally.right", westyEntry);
      Assert.assertEquals("seven", actual);
    } catch (Exception e) {
      failWithStackTrace("Exception while reading \"flapjack.sally.right\": ", e);
    }
    try {
      String actual = clientCommands.fieldRead(westyEntry.getGuid(),
              "flapjack.sally", westyEntry);
      String expected = "{ \"left\" : \"eight\" , \"right\" : \"seven\"}";
      JSONAssert.assertEquals(expected, actual,
              JSONCompareMode.NON_EXTENSIBLE);
    } catch (Exception e) {
      failWithStackTrace("Exception while reading \"flapjack.sally\": ", e);
    }

    try {
      String actual = clientCommands.fieldRead(westyEntry.getGuid(), "flapjack",
              westyEntry);
      String expected = "{ \"sammy\" : \"green\" , \"sally\" : { \"left\" : \"eight\" , \"right\" : \"seven\"}}";
      JSONAssert.assertEquals(expected, actual,
              JSONCompareMode.NON_EXTENSIBLE);
    } catch (Exception e) {
      failWithStackTrace("Exception while reading \"flapjack\": ", e);
    }
  }

  /**
   * Tests that dotted field updates work.
   *
   * @param westyEntry
   */
  public void test_430_NewUpdate(GuidEntry westyEntry) {
    //CHECKED FOR VALIDITY
    try {
      clientCommands.fieldUpdate(westyEntry.getGuid(), "flapjack.sally.right",
              "crank", westyEntry);
    } catch (IOException | ClientException e) {
      failWithStackTrace("Exception while updating field \"flapjack.sally.right\": ", e);
    }
    try {
      JSONObject expected = new JSONObject();
      expected.put("name", "frank");
      expected.put("occupation", "rocket scientist");
      expected.put("location", "work");
      expected.put("ip address", "127.0.0.1");
      expected.put("friends",
              new ArrayList<>(Arrays.asList("Joe", "Sam", "Billy")));
      JSONObject subJson = new JSONObject();
      subJson.put("sammy", "green");
      JSONObject subsubJson = new JSONObject();
      subsubJson.put("right", "crank");
      subsubJson.put("left", "eight");
      subJson.put("sally", subsubJson);
      expected.put("flapjack", subJson);
      JSONObject actual = clientCommands.read(westyEntry);
      JSONAssert.assertEquals(expected, actual,
              JSONCompareMode.NON_EXTENSIBLE);
      //System.out.println(actual);
    } catch (Exception e) {
      failWithStackTrace("Exception while reading JSON: ", e);
    }
    try {
      clientCommands.fieldUpdate(westyEntry.getGuid(), "flapjack.sammy",
              new ArrayList<>(Arrays.asList("One", "Ready", "Frap")),
              westyEntry);
    } catch (IOException | ClientException e) {
      failWithStackTrace("Exception while updating field \"flapjack.sammy\": ", e);
    }
    try {
      JSONObject expected = new JSONObject();
      expected.put("name", "frank");
      expected.put("occupation", "rocket scientist");
      expected.put("location", "work");
      expected.put("ip address", "127.0.0.1");
      expected.put("friends",
              new ArrayList<>(Arrays.asList("Joe", "Sam", "Billy")));
      JSONObject subJson = new JSONObject();
      subJson.put("sammy",
              new ArrayList<>(Arrays.asList("One", "Ready", "Frap")));
      JSONObject subsubJson = new JSONObject();
      subsubJson.put("right", "crank");
      subsubJson.put("left", "eight");
      subJson.put("sally", subsubJson);
      expected.put("flapjack", subJson);
      JSONObject actual = clientCommands.read(westyEntry);
      JSONAssert.assertEquals(expected, actual,
              JSONCompareMode.NON_EXTENSIBLE);
      //System.out.println(actual);
    } catch (Exception e) {
      failWithStackTrace("Exception while reading JSON: ", e);
    }
    try {
      JSONObject moreJson = new JSONObject();
      moreJson.put("name", "dog");
      moreJson.put("flyer", "shattered");
      moreJson.put("crash",
              new ArrayList<>(Arrays.asList("Tango", "Sierra", "Alpha")));
      clientCommands.fieldUpdate(westyEntry.getGuid(), "flapjack", moreJson,
              westyEntry);
    } catch (JSONException | IOException | ClientException e) {
      failWithStackTrace("Exception while updating field \"flapjack\": ", e);
    }
    try {
      JSONObject expected = new JSONObject();
      expected.put("name", "frank");
      expected.put("occupation", "rocket scientist");
      expected.put("location", "work");
      expected.put("ip address", "127.0.0.1");
      expected.put("friends",
              new ArrayList<>(Arrays.asList("Joe", "Sam", "Billy")));
      JSONObject moreJson = new JSONObject();
      moreJson.put("name", "dog");
      moreJson.put("flyer", "shattered");
      moreJson.put("crash",
              new ArrayList<>(Arrays.asList("Tango", "Sierra", "Alpha")));
      expected.put("flapjack", moreJson);
      JSONObject actual = clientCommands.read(westyEntry);
      JSONAssert.assertEquals(expected, actual,
              JSONCompareMode.NON_EXTENSIBLE);
      //System.out.println(actual);
    } catch (Exception e) {
      failWithStackTrace("Exception while reading JSON: ", e);
    }
  }

  /**
   * A set of tests for testing ByteFields
   *
   * @throws IOException
   * @throws ClientException
   */
  public void test_435_ByteField_Tests() throws ClientException, IOException {
    final String BYTE_TEST_FIELD = "testBytes" + RandomString.randomString(12);
    byte[] byteTestValue;
    byteTestValue = test_440_CreateBytesField(BYTE_TEST_FIELD);
    test_441_ReadBytesField(BYTE_TEST_FIELD, byteTestValue);

  }

  /**
   * Tests that updating a field with bytes.
   *
   * @param BYTE_TEST_FIELD
   * @return GuidEntry created
   * @throws IOException
   * @throws ClientException
   */
  public byte[] test_440_CreateBytesField(String BYTE_TEST_FIELD) throws ClientException, IOException {
    //CHECKED FOR VALIDITY
    byte[] byteTestValue = RandomUtils.nextBytes(16000);
    String encodedValue = Base64.encodeToString(byteTestValue, true);
    // System.out.println("Encoded string: " , encodedValue);
    clientCommands.fieldUpdate(masterGuid, BYTE_TEST_FIELD, encodedValue);
    return byteTestValue;
  }

  /**
   * Tests reading a field as bytes.
   *
   * @param byteTestValue
   * @param BYTE_TEST_FIELD
   */
  public void test_441_ReadBytesField(String BYTE_TEST_FIELD, byte[] byteTestValue) {
    //CHECKED FOR VALIDITY
    try {
      String string = clientCommands.fieldRead(masterGuid, BYTE_TEST_FIELD);
      // System.out.println("Read string: " + string);
      Assert.assertArrayEquals(byteTestValue, Base64.decode(string));
    } catch (Exception e) {
      failWithStackTrace("Exception while reading field: ", e);
    }
  }

  private static int numberToCreate = 2;

  /**
   * The test set for testing batch creates. The test will create batches
   * of size 2, 4, 8,..., 128.
   *
   * @throws Exception
   */
  @Test
  @Repeat(times = 7)
  public void test_500_Batch_Tests() throws Exception {
    GuidEntry accountGuidForBatch = test_510_CreateBatchAccountGuid();
    test_511_CreateBatch(accountGuidForBatch);
    test_512_CheckBatch(accountGuidForBatch);
    numberToCreate *= 2;
  }

  /**
   * Create an account for batch test.
   *
   * @return GuidEntry
   * @throws Exception
   */
  public GuidEntry test_510_CreateBatchAccountGuid() throws Exception {
    //CHECKED FOR VALIDITY
    // can change the number to create on the command line
//    GuidEntry accountGuidForBatch;
    if (System.getProperty("count") != null
            && !System.getProperty("count").isEmpty()) {
      numberToCreate = Integer.parseInt(System.getProperty("count"));
    }
    String batchAccountAlias = "batchTest510"
            + RandomString.randomString(12) + "@gns.name";
    client.execute(GNSCommand.createAccount(batchAccountAlias));
    return GuidUtils.getGUIDKeys(batchAccountAlias);
//    accountGuidForBatch = GuidUtils.lookupOrCreateAccountGuid(clientCommands,
//            batchAccountAlias, "password", true);
//    return accountGuidForBatch;
  }

  /**
   * Create some guids with batch create.
   *
   * @param accountGuidForBatch
   */
  public void test_511_CreateBatch(GuidEntry accountGuidForBatch) {
    //CHECKED FOR VALIDITY
    Set<String> aliases = new HashSet<>();
    for (int i = 0; i < numberToCreate; i++) {
      //Brendan: I added Integer.toString(i) to this to guarantee no collisions during creation.
      aliases.add("testGUID511" + Integer.toString(i) + RandomString.randomString(12));
    }
    try {
      clientCommands.guidBatchCreate(accountGuidForBatch, aliases, 20 * 1000);
      //result = client.guidBatchCreate(accountGuidForBatch, aliases);
    } catch (Exception e) {
      failWithStackTrace("Exception while creating guids: ", e);
    }
    //Assert.assertEquals(GNSProtocol.OK_RESPONSE.toString(), result);
  }

  /**
   * Check the batch creation.
   *
   * @param accountGuidForBatch
   */
  public void test_512_CheckBatch(GuidEntry accountGuidForBatch) {
    //CHECKED FOR VALIDITY
    try {
      JSONObject accountRecord = clientCommands
              .lookupAccountRecord(accountGuidForBatch.getGuid());
      Assert.assertEquals(numberToCreate, accountRecord.getInt("guidCnt"));
    } catch (JSONException | ClientException | IOException e) {
      failWithStackTrace("Exception while fetching account record: ", e);
    }
  }

  /**
   * A test set for checking field indices.
   *
   * @throws JSONException
   * @throws IOException
   * @throws ClientException
   */
  @Test
  @Repeat(times = REPEAT)
  public void test_530_Index_Tests() throws ClientException, IOException, JSONException {
    String createIndexTestField = test_540_CreateField();
    test_541_CreateIndex(createIndexTestField);
    test_610_SelectPass(createIndexTestField);
  }

  /**
   * Create a field for test index.
   *
   * @return Created index test field.
   * @throws JSONException
   * @throws IOException
   * @throws ClientException
   */
  public String test_540_CreateField() throws ClientException, IOException, JSONException {
    //CHECKED FOR VALIDITY
    String createIndexTestField = "testField" + RandomString.randomString(12);
    clientCommands.fieldUpdate(masterGuid, createIndexTestField,
            createGeoJSONPolygon(AREA_EXTENT));
    return createIndexTestField;
  }

  /**
   * Create an index.
   *
   * @param createIndexTestField
   */
  public void test_541_CreateIndex(String createIndexTestField) {
    /* TODO: Need to check that fieldCreateIndex always follows the fieldUpdate
	   * done in the previous test, or that it doesn't need to.
     */
    try {
      clientCommands.fieldCreateIndex(masterGuid, createIndexTestField,
              "2dsphere");
    } catch (IOException | ClientException e) {
      failWithStackTrace("Exception while creating index: ", e);
    }
  }

  /**
   * Insure that the indexed field can be used.
   *
   * @param createIndexTestField
   */
  public void test_610_SelectPass(String createIndexTestField) {
    try {
      JSONArray result = clientCommands.selectQuery(buildQuery(
              createIndexTestField, AREA_EXTENT));
      for (int i = 0; i < result.length(); i++) {
        System.out.print(result.get(i).toString() + " ");
      }
      Assert.assertThat(result.length(), Matchers.greaterThanOrEqualTo(1));
    } catch (Exception e) {
      failWithStackTrace("Exception executing second selectNear: ", e);
    }
  }

  private static final String TEST_HIGH_VALUE = "25";
  private static final String TEST_LOW_VALUE = "10";

  /**
   * A set of tests for testing Queries.
   *
   * @throws NoSuchAlgorithmException
   * @throws EncryptionException
   */
  @Test
  @Repeat(times = REPEAT)
  public void test_550_Query_Tests() throws EncryptionException, NoSuchAlgorithmException {
    String groupTestFieldName = "_SelectAutoGroupTestQueryField_" + RandomString.randomString(12);
    GuidEntry groupOneGuid;
    GuidEntry groupTwoGuid;
    String queryOne = "~" + groupTestFieldName + " : {$gt: 20}";
    String queryTwo = "~" + groupTestFieldName + " : 0";
    test_551_QueryRemovePreviousTestFields(groupTestFieldName);

    if (!enable552) {
      return;
    }

    List<GuidEntry> list = test_552_QuerySetupGuids(groupTestFieldName, queryOne, queryTwo);
    groupOneGuid = list.get(0);
    groupTwoGuid = list.get(1);

    test_553_QuerySetupGroup(groupTestFieldName, groupOneGuid);
    test_554_QuerySetupSecondGroup(groupTestFieldName, groupTwoGuid);
    test_555_QueryLookupGroup(groupTestFieldName, groupOneGuid);
    test_556_QueryLookupGroupAgain(groupTestFieldName, groupOneGuid);
    test_557_LookupGroupAgain2(groupTestFieldName, groupOneGuid);
    test_558_QueryLookupGroupAgain3(groupTestFieldName, groupOneGuid);
    test_559_QueryAlterGroup(groupTestFieldName, groupOneGuid);
    test_560_QueryLookupGroupAfterAlterations(groupTestFieldName, groupOneGuid);
    test_561_QueryLookupSecondGroup(groupTestFieldName, groupTwoGuid);
  }

  private static void checkSelectTheReturnValues(JSONArray result, String groupTestFieldName) throws Exception {
    // should be 5
    Assert.assertThat(result.length(), Matchers.equalTo(5));
    // look up the individual values
    for (int i = 0; i < result.length(); i++) {
      BasicGuidEntry guidInfo = new BasicGuidEntry(clientCommands.lookupGuidRecord(result.getString(i)));
      GuidEntry entry = GuidUtils.lookupGuidEntryFromDatabase(clientCommands, guidInfo.getEntityName());
      String value = clientCommands.fieldReadArrayFirstElement(entry, groupTestFieldName);
      Assert.assertEquals(TEST_HIGH_VALUE, value);
    }
  }

  /**
   * Remove any old fields from previous tests for group tests.
   *
   * @param groupTestFieldName
   */
  public void test_551_QueryRemovePreviousTestFields(String groupTestFieldName) {
    // find all the guids that have our field and remove it from them
    try {
      String query = "~" + groupTestFieldName + " : {$exists: true}";
      JSONArray result = clientCommands.selectQuery(query);
      for (int i = 0; i < result.length(); i++) {
        BasicGuidEntry guidInfo = new BasicGuidEntry(clientCommands.lookupGuidRecord(result.getString(i)));
        GuidEntry guidEntry = GuidUtils.lookupGuidEntryFromDatabase(clientCommands, guidInfo.getEntityName());
        System.out.println("Removing from " + guidEntry.getEntityName());
        clientCommands.fieldRemove(guidEntry, groupTestFieldName);
      }
    } catch (Exception e) {
      failWithStackTrace("Trying to remove previous test's fields: ", e);
    }
  }

  private static boolean enable552 = false;

  /**
   * Setup some guids for group testing.
   *
   * @param groupTestFieldName
   * @param queryOne
   * @param queryTwo
   * @return List of groups
   * @throws NoSuchAlgorithmException
   * @throws EncryptionException
   */
  public List<GuidEntry> test_552_QuerySetupGuids(String groupTestFieldName, String queryOne, String queryTwo) throws EncryptionException, NoSuchAlgorithmException {
    if (!enable552) {
      return new ArrayList<>(0);
    }
    try {
      for (int cnt = 0; cnt < 5; cnt++) {
        GuidEntry testEntry = clientCommands.guidCreate(masterGuid, "queryTest-" + RandomString.randomString(6));
        JSONArray array = new JSONArray(Arrays.asList(Integer.parseInt(TEST_HIGH_VALUE)));
        clientCommands.fieldReplaceOrCreateList(testEntry, groupTestFieldName, array);
      }
      for (int cnt = 0; cnt < 5; cnt++) {
        GuidEntry testEntry = clientCommands.guidCreate(masterGuid, "queryTest-" + RandomString.randomString(6));
        JSONArray array = new JSONArray(Arrays.asList(Integer.parseInt(TEST_LOW_VALUE)));
        clientCommands.fieldReplaceOrCreateList(testEntry, groupTestFieldName, array);
      }
    } catch (Exception e) {
      failWithStackTrace("Exception while trying to create the guids: ", e);
    }
    // the HRN is a hash of the query
    String groupOneGuidName = Base64.encodeToString(SHA1HashFunction.getInstance().hash(queryOne), false);
    GuidEntry groupOneGuid = GuidUtils.lookupOrCreateGuidEntry(groupOneGuidName, GNSClientCommands.getGNSProvider());
    //groupGuid = client.guidCreate(masterGuid, groupGuidName + RandomString.randomString(6));

    // the HRN is a hash of the query
    String groupTwoGuidName = Base64.encodeToString(SHA1HashFunction.getInstance().hash(queryTwo), false);
    GuidEntry groupTwoGuid = GuidUtils.lookupOrCreateGuidEntry(groupTwoGuidName, GNSClientCommands.getGNSProvider());
    //groupTwoGuid = client.guidCreate(masterGuid, groupTwoGuidName + RandomString.randomString(6));

    List<GuidEntry> list = new ArrayList<>(2);
    list.add(groupOneGuid);
    list.add(groupTwoGuid);
    return list;
  }

  /**
   * Setup some grouos for group testing.
   *
   * @param groupTestFieldName
   * @param groupOneGuid
   */
  public void test_553_QuerySetupGroup(String groupTestFieldName, GuidEntry groupOneGuid) {
    if (!enable552) {
      return;
    }

    try {
      String query = "~" + groupTestFieldName + " : {$gt: 20}";
      JSONArray result = clientCommands.selectSetupGroupQuery(masterGuid, groupOneGuid.getPublicKeyString(), query, 0); // make the min refresh 0 seconds so the test will never fail
      System.out.println("*****SETUP guid named " + groupOneGuid.getEntityName() + ": ");
      for (int i = 0; i < result.length(); i++) {
        System.out.println(result.get(i).toString());
      }
      // best we can do should be at least 5, but possibly more objects in results
      Assert.assertThat(result.length(), Matchers.greaterThanOrEqualTo(5));
    } catch (Exception e) {
      failWithStackTrace("Exception executing selectSetupGroupQuery: ", e);
    }
  }

  /**
   * Setup some empty group guids for group testing.
   *
   * @param groupTestFieldName
   * @param groupTwoGuid
   */
  public void test_554_QuerySetupSecondGroup(String groupTestFieldName, GuidEntry groupTwoGuid) {
    if (!enable552) {
      return;
    }

    try {
      String query = "~" + groupTestFieldName + " : 0";
      JSONArray result = clientCommands.selectSetupGroupQuery(masterGuid, groupTwoGuid.getPublicKeyString(), query, 0); // make the min refresh 0 seconds so the test will never fail
      System.out.println("*****SETUP SECOND guid named " + groupTwoGuid.getEntityName() + ": (should be empty) ");
      for (int i = 0; i < result.length(); i++) {
        System.out.println(result.get(i).toString());
      }
      // should be nothing in this group now
      Assert.assertThat(result.length(), Matchers.equalTo(0));
    } catch (Exception e) {
      failWithStackTrace("Exception executing second selectSetupGroupQuery: ", e);
    }
  }

  /**
   * Lookup the group members.
   *
   * @param groupTestFieldName
   * @param groupOneGuid
   */
  public void test_555_QueryLookupGroup(String groupTestFieldName, GuidEntry groupOneGuid) {
    if (!enable552) {
      return;
    }

    try {
      JSONArray result = clientCommands.selectLookupGroupQuery(groupOneGuid.getGuid());
      checkSelectTheReturnValues(result, groupTestFieldName);
    } catch (Exception e) {
      failWithStackTrace("Exception executing selectLookupGroupQuery: ", e);
    }
  }

  /**
   * Lookup the group members again.
   *
   * @param groupTestFieldName
   * @param groupOneGuid
   */
  public void test_556_QueryLookupGroupAgain(String groupTestFieldName, GuidEntry groupOneGuid) {
    if (!enable552) {
      return;
    }

    try {
      JSONArray result = clientCommands.selectLookupGroupQuery(groupOneGuid.getGuid());
      checkSelectTheReturnValues(result, groupTestFieldName);
    } catch (Exception e) {
      failWithStackTrace("Exception executing selectLookupGroupQuery: ", e);
    }
  }

  /**
   * Lookup the group members again.
   *
   * @param groupTestFieldName
   * @param groupOneGuid
   */
  public void test_557_LookupGroupAgain2(String groupTestFieldName, GuidEntry groupOneGuid) {
    if (!enable552) {
      return;
    }

    try {
      JSONArray result = clientCommands.selectLookupGroupQuery(groupOneGuid.getGuid());
      checkSelectTheReturnValues(result, groupTestFieldName);
    } catch (Exception e) {
      failWithStackTrace("Exception executing selectLookupGroupQuery: ", e);
    }
  }

  /**
   * Lookup the group members again.
   *
   * @param groupTestFieldName
   * @param groupOneGuid
   */
  public void test_558_QueryLookupGroupAgain3(String groupTestFieldName, GuidEntry groupOneGuid) {
    if (!enable552) {
      return;
    }

    try {
      JSONArray result = clientCommands.selectLookupGroupQuery(groupOneGuid.getGuid());
      checkSelectTheReturnValues(result, groupTestFieldName);
    } catch (Exception e) {
      failWithStackTrace("Exception executing selectLookupGroupQuery: ", e);
    }
  }

  /**
   * Change the value that the group is triggered on.
   *
   * @param groupTestFieldName
   * @param groupOneGuid
   */
  // Change all the testQuery fields except 1 to be equal to zero
  public void test_559_QueryAlterGroup(String groupTestFieldName, GuidEntry groupOneGuid) {
    if (!enable552) {
      return;
    }

    try {
      JSONArray result = clientCommands.selectLookupGroupQuery(groupOneGuid.getGuid());
      // change ALL BUT ONE to be ZERO
      for (int i = 0; i < result.length() - 1; i++) {
        BasicGuidEntry guidInfo = new BasicGuidEntry(clientCommands.lookupGuidRecord(result.getString(i)));
        GuidEntry entry = GuidUtils.lookupGuidEntryFromDatabase(clientCommands, guidInfo.getEntityName());
        JSONArray array = new JSONArray(Arrays.asList(0));
        clientCommands.fieldReplaceOrCreateList(entry, groupTestFieldName, array);
      }
    } catch (Exception e) {
      failWithStackTrace("Exception while trying to alter the fields: ", e);
    }
  }

  /**
   * Lookup the group members again after the change.
   *
   * @param groupTestFieldName
   * @param groupOneGuid
   */
  public void test_560_QueryLookupGroupAfterAlterations(String groupTestFieldName, GuidEntry groupOneGuid) {
    if (!enable552) {
      return;
    }

    try {
      JSONArray result = clientCommands.selectLookupGroupQuery(groupOneGuid.getGuid());
      // should only be one
      Assert.assertThat(result.length(), Matchers.equalTo(1));
      // look up the individual values
      for (int i = 0; i < result.length(); i++) {
        BasicGuidEntry guidInfo = new BasicGuidEntry(clientCommands.lookupGuidRecord(result.getString(i)));
        GuidEntry entry = GuidUtils.lookupGuidEntryFromDatabase(clientCommands, guidInfo.getEntityName());
        String value = clientCommands.fieldReadArrayFirstElement(entry, groupTestFieldName);
        Assert.assertEquals(TEST_HIGH_VALUE, value);
      }
    } catch (Exception e) {
      failWithStackTrace("Exception executing selectLookupGroupQuery: ", e);
    }
  }

  /**
   * Check to see if the second group has members now... it should.
   *
   * @param groupTestFieldName
   * @param groupTwoGuid
   */
  public void test_561_QueryLookupSecondGroup(String groupTestFieldName, GuidEntry groupTwoGuid) {
    if (!enable552) {
      return;
    }

    try {
      JSONArray result = clientCommands.selectLookupGroupQuery(groupTwoGuid.getGuid());
      // should be 4 now
      Assert.assertThat(result.length(), Matchers.equalTo(4));
      // look up the individual values
      for (int i = 0; i < result.length(); i++) {
        BasicGuidEntry guidInfo = new BasicGuidEntry(clientCommands.lookupGuidRecord(result.getString(i)));
        GuidEntry entry = GuidUtils.lookupGuidEntryFromDatabase(clientCommands, guidInfo.getEntityName());
        String value = clientCommands.fieldReadArrayFirstElement(entry, groupTestFieldName);
        Assert.assertEquals("0", value);
      }
    } catch (Exception e) {
      failWithStackTrace("Exception executing selectLookupGroupQuery: ", e);
    }
  }

  /**
   * Test to check context service triggers.
   */
  // these two attributes right now are supported by CS
  @Test
  @Repeat(times = REPEAT)
  public void test_620_contextServiceTest() {
    // run it only when CS is enabled
    // to check if context service is enabled.
    boolean enableContextService = Config.getGlobalBoolean(GNSConfig.GNSC.ENABLE_CNS);
    String csIPPort = Config.getGlobalString(GNSConfig.GNSC.CNS_NODE_ADDRESS);
    if (enableContextService) {
      try {
        JSONObject attrValJSON = new JSONObject();
        attrValJSON.put("geoLocationCurrentLat", 42.466);
        attrValJSON.put("geoLocationCurrentLong", -72.58);

        clientCommands.update(masterGuid, attrValJSON);
        // just wait for 2 sec before sending search
        Thread.sleep(1000);

        String[] parsed = csIPPort.split(":");
        String csIP = parsed[0];
        int csPort = Integer.parseInt(parsed[1]);

        ContextServiceClient<Integer> csClient = new ContextServiceClient<>(
                csIP, csPort);

        // context service query format
        String query = "SELECT GUID_TABLE.guid FROM GUID_TABLE WHERE geoLocationCurrentLat >= 40 "
                + "AND geoLocationCurrentLat <= 50 AND "
                + "geoLocationCurrentLong >= -80 AND "
                + "geoLocationCurrentLong <= -70";
        JSONArray resultArray = new JSONArray();
        // third argument is arbitrary expiry time, not used now
        int resultSize = csClient.sendSearchQuery(query, resultArray,
                300000);
        Assert.assertThat(resultSize, Matchers.greaterThanOrEqualTo(1));

      } catch (Exception e) {
        failWithStackTrace("Exception during contextServiceTest: ", e);
      }
    }
  }

  /**
   * A basic test to insure that setting LNS Proxy minimally doesn't break.
   */
  // This requires that the LOCAL_NAME_SERVER_NODES config option be set.
  @Test
  @Repeat(times = REPEAT)
  public void test_630_CheckLNSProxy() {
    try {
      //PaxosConfig.getActives() works here because the server and client use the same properties file.
      InetAddress lnsAddress = PaxosConfig.getActives().values().iterator().next().getAddress();
      clientCommands.setGNSProxy(new InetSocketAddress(lnsAddress, 24598));
    } catch (Exception e) {
      failWithStackTrace("Exception while setting proxy: ", e);
    }
    String guidString = null;
    try {
      guidString = clientCommands.lookupGuid(accountAlias);
    } catch (IOException | ClientException e) {
      failWithStackTrace("Exception while looking up guid: ", e);
    }
    JSONObject json = null;
    if (guidString != null) {
      try {
        json = clientCommands.lookupAccountRecord(guidString);
      } catch (IOException | ClientException e) {
        failWithStackTrace("Exception while looking up account record: ", e);
      }
    }
    Assert.assertNotNull("Account record is null", json);
    try {
      Assert.assertEquals("Account name doesn't match",
              accountAlias, json.getString(GNSProtocol.ACCOUNT_RECORD_USERNAME.toString()));
    } catch (JSONException e) {
      failWithStackTrace("Exception while looking up account name: ", e);
    }
    clientCommands.setGNSProxy(null);
  }

  // HELPER STUFF
  private static final String POLYGON = "Polygon";
  private static final String COORDINATES = "coordinates";
  private static final String TYPE = "type";

  private static JSONObject createGeoJSONPolygon(List<Point2D> coordinates)
          throws JSONException {
    JSONArray ring = new JSONArray();
    for (int i = 0; i < coordinates.size(); i++) {
      ring.put(
              i,
              new JSONArray(Arrays.asList(coordinates.get(i).getX(),
                      coordinates.get(i).getY())));
    }
    JSONArray jsonCoordinates = new JSONArray();
    jsonCoordinates.put(0, ring);
    JSONObject json = new JSONObject();
    json.put(TYPE, POLYGON);
    json.put(COORDINATES, jsonCoordinates);
    return json;
  }

  private static final double LEFT = -98.08;
  private static final double RIGHT = -96.01;
  private static final double TOP = 33.635;
  private static final double BOTTOM = 31.854;

  private static final Point2D UPPER_LEFT = new Point2D.Double(LEFT, TOP);
  // private static final GlobalCoordinate UPPER_LEFT = new
  // GlobalCoordinate(33.45, -98.08);
  private static final Point2D UPPER_RIGHT = new Point2D.Double(RIGHT, TOP);
  // private static final GlobalCoordinate UPPER_RIGHT = new
  // GlobalCoordinate(33.45, -96.01);
  private static final Point2D LOWER_RIGHT = new Point2D.Double(RIGHT, BOTTOM);
  // private static final GlobalCoordinate LOWER_RIGHT = new
  // GlobalCoordinate(32.23, -96.01);
  private static final Point2D LOWER_LEFT = new Point2D.Double(LEFT, BOTTOM);
  // private static final GlobalCoordinate LOWER_LEFT = new
  // GlobalCoordinate(32.23, -98.08);

  private static final List<Point2D> AREA_EXTENT = new ArrayList<>(
          Arrays.asList(UPPER_LEFT, UPPER_RIGHT, LOWER_RIGHT, LOWER_LEFT,
                  UPPER_LEFT));

  private static String buildQuery(String locationField,
          List<Point2D> coordinates) throws JSONException {
    return "~" + locationField + ":{" + "$geoIntersects :{" + "$geometry:"
            + createGeoJSONPolygon(coordinates).toString() + "}" + "}";
  }

  /**
   *
   * @param args
   */
  public static void main(String[] args) {
    Result result = JUnitCore.runClasses(ServerIntegrationTest.class
    );
    System.out.println("\n\n-----------Completed all "
            + result.getRunCount()
            + " tests"
            + (result.getFailureCount() > 0 ? "; the following "
                    + result.getFailureCount() + " tests failed:"
                    : " successfully") + "--------------");
    for (Failure failure : result.getFailures()) {
      System.out.println(failure.toString() + "\n");
    }
  }
}
