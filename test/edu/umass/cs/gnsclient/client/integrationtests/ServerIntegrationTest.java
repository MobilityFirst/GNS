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

import edu.umass.cs.gigapaxos.PaxosConfig;
import edu.umass.cs.gnscommon.CommandType;
import edu.umass.cs.gnscommon.GNSProtocol;
import edu.umass.cs.gnscommon.AclAccessType;
import edu.umass.cs.gnscommon.ResponseCode;
import edu.umass.cs.contextservice.client.ContextServiceClient;
import edu.umass.cs.gnsclient.client.GNSClient;
import edu.umass.cs.gnsclient.client.GNSClientCommands;
import edu.umass.cs.gnsclient.client.util.BasicGuidEntry;
import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.gnsclient.client.util.GuidUtils;
import edu.umass.cs.gnsclient.client.util.JSONUtils;
import edu.umass.cs.gnsclient.client.util.SHA1HashFunction;
import edu.umass.cs.gnscommon.utils.RandomString;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnscommon.exceptions.client.EncryptionException;
import edu.umass.cs.gnscommon.exceptions.client.FieldNotFoundException;
import edu.umass.cs.gnsclient.jsonassert.JSONAssert;
import edu.umass.cs.gnsclient.jsonassert.JSONCompareMode;
import edu.umass.cs.gnscommon.utils.Base64;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import static org.hamcrest.Matchers.*;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;
import org.junit.runners.MethodSorters;
import edu.umass.cs.gnscommon.utils.ThreadUtils;
import edu.umass.cs.gnsserver.database.MongoRecords;
import edu.umass.cs.gnsserver.main.GNSConfig;
import edu.umass.cs.reconfiguration.ReconfigurationConfig;
import edu.umass.cs.reconfiguration.reconfigurationutils.DefaultNodeConfig;
import edu.umass.cs.utils.Config;
import edu.umass.cs.utils.DefaultTest;
import edu.umass.cs.utils.Util;

import java.awt.geom.Point2D;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.security.NoSuchAlgorithmException;
import java.util.Set;

import org.apache.commons.lang3.RandomUtils;
import org.json.JSONException;
import org.junit.Assert;

/**
 * Functionality test for core elements in the client using the
 * GNSClientCommands.
 *
 */
//@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ServerIntegrationTest extends DefaultTest {

  private static final String DEFAULT_ACCOUNT_ALIAS = "support@gns.name";

  private static String accountAlias = DEFAULT_ACCOUNT_ALIAS; // REPLACE //
  // ALIAS
  private static final String PASSWORD = "password";
  private static GNSClientCommands client = null;
  //private static GNSClientCommandsV2 client = null;
  private static GuidEntry masterGuid;
  private static GuidEntry guidToDeleteEntry;

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
  
  private static final int DEFAULT_READ_TIMEOUT = 10*1000; //Default read timeout in ms.
  private static final int LONG_READ_TIMEOUT = 30*1000; //Read timeout for tests that require more time.

  private static final String getPath(String filename) {
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
            "conf/logging.gns.properties"),
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

  private static final void setProperties() {
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

  private static final String getGigaPaxosOptions() {
    String gpOptions = "";
    for (DefaultProps prop : DefaultProps.values()) {
      gpOptions += " -D" + prop.key + "=" + System.getProperty(prop.key);
    }
    return gpOptions + " -ea";
  }

  private static final boolean useGPScript() {
    return System.getProperty(DefaultProps.SERVER_COMMAND.key).contains(
            GP_SERVER);
  }

  private static final void failWithStackTrace(String message, Exception... e) {
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
  private static long WAIT_TILL_ALL_SERVERS_READY = 5000;

  /**
   *
   * @throws Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    // Run the server.
    String waitString = System.getProperty("waitTillAllServersReady");
    if (waitString != null) {
      WAIT_TILL_ALL_SERVERS_READY = Integer.parseInt(waitString);
    }

    if (System.getProperty("startServer") != null
            && System.getProperty("startServer").equals("true")) {

      // clear explicitly if gigapaxos
      if (useGPScript()) {
        RunServer
                .command(
                        "kill -s TERM `ps -ef | grep GNS.jar | grep -v grep | "
                        + "grep -v ServerIntegrationTest  | grep -v \"context\" | awk '{print $2}'`",
                        ".");
        System.out.println(System
                .getProperty(DefaultProps.SERVER_COMMAND.key)
                + " "
                + getGigaPaxosOptions() + " forceclear all");

        RunServer.command(
                System.getProperty(DefaultProps.SERVER_COMMAND.key)
                + " " + getGigaPaxosOptions()
                + " forceclear all", ".");

        /* We need to do this to limit the number of files used by mongo.
				 * Otherwise failed runs quickly lead to more failed runs because
				 * index files created in previous runs are not removed.
         */
        dropAllDatabases();

        options = getGigaPaxosOptions() + " restart all";
      } else {
        options = SCRIPTS_OPTIONS;
      }

      String logFile = System.getProperty(DefaultProps.LOGGING_PROPERTIES.key);
      ArrayList<String> output = RunServer.command("cat " + logFile + " | grep \"java.util.logging.FileHandler.pattern\" | sed 's/java.util.logging.FileHandler.pattern = //g'", ".", false);
      String logFiles = output.get(0) + "*";
      RunServer.command("rm -f " + logFiles, ".", false);

      System.out.println(System
              .getProperty(DefaultProps.SERVER_COMMAND.key)
              + " "
              + options);
      output = RunServer.command(
              System.getProperty(DefaultProps.SERVER_COMMAND.key) + " "
              + options, ".");
      if (output != null) {
        for (String line : output) {
          System.out.println(line);
        }
      } else {
        failWithStackTrace("Server command failure: ; aborting all tests.");
      }
    }

    String gpConfFile = System.getProperty(DefaultProps.GIGAPAXOS_CONFIG.key);
    String logFile = System.getProperty(DefaultProps.LOGGING_PROPERTIES.key);

    ArrayList<String> output = RunServer.command("cat " + logFile + " | grep \"java.util.logging.FileHandler.pattern\" | sed 's/java.util.logging.FileHandler.pattern = //g'", ".", false);
    String logFiles = output.get(0) + "*";

    System.out.println("Waiting for servers to be ready...");
    output = RunServer.command("cat " + gpConfFile + " | grep \"reconfigurator\\.\" | wc -l ", ".", false);
    String temp = output.get(0);
    temp = temp.replaceAll("\\s", "");
    int numRC = Integer.parseInt(temp);
    output = RunServer.command("cat " + gpConfFile + " | grep \"active\\.\" | wc -l ", ".", false);
    temp = output.get(0);
    temp = temp.replaceAll("\\s", "");
    int numAR = Integer.parseInt(temp);
    int numServers = numRC + numAR;

    output = RunServer.command("ls " + logFiles + " 2> /dev/null | wc -l ", ".", false);
    temp = output.get(0);
    temp = temp.replaceAll("\\s", "");
    int numLogFiles = Integer.parseInt(temp);
    while (numLogFiles == 0) {
      Thread.sleep(5000);
      output = RunServer.command("ls " + logFiles + " 2> /dev/null | wc -l ", ".", false);
      temp = output.get(0);
      temp = temp.replaceAll("\\s", "");
      numLogFiles = Integer.parseInt(temp);
    }

    output = RunServer.command("cat " + logFiles + " | grep -a \"server ready\" | wc -l ", ".", false);
    temp = output.get(0);
    temp = temp.replaceAll("\\s", "");
    int numServersUp = Integer.parseInt(temp);
    System.out.println(Integer.toString(numServersUp) + " out of " + Integer.toString(numServers) + " servers are ready.");
    while (numServersUp < numServers) {
      Thread.sleep(5000);
      output = RunServer.command("cat " + logFiles + " | grep -a \"server ready\" | wc -l ", ".", false);
      temp = output.get(0);
      temp = temp.replaceAll("\\s", "");
      numServersUp = Integer.parseInt(temp);
      System.out.println(Integer.toString(numServersUp) + " out of " + Integer.toString(numServers) + " servers are ready.");

    }

    System.out.println("Starting client");

    client = new GNSClientCommands();
    //client = new GNSClientCommandsV2();
    // Make all the reads be coordinated
    client.setForceCoordinatedReads(true);
    //Set default read timoeut
    client.setReadTimeout(DEFAULT_READ_TIMEOUT);
    
    // arun: connectivity check embedded in GNSClient constructor
    boolean connected = client instanceof GNSClient;
    if (connected) {
      System.out.println("Client created and connected to server.");
    }
    //
    int tries = 5;
    boolean accountCreated = false;

    long t = System.currentTimeMillis();
    Thread.sleep(WAIT_TILL_ALL_SERVERS_READY);

    do {
      try {
        System.out.println("Creating account guid: " + (tries - 1)
                + " attempt remaining.");
        masterGuid = GuidUtils.lookupOrCreateAccountGuid(client,
                accountAlias, PASSWORD, true);
        accountCreated = true;
      } catch (Exception e) {
        e.printStackTrace();
        ThreadUtils.sleep((5 - tries) * 5000);
      }
    } while (!accountCreated && --tries > 0);
    if (accountCreated == false) {
      failWithStackTrace("Failure setting up account guid; aborting all tests.");
    }

  }

  /**
   *
   * @throws Exception
   */
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
	  client.close();
    /* arun: need a more efficient, parallel implementation of removal
		 * of sub-guids, otherwise this times out.
     */
    //client.accountGuidRemove(masterGuid);
    if (System.getProperty("startServer") != null
            && System.getProperty("startServer").equals("true")) {
      if (useGPScript()) {
        String command = System
                .getProperty(DefaultProps.SERVER_COMMAND.key)
                + " "
                + getGigaPaxosOptions() + " stop all";
        System.out
                .print("Stopping all servers in "
                        + System.getProperty(DefaultProps.GIGAPAXOS_CONFIG.key) + "...");

        try {
          RunServer.command(command, ".");
        } catch (Exception e) {
          System.out.println(" failed to stop all servers with [" + command + "]");
          e.printStackTrace();
          throw e;
        }
        System.out.println(" stopped all servers.");
      } else {
        ArrayList<String> output = RunServer.command(
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

    if (client != null) {
      client.close();
    }
    System.out.println("\nPrinting reverse-engineered return types:");
    for (CommandType type : GNSClientCommands.REVERSE_ENGINEER.keySet()) {
      System.out.println(type + " returns "
              + GNSClientCommands.REVERSE_ENGINEER.get(type) + "; e.g., "
              + Util.truncate(GNSClientCommands.RETURN_VALUE_EXAMPLE.get(type), 64, 64));

    }
  }

  private static void dropAllDatabases() {
    for (String server : new DefaultNodeConfig<String>(
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

  private static final int RETRANSMISSION_INTERVAL = 100;
  
  // arun: this should be zero
  /* Brendan: setting this to nonzero so it can be used for SELECT tests since
   * SELECTS don't consistently read UPDATES.
   */
  private static final int COORDINATION_WAIT = 10000;

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
  private static void waitSettle() {
    try {
      if (COORDINATION_WAIT > 0) {
        Thread.sleep(COORDINATION_WAIT);
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
   */
  @Test
  public void test_010_CreateEntity() {
    //CHECKED FOR VALIDITY
    String alias = "testGUID" + RandomString.randomString(12);
    GuidEntry guidEntry = null;
    try {
      guidEntry = client.guidCreate(masterGuid, alias);
    } catch (Exception e) {
      failWithStackTrace("Exception while creating guid: ", e);
    }
    Assert.assertNotNull(guidEntry);
    Assert.assertEquals(alias, guidEntry.getEntityName());
  }

  /**
   * Removes a guid.
   */
  @Test
  public void test_020_RemoveGuid() {
    //CHECKED FOR VALIDITY
    String testGuidName = "testGUID" + RandomString.randomString(12);
    GuidEntry testGuid = null;
    try {
      testGuid = client.guidCreate(masterGuid, testGuidName);
    } catch (Exception e) {
      failWithStackTrace("Exception while creating testGuid: ", e);
    }
    try {
      client.guidRemove(masterGuid, testGuid.getGuid());
    } catch (Exception e) {
      failWithStackTrace("Exception while removing testGuid: ", e);
    }
    try {
      client.lookupGuidRecord(testGuid.getGuid());
      failWithStackTrace("Lookup testGuid should have throw an exception.");
    } catch (ClientException e) {

    } catch (IOException e) {
      failWithStackTrace("Exception while doing Lookup testGuid: ", e);
    }
  }

  /**
   * Removes a guid not using an account guid.
   */
  @Test
  public void test_030_RemoveGuidSansAccountInfo() {
    //CHECKED FOR VALIDITY
    String testGuidName = "testGUID" + RandomString.randomString(12);
    GuidEntry testGuid = null;
    try {
      testGuid = client.guidCreate(masterGuid, testGuidName);
    } catch (Exception e) {
      failWithStackTrace("Exception while creating testGuid: ", e);
    }
    try {
      client.guidRemove(testGuid);
    } catch (Exception e) {
      failWithStackTrace("Exception while removing testGuid: ", e);
    }
    
    try {
      client.lookupGuidRecord(testGuid.getGuid());
      failWithStackTrace("Lookup testGuid should have throw an exception.");
    } catch (ClientException e) {

    } catch (IOException e) {
      failWithStackTrace("Exception while doing Lookup testGuid: ", e);
    }
  }

  private static final String REMOVE_ACCOUNT_PASSWORD = "removalPassword";

  /**
   * Runs the RemoveAccountWithPassword tests as one independent unit.
   * @throws Exception
   */
  @Test
  public void test_031_RemoveAccountWithPasswordTest() throws Exception{
	  String accountToRemoveWithPassword =  RandomString.randomString(12)+ "-" + "passwordremovetest@gns.name";
	  GuidEntry accountToRemoveGuid = test_035_RemoveAccountWithPasswordCreateAccount(accountToRemoveWithPassword);
	  test_036_RemoveAccountWithPasswordCheckAccount(accountToRemoveGuid);
	  test_037_RemoveAccountWithPasswordRemoveAccount(accountToRemoveWithPassword);
	  test_038_RemoveAccountWithPasswordCheckAccountAfterRemove(accountToRemoveWithPassword);
  }
  
  /**
   * Create an account to remove using the password.
 * @throws Exception 
   */
  private GuidEntry test_035_RemoveAccountWithPasswordCreateAccount(String accountToRemoveWithPassword) throws Exception {
    /* FIXED: GuidUtils.lookupOrCreateAccountGuid() is safe 
	 * since the account verification step is coordinated later on in its chain.
	 * TODO: Make sure that gigapaxos guaruntees UPDATE your CREATES for servers with 
	 * greater than 3 replicas.
     */

      return GuidUtils.lookupOrCreateAccountGuid(client, accountToRemoveWithPassword, REMOVE_ACCOUNT_PASSWORD, true);
  }

  /**
   * Check the account to remove using the password.
 * @throws IOException 
 * @throws ClientException 
   */
  private void test_036_RemoveAccountWithPasswordCheckAccount(GuidEntry accountToRemoveGuid) throws ClientException, IOException {
    //CHECKED FOR VALIDITY
      // this should be using the guid
      ThreadUtils.sleep(100);
      client.lookupAccountRecord(accountToRemoveGuid.getGuid());
  }

  /**
   * Remove the account using the password.
 * @throws Exception 
   */
  private void test_037_RemoveAccountWithPasswordRemoveAccount(String accountToRemoveWithPassword) throws Exception {
    //CHECKED FOR VALIDITY
      client.accountGuidRemoveWithPassword(accountToRemoveWithPassword, REMOVE_ACCOUNT_PASSWORD);
  }

  /**
   * Check the account removed using the password.
   * @throws IOException 
   */
  private void test_038_RemoveAccountWithPasswordCheckAccountAfterRemove(String accountToRemoveWithPassword) throws IOException {
    //CHECKED FOR VALIDITY
    try {
      client.lookupGuid(accountToRemoveWithPassword);
      failWithStackTrace("lookupGuid for " + accountToRemoveWithPassword + " should have throw an exception.");
    } catch (ClientException e) {
    		//This exception is expected.
    } 
  }

  /**
   * Look up a primary guid.
   */
  @Test
  public void test_040_LookupPrimaryGuid() {
    //CHECKED FOR VALIDITY
    String testGuidName = "testGUID" + RandomString.randomString(12);
    GuidEntry testGuid = null;
    try {
      testGuid = client.guidCreate(masterGuid, testGuidName);
    } catch (Exception e) {
      failWithStackTrace("Exception while creating testGuid: ", e);
    }
    
    try {
      Assert.assertEquals(masterGuid.getGuid(),
              client.lookupPrimaryGuid(testGuid.getGuid()));
    } catch (Exception e) {
      failWithStackTrace("Exception while looking up primary guid for testGuid: ", e);
    }
  }
  /**
   * Runs the Field tests as one independent unit.
   * @throws Exception 
   */
  @Test
  public void test_041_FieldTests() throws Exception{
	  GuidEntry subGuidEntry = test_050_CreateSubGuid();
	  test_060_FieldNotFoundException(subGuidEntry);
	  test_070_FieldExistsFalse(subGuidEntry);
	  test_080_CreateFieldForFieldExists(subGuidEntry);
	  test_090_FieldExistsTrue(subGuidEntry);
	  
	  
  }

  /**
   * Create a sub guid.
   * @throws Exception 
   */
  private GuidEntry test_050_CreateSubGuid() throws Exception {
    //CHECKED FOR VALIDITY
      GuidEntry subGuidEntry = client.guidCreate(masterGuid, "subGuid"
              + RandomString.randomString(12));
      System.out.print("Created: " + subGuidEntry);
      return subGuidEntry;
  }

  /**
   * Check field not found exception.
   * @throws Exception 
   */
  private void test_060_FieldNotFoundException(GuidEntry subGuidEntry) throws Exception {
    //CHECKED FOR VALIDITY
    try {
      client.fieldReadArrayFirstElement(subGuidEntry.getGuid(),
              "environment", subGuidEntry);
      failWithStackTrace("Should have thrown an exception.");
    } catch (FieldNotFoundException e) {
      //This is expected.
    }
  }

  /**
   * Test fieldExists.
   * @throws Exception 
   */
  private void test_070_FieldExistsFalse(GuidEntry subGuidEntry) throws Exception {
    //CHECKED FOR VALIDITY
    try {
       Assert.assertFalse(client.fieldExists(subGuidEntry.getGuid(),
              "environment", subGuidEntry));
    } catch (ClientException e) {
      // System.out.println("This was expected: " , e);
    }
  }

  /**
   * Create a field for fieldExists.
   * @throws IOException 
   * @throws ClientException 
   */
  private void test_080_CreateFieldForFieldExists(GuidEntry subGuidEntry) throws ClientException, IOException {
	  //CHECKED FOR VALIDITY
	  client.fieldCreateOneElementList(subGuidEntry.getGuid(),
			  "environment", "work", subGuidEntry);
  }

  /**
   * Create a field for fieldExists true.
   * @throws Exception 
   */
  private void test_090_FieldExistsTrue(GuidEntry subGuidEntry) throws Exception {
	  //CHECKED FOR VALIDITY
	  Assert.assertTrue(client.fieldExists(subGuidEntry.getGuid(),
			  "environment", subGuidEntry));
  }

  
  /**
   * Runs the set of ACL tests for checking All Fields Access as an independent unit.
 * @throws Exception 
 * @throws JSONException 
   */
  @Test
  public void test_100_ACLTest_All_Fields() throws JSONException, Exception{
	  final String TEST_FIELD_NAME = "testField";
	  String testFieldName = TEST_FIELD_NAME + RandomString.randomString(6);
	  test_101_ACLCreateField(testFieldName);
	  test_110_ACLMaybeAddAllFields();
	  test_111_ACLCheckForAllFieldsPass();
	  test_112_ACLRemoveAllFields();
	  test_113_ACLCheckForAllFieldsMissing();
	  test_114_CheckAllFieldsAcl();
	  test_115_DeleteAllFieldsAcl();
	  test_116_CheckAllFieldsAclGone();	  
  }
  
  


  /**
 * @throws IOException 
 * @throws ClientException 
   *
   */
  private void test_101_ACLCreateField(String testFieldName) throws ClientException, IOException {
      //CHECKED FOR VALIDITY
      client.fieldCreateOneElementList(masterGuid.getGuid(), testFieldName, "testValue", masterGuid);
  }

  //
  // Start with some simple tests to insure that basic ACL mechanics work
  //
  /**
   * Add the ALL_GUID to GNSProtocol.ENTIRE_RECORD.toString() if it's not there already.
 * @throws Exception 
 * @throws JSONException 
   */
  private void test_110_ACLMaybeAddAllFields() throws JSONException, Exception {
    //CHECKED FOR VALIDITY
      if (!JSONUtils.JSONArrayToArrayList(client.aclGet(AclAccessType.READ_WHITELIST, masterGuid,
                      GNSProtocol.ENTIRE_RECORD.toString(), masterGuid.getGuid()))
              .contains(GNSProtocol.ALL_GUIDS.toString())) {
        client.aclAdd(AclAccessType.READ_WHITELIST, masterGuid,
                GNSProtocol.ENTIRE_RECORD.toString(),
                GNSProtocol.ALL_GUIDS.toString());
      }
  }

  /**
 * @throws Exception 
 * @throws JSONException 
   *
   */
  private void test_111_ACLCheckForAllFieldsPass() throws JSONException, Exception {
    //CHECKED FOR VALIDITY
      ThreadUtils.sleep(100);
      JSONArray expected = new JSONArray(Arrays.asList(GNSProtocol.ALL_GUIDS.toString()));
      JSONAssert.assertEquals(expected,
              client.aclGet(AclAccessType.READ_WHITELIST, masterGuid,
                      GNSProtocol.ENTIRE_RECORD.toString(), masterGuid.getGuid()), true);
  }

  private void test_112_ACLRemoveAllFields() throws Exception {
    //CHECKED FOR VALIDITY
      // remove default read access for this test
      client.aclRemove(AclAccessType.READ_WHITELIST, masterGuid,
              GNSProtocol.ENTIRE_RECORD.toString(), GNSProtocol.ALL_GUIDS.toString());
  }

  private void test_113_ACLCheckForAllFieldsMissing() throws JSONException, Exception {
    //CHECKED FOR VALIDITY
      JSONArray expected = new JSONArray();
      JSONAssert.assertEquals(expected,
              client.aclGet(AclAccessType.READ_WHITELIST, masterGuid,
                      GNSProtocol.ENTIRE_RECORD.toString(), masterGuid.getGuid()), true);
  }

  private void test_114_CheckAllFieldsAcl() throws Exception {
    //CHECKED FOR VALIDITY
       Assert.assertTrue(client.fieldAclExists(AclAccessType.READ_WHITELIST, masterGuid, 
               GNSProtocol.ENTIRE_RECORD.toString()));
  }

  private void test_115_DeleteAllFieldsAcl() throws Exception {
    //CHECKED FOR VALIDITY
      client.fieldDeleteAcl(AclAccessType.READ_WHITELIST, masterGuid, GNSProtocol.ENTIRE_RECORD.toString());
  }

  private void test_116_CheckAllFieldsAclGone() throws Exception {
    //CHECKED FOR VALIDITY
       Assert.assertFalse(client.fieldAclExists(AclAccessType.READ_WHITELIST, masterGuid, GNSProtocol.ENTIRE_RECORD.toString()));
  }

  /**
   * Runs the set of ACL tests for checking single field access as an independent unit.
 * @throws Exception 
 * @throws JSONException 
   */
  @Test
  public void test_117_ACLTest_Single_Field() throws JSONException, Exception{
	  final String TEST_FIELD_NAME = "testField";
	  String testFieldName = TEST_FIELD_NAME + RandomString.randomString(6);
	  test_101_ACLCreateField(testFieldName);
	  test_120_CreateAcl(testFieldName);
	  test_121_CheckAcl(testFieldName);
	  test_122_DeleteAcl(testFieldName);
	  test_123_CheckAclGone(testFieldName);
	  
	  
  }
  
  private void test_120_CreateAcl(String testFieldName) throws Exception {
    //CHECKED FOR VALIDITY
      client.fieldCreateAcl(AclAccessType.READ_WHITELIST, masterGuid, testFieldName);
  }

  private void test_121_CheckAcl(String testFieldName) throws Exception {
    //CHECKED FOR VALIDITY
       Assert.assertTrue(client.fieldAclExists(AclAccessType.READ_WHITELIST, masterGuid, testFieldName));
  }

  private void test_122_DeleteAcl(String testFieldName) throws Exception {
    //CHECKED FOR VALIDITY
      client.fieldDeleteAcl(AclAccessType.READ_WHITELIST, masterGuid, testFieldName);
  }

  private void test_123_CheckAclGone(String testFieldName) throws Exception {
    //CHECKED FOR VALIDITY
       Assert.assertFalse(client.fieldAclExists(AclAccessType.READ_WHITELIST, masterGuid, testFieldName));
  }

  /**
   * Runs a set of ACL tests checking if others can read or not ACLS set by another.
 * @throws Exception 
 * @throws JSONException 
   */
  @Test
  public void test_124_ACLTests_OtherUser() throws JSONException, Exception{
	  GuidEntry westyEntry = GuidUtils.lookupOrCreateGuid(client, masterGuid, "westy124" + RandomString.randomString(6));
	  GuidEntry samEntry = GuidUtils.lookupOrCreateGuid(client, masterGuid, "sam124" + RandomString.randomString(6));
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
 * @throws Exception 
 * @throws JSONException 
   */
  @Test
  public void test_125_ACLTests_OtherUser2() throws JSONException, Exception{
	  GuidEntry westyEntry = GuidUtils.lookupOrCreateGuid(client, masterGuid, "westy125" + RandomString.randomString(6));
	  GuidEntry samEntry = GuidUtils.lookupOrCreateGuid(client, masterGuid, "sam125" + RandomString.randomString(6));
	  GuidEntry barneyEntry = GuidUtils.lookupOrCreateGuid(client, masterGuid, "barney125" + RandomString.randomString(6));

	  test_143_ACLAdjustACL(barneyEntry);
	  test_144_ACLCreateFields(barneyEntry);
	  test_145_ACLUpdateACL(barneyEntry);
	  test_146_ACLTestReadsOne(barneyEntry,  samEntry);
	  test_147_ACLTestReadsTwo(barneyEntry, westyEntry);
	  test_148_ACLTestReadsThree(barneyEntry, samEntry);
	  test_149_ACLALLFields(barneyEntry);
	  
	  
  }
  
  /**
   * Runs a set of ACL tests checking nested fields.
 * @throws Exception 
 * @throws JSONException 
   */
  @Test
  public void test_126_ACLTests_DeeperFields() throws JSONException, Exception{
	  GuidEntry westyEntry = GuidUtils.lookupOrCreateGuid(client, masterGuid, "westy126" + RandomString.randomString(6));
	  GuidEntry samEntry = GuidUtils.lookupOrCreateGuid(client, masterGuid, "sam126" + RandomString.randomString(6));
	  
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
  

  public void test_131_ACLRemoveAllFields(GuidEntry westyEntry, GuidEntry samEntry) {
    //CHECKED FOR VALIDITY
    try {
      // remove default read access for this test
      client.aclRemove(AclAccessType.READ_WHITELIST, westyEntry,
              GNSProtocol.ENTIRE_RECORD.toString(), GNSProtocol.ALL_GUIDS.toString());
      client.aclRemove(AclAccessType.READ_WHITELIST, samEntry,
              GNSProtocol.ENTIRE_RECORD.toString(), GNSProtocol.ALL_GUIDS.toString());
    } catch (Exception e) {
      failWithStackTrace("Exception while removing ACL in ACLRemoveAllFields: ", e);
    }
  }

  /**
   *
   */
  public void test_132_ACLCreateFields(GuidEntry westyEntry) {
    //CHECKED FOR VALIDITY
    try {
      client.fieldUpdate(westyEntry.getGuid(), "environment", "work", westyEntry);
      client.fieldUpdate(westyEntry.getGuid(), "ssn", "000-00-0000", westyEntry);
      client.fieldUpdate(westyEntry.getGuid(), "password", "666flapJack", westyEntry);
      client.fieldUpdate(westyEntry.getGuid(), "address", "100 Hinkledinkle Drive", westyEntry);
    } catch (Exception e) {
      failWithStackTrace("Exception while creating fields in ACLCreateFields: ", e);
    }
  }

  public void test_135_ACLMaybeAddAllFieldsForMaster(GuidEntry westyEntry) {
    //CHECKED FOR VALIDITY
    try {
      if (!JSONUtils.JSONArrayToArrayList(client.aclGet(AclAccessType.READ_WHITELIST, westyEntry,
                      GNSProtocol.ENTIRE_RECORD.toString(), westyEntry.getGuid()))
              .contains(masterGuid.getGuid())) {
        client.aclAdd(AclAccessType.READ_WHITELIST, westyEntry,
                GNSProtocol.ENTIRE_RECORD.toString(),
                masterGuid.getGuid());
      }
    } catch (Exception e) {
      failWithStackTrace("Exception while checking for ALL_FIELDS in ACLMaybeAddAllFields: ", e);
    }
  }

  /**
   *
   */
  public void test_136_ACLMasterReadAllFields(GuidEntry westyEntry) {
    //CHECKED FOR VALIDITY
    try {
      JSONObject expected = new JSONObject();
      expected.put("environment", "work");
      expected.put("password", "666flapJack");
      expected.put("ssn", "000-00-0000");
      expected.put("address", "100 Hinkledinkle Drive");
      JSONObject actual = new JSONObject(client.fieldRead(westyEntry.getGuid(),
              GNSProtocol.ENTIRE_RECORD.toString(), masterGuid));
      JSONAssert.assertEquals(expected, actual, true);
    } catch (Exception e) {
      failWithStackTrace("Exception while reading all fields in ACLReadAllFields: ", e);
    }
  }

  /**
   *
   */
  public void test_137_ACLReadMyFields(GuidEntry westyEntry) {
    //CHECKED FOR VALIDITY
    try {
      // read my own field
      Assert.assertEquals("work",
              client.fieldRead(westyEntry.getGuid(), "environment", westyEntry));
      // read another one of my fields field
      Assert.assertEquals("000-00-0000",
              client.fieldRead(westyEntry.getGuid(), "ssn", westyEntry));

    } catch (Exception e) {
      failWithStackTrace("Exception while reading fields in ACLReadMyFields: ", e);
    }
  }

  /**
   *
   */
  public void test_138_ACLNotReadOtherGuidAllFieldsTest(GuidEntry westyEntry, GuidEntry samEntry) {
    //CHECKED FOR VALIDITY
    try {
      try {
        String result = client.fieldRead(westyEntry.getGuid(), GNSProtocol.ENTIRE_RECORD.toString(), samEntry);
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
   */
  public void test_139_ACLNotReadOtherGuidFieldTest(GuidEntry westyEntry, GuidEntry samEntry) {
    //CHECKED FOR VALIDITY
    try {
      try {
        String result = client.fieldRead(westyEntry.getGuid(), "environment",
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
   */
  public void test_140_AddACLTest(GuidEntry westyEntry, GuidEntry samEntry) {
    //CHECKED FOR VALIDITY
    try {
      try {
        client.aclAdd(AclAccessType.READ_WHITELIST, westyEntry, "environment", samEntry.getGuid());
      } catch (Exception e) {
        failWithStackTrace("Exception adding Sam to Westy's readlist: ", e);
      }
    } catch (Exception e) {
      failWithStackTrace("Exception when we were not expecting it in ACLPartOne: ", e);
    }
  }

  public void test_141_CheckACLTest(GuidEntry westyEntry, GuidEntry samEntry) {
    //CHECKED FOR VALIDITY
    try {
      try {
        Assert.assertEquals("work", client.fieldRead(westyEntry.getGuid(), "environment", samEntry));
      } catch (Exception e) {
        failWithStackTrace("Exception while Sam reading Westy's field: ", e);
      }
    } catch (Exception e) {
      failWithStackTrace("Exception when we were not expecting it in ACLPartOne: ", e);
    }
  }


  public void test_143_ACLAdjustACL(GuidEntry barneyEntry) {
    //CHECKED FOR VALIDITY
    try {
      // remove default read access for this test
      client.aclRemove(AclAccessType.READ_WHITELIST, barneyEntry,
              GNSProtocol.ENTIRE_RECORD.toString(), GNSProtocol.ALL_GUIDS.toString());
    } catch (Exception e) {
      failWithStackTrace("Exception when we were not expecting it in ACLPartTwo: ", e);
    }
  }

  public void test_144_ACLCreateFields(GuidEntry barneyEntry) {
    //CHECKED FOR VALIDITY
    try {
      // remove default read access for this test
      client.fieldUpdate(barneyEntry.getGuid(), "cell", "413-555-1234", barneyEntry);
      client.fieldUpdate(barneyEntry.getGuid(), "address", "100 Main Street", barneyEntry);
    } catch (Exception e) {
      failWithStackTrace("Exception when we were not expecting it in ACLPartTwo: ", e);
    }
  }

  public void test_145_ACLUpdateACL(GuidEntry barneyEntry) {
    //CHECKED FOR VALIDITY
    try {
      try {
        // let anybody read barney's cell field
        client.aclAdd(AclAccessType.READ_WHITELIST, barneyEntry, "cell",
                GNSProtocol.ALL_GUIDS.toString());
      } catch (Exception e) {
        failWithStackTrace("Exception creating ALL_GUIDS access for Barney's cell: ", e);
      }
    } catch (Exception e) {
      failWithStackTrace("Exception when we were not expecting it in ACLPartTwo: ", e);
    }
  }

  public void test_146_ACLTestReadsOne(GuidEntry barneyEntry, GuidEntry samEntry) {
    //CHECKED FOR VALIDITY
    try {
      try {
        Assert.assertEquals("413-555-1234",
                client.fieldRead(barneyEntry.getGuid(), "cell", samEntry));
      } catch (Exception e) {
        failWithStackTrace("Exception while Sam reading Barney' cell: ", e);
      }
    } catch (Exception e) {
      failWithStackTrace("Exception when we were not expecting it in ACLPartOne: ", e);
    }
  }

  public void test_147_ACLTestReadsTwo(GuidEntry barneyEntry, GuidEntry westyEntry) {
    //CHECKED FOR VALIDITY
    try {
      try {
        Assert.assertEquals("413-555-1234",
                client.fieldRead(barneyEntry.getGuid(), "cell", westyEntry));
      } catch (Exception e) {
        failWithStackTrace("Exception while Westy reading Barney' cell: ", e);
      }
    } catch (Exception e) {
      failWithStackTrace("Exception when we were not expecting it in ACLTestReadsTwo: ", e);
    }
  }

  public void test_148_ACLTestReadsThree(GuidEntry barneyEntry, GuidEntry samEntry) {
    //CHECKED FOR VALIDITY
    try {
      try {
        String result = client.fieldRead(barneyEntry.getGuid(), "address",
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

  public void test_149_ACLALLFields(GuidEntry barneyEntry) {
    //CHECKED FOR VALIDITY
    String superUserName = "superuser" + RandomString.randomString(6);
    try {
      try {
        client.lookupGuid(superUserName);
        failWithStackTrace(superUserName + " entity should not exist");
      } catch (ClientException e) {
      }

      GuidEntry superuserEntry = client.guidCreate(masterGuid, superUserName);

      // let superuser read any of barney's fields
      client.aclAdd(AclAccessType.READ_WHITELIST, barneyEntry,
              GNSProtocol.ENTIRE_RECORD.toString(), superuserEntry.getGuid());

      Assert.assertEquals("413-555-1234",
              client.fieldRead(barneyEntry.getGuid(), "cell", superuserEntry));
      Assert.assertEquals("100 Main Street",
              client.fieldRead(barneyEntry.getGuid(), "address", superuserEntry));

    } catch (Exception e) {
      failWithStackTrace("Exception when we were not expecting it in ACLALLFields: ", e);
    }
  }

  public void test_150_ACLCreateDeeperField(GuidEntry westyEntry) {
    //CHECKED FOR VALIDITY
    try {
      try {
        client.fieldUpdate(westyEntry.getGuid(), "test.deeper.field", "fieldValue", westyEntry);
      } catch (Exception e) {
        failWithStackTrace("Problem updating field: ", e);
      }
    } catch (Exception e) {
      failWithStackTrace("Exception when we were not expecting it ACLCreateDeeperField: ", e);
    }
  }

  public void test_151_ACLAddDeeperFieldACL(GuidEntry westyEntry) {
    //CHECKED FOR VALIDITY
    try {
      try {
        // Create an empty ACL, effectively disabling access except by the guid itself.
        client.fieldCreateAcl(AclAccessType.READ_WHITELIST, westyEntry, "test.deeper.field");
      } catch (Exception e) {
        failWithStackTrace("Problem adding acl: ", e);
      }
    } catch (Exception e) {
      failWithStackTrace("Exception when we were not expecting it ACLCreateDeeperField: ", e);
    }
  }

  public void test_152_ACLCheckDeeperFieldACLExists(GuidEntry westyEntry) {
    //CHECKED FOR VALIDITY
    try {
      try {
         Assert.assertTrue(client.fieldAclExists(AclAccessType.READ_WHITELIST, westyEntry, "test.deeper.field"));
      } catch (Exception e) {
        failWithStackTrace("Problem reading acl: ", e);
      }
    } catch (Exception e) {
      failWithStackTrace("Exception when we were not expecting it ACLCreateDeeperField: ", e);
    }
  }

  // This should pass even though the ACL for test.deeper.field is empty because you
  // can always read your own fields.
  public void test_153_ACLReadDeeperFieldSelf(GuidEntry westyEntry) {
    //CHECKED FOR VALIDITY
    try {
      try {
        Assert.assertEquals("fieldValue", client.fieldRead(westyEntry.getGuid(), "test.deeper.field", westyEntry));
      } catch (Exception e) {
        failWithStackTrace("Problem adding read field: ", e);
      }
    } catch (Exception e) {
      failWithStackTrace("Exception when we were not expecting it ACLCreateDeeperField: ", e);
    }
  }

  // This should fail because the ACL for test.deeper.field is empty.
  public void test_154_ACLReadDeeperFieldOtherFail(GuidEntry westyEntry, GuidEntry samEntry) {
    //CHECKED FOR VALIDITY
    try {
      try {
        Assert.assertEquals("fieldValue", client.fieldRead(westyEntry.getGuid(), "test.deeper.field", samEntry));
        failWithStackTrace("This read should have failed.");
      } catch (Exception e) {
      }
    } catch (Exception e) {
      failWithStackTrace("Exception when we were not expecting it ACLCreateDeeperField: ", e);
    }
  }

  // This should fail because the ACL for test.deeper.field is empty.
  public void test_156_ACLReadShallowFieldOtherFail(GuidEntry westyEntry, GuidEntry samEntry) {
    //CHECKED FOR VALIDITY
    try {
      try {
        Assert.assertEquals("fieldValue", client.fieldRead(westyEntry.getGuid(), "test.deeper", samEntry));
        failWithStackTrace("This read should have failed.");
      } catch (Exception e) {
      }
    } catch (Exception e) {
      failWithStackTrace("Exception when we were not expecting it ACLCreateDeeperField: ", e);
    }
  }

  public void test_157_AddAllRecordACL(GuidEntry westyEntry) {
    //CHECKED FOR VALIDITY
    try {
      client.aclAdd(AclAccessType.READ_WHITELIST, westyEntry, "test", GNSProtocol.ALL_GUIDS.toString());
    } catch (Exception e) {
      failWithStackTrace("Exception when we were not expecting it ACLCreateDeeperField: ", e);
    }
  }

  // This should still fail because the ACL for test.deeper.field is empty even though test 
  // now has an GNSProtocol.ALL_GUIDS.toString() at the root (this is different than the old model).
  public void test_158_ACLReadDeeperFieldOtherFail(GuidEntry westyEntry, GuidEntry samEntry) {
    //CHECKED FOR VALIDITY
    try {
      try {
        Assert.assertEquals("fieldValue", client.fieldRead(westyEntry.getGuid(), "test.deeper.field", samEntry));
        failWithStackTrace("This read should have failed.");
      } catch (Exception e) {
      }
    } catch (Exception e) {
      failWithStackTrace("Exception when we were not expecting it ACLCreateDeeperField: ", e);
    }
  }

  
  /**
   * Runs a set of DB tests.
 * @throws Exception 
 * @throws JSONException 
   */
  @Test
  public void test_160_DBTests() throws JSONException, Exception{
	  GuidEntry westyEntry = GuidUtils.lookupOrCreateGuid(client, masterGuid, "westy160" + RandomString.randomString(6));
	  
	  test_170_DB(westyEntry);
	  test_180_DBUpserts(westyEntry);
	  
	  
  }
  
  /**
   * Tests a bunch of different access methods.
   */
  public void test_170_DB(GuidEntry westyEntry) {
    //CHECKED FOR VALIDITY
    try {

      client.fieldCreateOneElementList(westyEntry.getGuid(), "cats",
              "whacky", westyEntry);

      
      Assert.assertEquals("whacky", client.fieldReadArrayFirstElement(
              westyEntry.getGuid(), "cats", westyEntry));

      client.fieldAppendWithSetSemantics(
              westyEntry.getGuid(),
              "cats",
              new JSONArray(Arrays.asList("hooch", "maya", "red", "sox",
                      "toby")), westyEntry);

      
      HashSet<String> expected = new HashSet<String>(Arrays.asList(
              "hooch", "maya", "red", "sox", "toby", "whacky"));
      HashSet<String> actual = JSONUtils.JSONArrayToHashSet(client
              .fieldReadArray(westyEntry.getGuid(), "cats", westyEntry));
      Assert.assertEquals(expected, actual);

      client.fieldClear(westyEntry.getGuid(), "cats", new JSONArray(
              Arrays.asList("maya", "toby")), westyEntry);
      
      expected = new HashSet<String>(Arrays.asList("hooch", "red", "sox",
              "whacky"));
      actual = JSONUtils.JSONArrayToHashSet(client.fieldReadArray(
              westyEntry.getGuid(), "cats", westyEntry));
      Assert.assertEquals(expected, actual);

      client.fieldReplaceFirstElement(westyEntry.getGuid(), "cats",
              "maya", westyEntry);
      
      Assert.assertEquals("maya", client.fieldReadArrayFirstElement(
              westyEntry.getGuid(), "cats", westyEntry));
      client.fieldAppendWithSetSemantics(westyEntry.getGuid(), "cats",
              "fred", westyEntry);
      
      expected = new HashSet<String>(Arrays.asList("maya", "fred"));
      actual = JSONUtils.JSONArrayToHashSet(client.fieldReadArray(
              westyEntry.getGuid(), "cats", westyEntry));
      Assert.assertEquals(expected, actual);

      client.fieldAppendWithSetSemantics(westyEntry.getGuid(), "cats",
              "fred", westyEntry);
      
      expected = new HashSet<String>(Arrays.asList("maya", "fred"));
      actual = JSONUtils.JSONArrayToHashSet(client.fieldReadArray(
              westyEntry.getGuid(), "cats", westyEntry));
      Assert.assertEquals(expected, actual);
    } catch (Exception e) {
      failWithStackTrace("Exception when we were not expecting testing DB: ", e);
    }
  }

  /**
   * Tests a bunch of different DB upsert methods.
   */
  public void test_180_DBUpserts(GuidEntry westyEntry) {
    //CHECKED FOR VALIDITY
    HashSet<String> expected;
    HashSet<String> actual;
    try {

      client.fieldAppendOrCreate(westyEntry.getGuid(), "dogs", "bear",
              westyEntry);
      
      expected = new HashSet<String>(Arrays.asList("bear"));
      actual = JSONUtils.JSONArrayToHashSet(client.fieldReadArray(
              westyEntry.getGuid(), "dogs", westyEntry));
      Assert.assertEquals(expected, actual);

    } catch (Exception e) {
      failWithStackTrace("1) Looking for bear: ", e);
    }
    try {
      client.fieldAppendOrCreateList(westyEntry.getGuid(), "dogs",
              new JSONArray(Arrays.asList("wags", "tucker")), westyEntry);
      
      expected = new HashSet<String>(Arrays.asList("bear", "wags",
              "tucker"));
      actual = JSONUtils.JSONArrayToHashSet(client.fieldReadArray(
              westyEntry.getGuid(), "dogs", westyEntry));
      Assert.assertEquals(expected, actual);
    } catch (Exception e) {
      failWithStackTrace("2) Looking for bear, wags, tucker: ", e);
    }
    try {
      client.fieldReplaceOrCreate(westyEntry.getGuid(), "goats", "sue",
              westyEntry);
      
      expected = new HashSet<String>(Arrays.asList("sue"));
      actual = JSONUtils.JSONArrayToHashSet(client.fieldReadArray(
              westyEntry.getGuid(), "goats", westyEntry));
      Assert.assertEquals(expected, actual);
    } catch (Exception e) {
      failWithStackTrace("3) Looking for sue: ", e);
    }
    try {
      client.fieldReplaceOrCreate(westyEntry.getGuid(), "goats",
              "william", westyEntry);
      
      expected = new HashSet<String>(Arrays.asList("william"));
      actual = JSONUtils.JSONArrayToHashSet(client.fieldReadArray(
              westyEntry.getGuid(), "goats", westyEntry));
      Assert.assertEquals(expected, actual);
    } catch (Exception e) {
      failWithStackTrace("4) Looking for william: ", e);
    }
    try {
      client.fieldReplaceOrCreateList(westyEntry.getGuid(), "goats",
              new JSONArray(Arrays.asList("dink", "tink")), westyEntry);
      
      expected = new HashSet<String>(Arrays.asList("dink", "tink"));
      actual = JSONUtils.JSONArrayToHashSet(client.fieldReadArray(
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
      testEntry = client.guidCreate(masterGuid, testSubstituteGuid);
      System.out.print("created test guid: " + testEntry);
    } catch (Exception e) {
      failWithStackTrace("Exception during init: ", e);
    }
    try {
      client.fieldAppendOrCreateList(
              testEntry.getGuid(),
              field,
              new JSONArray(Arrays
                      .asList("Frank", "Joe", "Sally", "Rita")),
              testEntry);
    } catch (Exception e) {
      failWithStackTrace("Exception during create: ", e);
    }

    try {
      HashSet<String> expected = new HashSet<String>(Arrays.asList(
              "Frank", "Joe", "Sally", "Rita"));
      HashSet<String> actual = JSONUtils.JSONArrayToHashSet(client
              .fieldReadArray(testEntry.getGuid(), field, testEntry));
      Assert.assertEquals(expected, actual);
    } catch (Exception e) {
      failWithStackTrace("Exception when we were not expecting it: ", e);
    }

    try {
      client.fieldSubstitute(testEntry.getGuid(), field, "Christy",
              "Sally", testEntry);
    } catch (Exception e) {
      failWithStackTrace("Exception during substitute: ", e);
    }

    try {
      HashSet<String> expected = new HashSet<String>(Arrays.asList(
              "Frank", "Joe", "Christy", "Rita"));
      HashSet<String> actual = JSONUtils.JSONArrayToHashSet(client
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
  public void test_200_SubstituteList() {
    //CHECKED FOR VALIDITY
    String testSubstituteListGuid = "testSubstituteListGUID"
            + RandomString.randomString(12);
    String field = "people";
    GuidEntry testEntry = null;
    try {
      // Utils.clearTestGuids(client);
      // System.out.println("cleared old GUIDs");
      testEntry = client.guidCreate(masterGuid, testSubstituteListGuid);
      System.out.print("created test guid: " + testEntry);
    } catch (Exception e) {
      failWithStackTrace("Exception during init: ", e);
    }
    try {
      client.fieldAppendOrCreateList(
              testEntry.getGuid(),
              field,
              new JSONArray(Arrays
                      .asList("Frank", "Joe", "Sally", "Rita")),
              testEntry);
    } catch (Exception e) {
      failWithStackTrace("Exception during create: ", e);
    }

    try {
      HashSet<String> expected = new HashSet<String>(Arrays.asList(
              "Frank", "Joe", "Sally", "Rita"));
      HashSet<String> actual = JSONUtils.JSONArrayToHashSet(client
              .fieldReadArray(testEntry.getGuid(), field, testEntry));
      Assert.assertEquals(expected, actual);
    } catch (Exception e) {
      failWithStackTrace("Exception when we were not expecting it: ", e);
    }

    try {
      client.fieldSubstitute(testEntry.getGuid(), field, new JSONArray(
              Arrays.asList("BillyBob", "Hank")),
              new JSONArray(Arrays.asList("Frank", "Joe")), testEntry);
    } catch (Exception e) {
      failWithStackTrace("Exception during substitute: ", e);
    }

    try {
      HashSet<String> expected = new HashSet<String>(Arrays.asList(
              "BillyBob", "Hank", "Sally", "Rita"));
      HashSet<String> actual = JSONUtils.JSONArrayToHashSet(client
              .fieldReadArray(testEntry.getGuid(), field, testEntry));
      Assert.assertEquals(expected, actual);
    } catch (Exception e) {
      failWithStackTrace("Exception when we were not expecting it: ", e);
    }
  }
  
  
  /**
   * A set of tests for checking Group functionality.
 * @throws Exception 
   */
  @Test
  public void test_210_GroupTests() throws Exception {
	  GuidEntry westyEntry = GuidUtils.lookupOrCreateGuid(client, masterGuid, "westy210" + RandomString.randomString(6));
	  GuidEntry samEntry = GuidUtils.lookupOrCreateGuid(client, masterGuid, "sam210" + RandomString.randomString(6));
	  GuidEntry mygroupEntry = test_210_GroupCreate();
	  test_211_GroupAdd(westyEntry, samEntry, mygroupEntry);
	  test_212_GroupRemoveGuid();
	  
	  GuidEntry groupAccessUserEntry = test_220_GroupAndACLCreateGuids(mygroupEntry);
	  test_221_GroupAndACLTestBadAccess(groupAccessUserEntry, westyEntry);
	  test_222_GroupAndACLTestGoodAccess(groupAccessUserEntry, westyEntry);
	  test_223_GroupAndACLTestRemoveGuid(westyEntry, mygroupEntry);
	  test_224_GroupAndACLTestRemoveGuidCheck(samEntry, mygroupEntry);
  }


  
  
  /**
   * Create a group to test.
 * @throws Exception 
   */
  public GuidEntry test_210_GroupCreate() throws Exception {
    //CHECKED FOR VALIDITY
    String mygroupName = "mygroup" + RandomString.randomString(12);
      try {
        client.lookupGuid(mygroupName);
        failWithStackTrace(mygroupName + " entity should not exist");
      } catch (ClientException e) {
    	  //Expected
      }
      guidToDeleteEntry = client.guidCreate(masterGuid, "deleteMe"
              + RandomString.randomString(12));
      GuidEntry mygroupEntry = client.guidCreate(masterGuid, mygroupName);
      return mygroupEntry;
  }

  /**
   * Add guids to a group.
   */
  public void test_211_GroupAdd(GuidEntry westyEntry, GuidEntry samEntry, GuidEntry mygroupEntry) {
    //CHECKED FOR VALIDITY
    try {
      client.groupAddGuid(mygroupEntry.getGuid(), westyEntry.getGuid(),
              mygroupEntry);
      client.groupAddGuid(mygroupEntry.getGuid(), samEntry.getGuid(),
              mygroupEntry);
      client.groupAddGuid(mygroupEntry.getGuid(),
              guidToDeleteEntry.getGuid(), mygroupEntry);
    } catch (Exception e) {
      failWithStackTrace("Exception while adding to groups: ", e);
    }
    try {
      HashSet<String> expected = new HashSet<String>(Arrays.asList(
              westyEntry.getGuid(), samEntry.getGuid(),
              guidToDeleteEntry.getGuid()));
      HashSet<String> actual = JSONUtils.JSONArrayToHashSet(client
              .groupGetMembers(mygroupEntry.getGuid(), mygroupEntry));
      Assert.assertEquals(expected, actual);

      expected = new HashSet<String>(
              Arrays.asList(mygroupEntry.getGuid()));
      actual = JSONUtils.JSONArrayToHashSet(client.guidGetGroups(
              westyEntry.getGuid(), westyEntry));
      Assert.assertEquals(expected, actual);

    } catch (Exception e) {
      failWithStackTrace("Exception while getting members and groups: ", e);
    }
  }

  /**
   * Remove a guid from a group.
   */
  public void test_212_GroupRemoveGuid() {
    //CHECKED FOR VALIDITY
    // now remove a guid and check for group updates
    try {
      client.guidRemove(masterGuid, guidToDeleteEntry.getGuid());
    } catch (Exception e) {
      failWithStackTrace("Exception while removing testGuid: ", e);
    }
    try {
      client.lookupGuidRecord(guidToDeleteEntry.getGuid());
      failWithStackTrace("Lookup testGuid should have throw an exception.");
    } catch (ClientException e) {

    } catch (IOException e) {
      failWithStackTrace("Exception while doing Lookup testGuid: ", e);
    }
  }


  /**
   * Update some ACLs for group tests.
 * @throws Exception 
   */
  public GuidEntry test_220_GroupAndACLCreateGuids(GuidEntry mygroupEntry) throws Exception {
    //CHECKED FOR VALIDITY
    // testGroup();
    String groupAccessUserName = "groupAccessUser"
            + RandomString.randomString(12);
    GuidEntry groupAccessUserEntry;
      try {
        client.lookupGuid(groupAccessUserName);
        failWithStackTrace(groupAccessUserName + " entity should not exist");
      } catch (ClientException e) {
    	  //Expected
      }
      groupAccessUserEntry = client.guidCreate(masterGuid,
              groupAccessUserName);
      // remove all fields read by all
      client.aclRemove(AclAccessType.READ_WHITELIST,
              groupAccessUserEntry, GNSProtocol.ENTIRE_RECORD.toString(),
              GNSProtocol.ALL_GUIDS.toString());

      // test of remove all fields read by all
      JSONAssert.assertEquals(new JSONArray(Arrays.asList(masterGuid.getGuid())),
              client.aclGet(AclAccessType.READ_WHITELIST, groupAccessUserEntry,
                      GNSProtocol.ENTIRE_RECORD.toString(), groupAccessUserEntry.getGuid()),
              JSONCompareMode.STRICT);

      client.fieldCreateOneElementList(groupAccessUserEntry.getGuid(),
              "address", "23 Jumper Road", groupAccessUserEntry);
      client.fieldCreateOneElementList(groupAccessUserEntry.getGuid(),
              "age", "43", groupAccessUserEntry);
      client.fieldCreateOneElementList(groupAccessUserEntry.getGuid(),
              "hometown", "whoville", groupAccessUserEntry);
      
      client.aclAdd(AclAccessType.READ_WHITELIST, groupAccessUserEntry,
              "hometown", mygroupEntry.getGuid());
      
      return groupAccessUserEntry;
  }

  /**
   * Test a a group access that should be denied.
   */
  public void test_221_GroupAndACLTestBadAccess(GuidEntry groupAccessUserEntry, GuidEntry westyEntry) {
    //CHECKED FOR VALIDITY
    try {
      try {
        String result = client.fieldReadArrayFirstElement(
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
   */
  public void test_222_GroupAndACLTestGoodAccess(GuidEntry groupAccessUserEntry, GuidEntry westyEntry) {
    //CHECKED FOR VALIDITY
    try {
      Assert.assertEquals("whoville", client.fieldReadArrayFirstElement(
              groupAccessUserEntry.getGuid(), "hometown", westyEntry));
    } catch (Exception e) {
      failWithStackTrace("Exception while attempting read of groupAccessUser's hometown by westy: ",
              e);
    }
  }

  /**
   * Remove a guid and test that it was.
   */
  public void test_223_GroupAndACLTestRemoveGuid(GuidEntry westyEntry, GuidEntry mygroupEntry) {
    //CHECKED FOR VALIDITY
    try {
      try {
        client.groupRemoveGuid(mygroupEntry.getGuid(),
                westyEntry.getGuid(), mygroupEntry);
      } catch (Exception e) {
        failWithStackTrace("Exception removing westy from mygroup: ", e);
      }
    } catch (Exception e) {
      failWithStackTrace("Exception when we were not expecting it: ", e);
    }

  }

  public void test_224_GroupAndACLTestRemoveGuidCheck(GuidEntry samEntry, GuidEntry mygroupEntry) {
    HashSet<String> expected = new HashSet<String>(
            Arrays.asList(samEntry.getGuid()));
    HashSet<String> actual = null;
    int count = 50; // try 10 times or so
    do {
      try {
        if (count != 50) {
        Thread.sleep(100);
      }
        actual = JSONUtils.JSONArrayToHashSet(client.groupGetMembers(mygroupEntry.getGuid(), mygroupEntry));
      } catch (ClientException | IOException | JSONException | InterruptedException e) {
      }
    } while (count-- > 0 && (actual == null || !actual.equals(expected)));
    if (count < 49) {
      System.out.println("Waited " + (49 - count) + " times");
    }
    Assert.assertEquals(expected, actual);
  }

  
  /**
   * Runs a set of Alias tests.
   */
  @Test
  public void test_229_AliasTests(){
	  String alias = "ALIAS-" + RandomString.randomString(6) + "@blah.org";
	  test_230_AliasAdd(alias);
	  test_231_AliasIsPresent(alias);
	  test_232_AliasCheckRemove(alias); 
  }

  /**
   * Add an alias.
   */
  public void test_230_AliasAdd(String alias) {
    //CHECKED FOR VALIDITY
    try {
      // KEEP IN MIND THAT CURRENTLY ONLY ACCOUNT GUIDS HAVE ALIASES
      // add an alias to the masterGuid
      client.addAlias(masterGuid, alias);
      // lookup the guid using the alias
      Assert.assertEquals(masterGuid.getGuid(), client.lookupGuid(alias));
    } catch (Exception e) {
      failWithStackTrace("Exception while adding alias: ", e);
    }
  }

  /**
   * Test that recently added alias is present.
   */
  public void test_231_AliasIsPresent(String alias) {
    //CHECKED FOR VALIDITY
    try {
      // grab all the alias from the guid
      HashSet<String> actual = JSONUtils.JSONArrayToHashSet(client
              .getAliases(masterGuid));

      /* arun: This test has no reason to succeed because getAliases is
			 * not coordinated or forceCoordinateable.
       */
      // make sure our new one is in there
      Assert.assertThat(actual, hasItem(alias));
      // now remove it
      client.removeAlias(masterGuid, alias);
    } catch (Exception e) {
      failWithStackTrace("Exception removing alias: ", e);
    }
  }

  /**
   * Check that removed alias is gone.
   */
  public void test_232_AliasCheckRemove(String alias) {
    //CHECKED FOR VALIDITY
    try {
      // and make sure it is gone
      try {
        client.lookupGuid(alias);
        failWithStackTrace(alias + " should not exist");
      } catch (ClientException e) {
      }
    } catch (Exception e) {
      failWithStackTrace("Exception while checking alias: ", e);
    }
  }

  /**
   * Write access checks.
 * @throws Exception 
   */
  @Test
  public void test_240_WriteAccess() throws Exception {
	  GuidEntry westyEntry = GuidUtils.lookupOrCreateGuid(client, masterGuid, "westy240" + RandomString.randomString(6));
	  GuidEntry samEntry = GuidUtils.lookupOrCreateGuid(client, masterGuid, "sam240" + RandomString.randomString(6));
	  GuidEntry barneyEntry = GuidUtils.lookupOrCreateGuid(client, masterGuid, "barney240" + RandomString.randomString(6));
    //CHECKED FOR VALIDITY
    String fieldName = "whereAmI";
      try {
        client.aclAdd(AclAccessType.WRITE_WHITELIST, westyEntry,
                fieldName, samEntry.getGuid());
      } catch (Exception e) {
        failWithStackTrace("Exception adding Sam to Westy's writelist: ", e);
      }
      // write my own field
      try {
        client.fieldReplaceFirstElement(westyEntry.getGuid(),
                fieldName, "shopping", westyEntry);
      } catch (Exception e) {
        failWithStackTrace("Exception while Westy's writing own field: ", e);
      }
      // now check the value
      Assert.assertEquals("shopping", client.fieldReadArrayFirstElement(
              westyEntry.getGuid(), fieldName, westyEntry));
      // someone else write my field
      try {
        client.fieldReplaceFirstElement(westyEntry.getGuid(),
                fieldName, "driving", samEntry);
      } catch (Exception e) {
        failWithStackTrace("Exception while Sam writing Westy's field: ", e);
      }
      // now check the value
      Assert.assertEquals("driving", client.fieldReadArrayFirstElement(
              westyEntry.getGuid(), fieldName, westyEntry));
      // do one that should fail
      try {
        client.fieldReplaceFirstElement(westyEntry.getGuid(),
                fieldName, "driving", barneyEntry);
        failWithStackTrace("Write by barney should have failed!");
      } catch (ClientException e) {
    	  //expected
      }
  }
  
  /**
   * Runs a set of UnsignedRead tests.
 * @throws Exception 
   */
  @Test
  public void test_245_UnsignedReadTests() throws Exception{
	  GuidEntry unsignedReadAccountGuid = test_249_UnsignedReadDefaultWriteCreateAccountGuid();
	  test_250_UnsignedReadDefaultAccountGuidWrite(unsignedReadAccountGuid);
	  test_251_UnsignedReadDefaultAccountGuidRead(unsignedReadAccountGuid);
	  
	  String unsignedReadFieldName = "allreadaccess";
	  String unreadAbleReadFieldName = "cannotreadreadaccess";
	  GuidEntry unsignedReadTestGuid = test_252_UnsignedReadCreateGuids(unsignedReadAccountGuid);
	  test_253_UnsignedReadCheckACL(unsignedReadAccountGuid, unsignedReadTestGuid);
	  
	  test_254_UnsignedReadDefaultWrite(unsignedReadTestGuid, unsignedReadFieldName);
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
  
  
  public GuidEntry test_249_UnsignedReadDefaultWriteCreateAccountGuid() throws Exception {
      GuidEntry unsignedReadAccountGuid = GuidUtils.lookupOrCreateAccountGuid(client, 
              "unsignedReadAccountGuid249" + RandomString.randomString(12), PASSWORD, true);
      return unsignedReadAccountGuid;
  }
   
  
  /**
   * Tests for the default ACL list working with unsigned read.
   * Sets up the field in the account guid.
 * @throws IOException 
 * @throws ClientException 
   */
  public void test_250_UnsignedReadDefaultAccountGuidWrite(GuidEntry unsignedReadAccountGuid) throws ClientException, IOException {
      client.fieldUpdate(unsignedReadAccountGuid, "aRandomFieldForUnsignedRead", "aRandomValue");
  }

  /**
   * Tests for the default ACL list working with unsigned read.
   * Attempts to read the field.
   */
  public void test_251_UnsignedReadDefaultAccountGuidRead(GuidEntry unsignedReadAccountGuid) {
    try {
      String response = client.fieldRead(unsignedReadAccountGuid.getGuid(), "aRandomFieldForUnsignedRead", null);
      Assert.assertEquals("aRandomValue", response);
    } catch (Exception e) {
      failWithStackTrace("Exception writing field UnsignedReadDefaultMasterWrite: ", e);
    }
  }



  // Creating 4 fields


  /**
   * Create the subguid for the unsigned read tests.
 * @throws Exception 
   */
  public GuidEntry test_252_UnsignedReadCreateGuids(GuidEntry unsignedReadAccountGuid) throws Exception {
      GuidEntry unsignedReadTestGuid = client.guidCreate(unsignedReadAccountGuid, "unsignedReadTestGuid" + RandomString.randomString(12));
      System.out.println("Created: " + unsignedReadTestGuid);
    return unsignedReadTestGuid;
  }

  /**
   * Check the default ACL for the unsigned read tests.
   * The account guid and EVERYONE should be in the ENTIRE_RECORD ACL.
   */
  public void test_253_UnsignedReadCheckACL(GuidEntry unsignedReadAccountGuid, GuidEntry unsignedReadTestGuid) {
    try {
      JSONArray expected = new JSONArray(new ArrayList<>(Arrays.asList(unsignedReadAccountGuid.getGuid(),
              GNSProtocol.EVERYONE.toString())));
      JSONArray actual = client.aclGet(AclAccessType.READ_WHITELIST, unsignedReadTestGuid,
              GNSProtocol.ENTIRE_RECORD.toString(), unsignedReadTestGuid.getGuid());
      JSONAssert.assertEquals(expected, actual, false);
    } catch (Exception e) {
      failWithStackTrace("Exception while retrieving ACL in UnsignedReadCheckACL: ", e);
    }
  }

  /**
   * Write the value the unsigned read tests.
   */
  public void test_254_UnsignedReadDefaultWrite(GuidEntry unsignedReadTestGuid, String unsignedReadFieldName) {
    try {
      client.fieldUpdate(unsignedReadTestGuid.getGuid(),
              unsignedReadFieldName, "funkadelicread", unsignedReadTestGuid);
    } catch (Exception e) {
      failWithStackTrace("Exception while writing value UnsignedReadDefaultWrite: ", e);
    }
  }

  /**
   * Check the value the unsigned read tests.
   */
  public void test_255_UnsignedReadDefaultRead(GuidEntry unsignedReadTestGuid, String unsignedReadFieldName) {
    try {
      Assert.assertEquals("funkadelicread",
              client.fieldRead(unsignedReadTestGuid.getGuid(), unsignedReadFieldName, null));
    } catch (Exception e) {
      failWithStackTrace("Exception reading value in UnsignedReadDefaultRead: ", e);
    }
  }

  /**
   * Remove the default ENTIRE_RECORD read access.
   */
  public void test_256_UnsignedReadFailRemoveDefaultReadAccess(GuidEntry unsignedReadTestGuid) {
    try {
      client.aclRemove(AclAccessType.READ_WHITELIST, unsignedReadTestGuid,
              GNSProtocol.ENTIRE_RECORD.toString(), GNSProtocol.ALL_GUIDS.toString());
    } catch (Exception e) {
      failWithStackTrace("Exception removing defa in UnsignedReadDefaultRead: ", e);
    }
  }

  /**
   * Ensure that only the account guid is in the ACL.
   */
  public void test_257_UnsignedReadCheckACLForRecord(GuidEntry unsignedReadAccountGuid, GuidEntry unsignedReadTestGuid) {
    try {
      JSONArray expected = new JSONArray(new ArrayList<>(Arrays.asList(unsignedReadAccountGuid.getGuid())));
      JSONArray actual = client.aclGet(AclAccessType.READ_WHITELIST, unsignedReadTestGuid,
              GNSProtocol.ENTIRE_RECORD.toString(), unsignedReadTestGuid.getGuid());
      JSONAssert.assertEquals(expected, actual, false);
    } catch (Exception e) {
      failWithStackTrace("Exception while retrieving ACL in UnsignedReadCheckACL: ", e);
    }
  }

  /**
   * Write a value to the field we're going to try to read.
   */
  public void test_258_UnsignedReadFailWriteField(GuidEntry unsignedReadTestGuid, String unreadAbleReadFieldName) {

    try {
      client.fieldUpdate(unsignedReadTestGuid.getGuid(), unreadAbleReadFieldName, "bummer", unsignedReadTestGuid);
    } catch (Exception e) {
      failWithStackTrace("Exception while testing for denied unsigned access in UnsignedRead: ", e);
    }
  }

  /**
   * Attempt a read that should fail because ENTIRE_RECORD was removed.
   */
  public void test_259_UnsignedReadFailRead(GuidEntry unsignedReadTestGuid, String unreadAbleReadFieldName) {
    try {
      try {
        String result = client.fieldRead(unsignedReadTestGuid.getGuid(), unreadAbleReadFieldName, null);
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
   */
  public void test_260_UnsignedReadAddFieldAccess(GuidEntry unsignedReadTestGuid, String unsignedReadFieldName) {
    try {
      client.aclAdd(AclAccessType.READ_WHITELIST, unsignedReadTestGuid, unsignedReadFieldName,
              GNSProtocol.ALL_GUIDS.toString());
    } catch (Exception e) {
      failWithStackTrace("Exception adding unsigned access in UnsignedReadAddFieldAccess: ", e);
    }
  }

  /**
   * Insures that we can read a world readable field without a guid.
   * This one has an ALL_GUIDS ACL just for this field.
   */
  public void test_261_UnsignedReadWithFieldAccess(GuidEntry unsignedReadTestGuid, String unsignedReadFieldName) {
    try {
      Assert.assertEquals("funkadelicread", client.fieldRead(unsignedReadTestGuid.getGuid(),
              unsignedReadFieldName, null));
    } catch (Exception e) {
      failWithStackTrace("Exception while testing for unsigned access in UnsignedReadAddReadWithFieldAccess: ", e);
    }
  }

  /**
   * Insures that we still can't read the non-world-readable field without a guid.
   */
  public void test_262_UnsignedReadFailAgain(GuidEntry unsignedReadTestGuid, String unreadAbleReadFieldName) {
    try {
      try {
        String result = client.fieldRead(unsignedReadTestGuid.getGuid(), unreadAbleReadFieldName, null);
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
  
  public void test_263_UnsignedReadFailMissingField(GuidEntry unsignedReadTestGuid) {
    String missingFieldName = "missingField" + RandomString.randomString(12);
    try {
      try {
        String result = client.fieldRead(unsignedReadTestGuid.getGuid(), missingFieldName, null);
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
 * @throws Exception 
   */
  @Test
  public void test_264_UnsignedWriteTests() throws Exception{
		GuidEntry westyEntry = GuidUtils.lookupOrCreateGuid(client, masterGuid, "westy264" + RandomString.randomString(6));
		test_265_UnsignedWrite(westyEntry);
		test_270_RemoveField(westyEntry);
  }

  /**
   * Unsigned write tests.
 * @throws Exception 
   */
  public void test_265_UnsignedWrite(GuidEntry westyEntry) throws Exception {
    //CHECKED FOR VALIDITY

    String unsignedWriteFieldName = "allwriteaccess";
    String standardWriteFieldName = "standardwriteaccess";
    try {
      client.fieldCreateOneElementList(westyEntry.getGuid(),
              unsignedWriteFieldName, "default", westyEntry);
      // make it writeable by everyone
      client.aclAdd(AclAccessType.WRITE_WHITELIST, westyEntry,
              unsignedWriteFieldName, GNSProtocol.ALL_GUIDS.toString());
      client.fieldReplaceFirstElement(westyEntry.getGuid(),
              unsignedWriteFieldName, "funkadelicwrite", westyEntry);
      Assert.assertEquals("funkadelicwrite", client.fieldReadArrayFirstElement(
              westyEntry.getGuid(), unsignedWriteFieldName, westyEntry));

      client.fieldCreateOneElementList(westyEntry.getGuid(),
              standardWriteFieldName, "bummer", westyEntry);
      try {
        client.fieldReplaceFirstElement(westyEntry.getGuid(),
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
   */
  public void test_270_RemoveField(GuidEntry westyEntry) {
    //CHECKED FOR VALIDITY
    String fieldToDelete = "fieldToDelete";
    try {
      client.fieldCreateOneElementList(westyEntry.getGuid(),
              fieldToDelete, "work", westyEntry);
    } catch (Exception e) {
      failWithStackTrace("Exception while creating the field: ", e);
    }
    try {
      // read my own field
      Assert.assertEquals("work", client.fieldReadArrayFirstElement(
              westyEntry.getGuid(), fieldToDelete, westyEntry));
    } catch (Exception e) {
      failWithStackTrace("Exception while reading the field " + fieldToDelete + ": ", e);
    }
    try {
      client.fieldRemove(westyEntry.getGuid(), fieldToDelete, westyEntry);
    } catch (Exception e) {
      failWithStackTrace("Exception while removing field: ", e);
    }

    try {
      String result = client.fieldReadArrayFirstElement(
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
   */
  public void test_280_ListOrderAndSetElement(GuidEntry westyEntry) {
    //CHECKED FOR VALIDITY
    try {
      westyEntry = client.guidCreate(masterGuid,
              "westy" + RandomString.randomString(12));
    } catch (Exception e) {
      failWithStackTrace("Exception during creation of westyEntry: ", e);
    }
    try {

      client.fieldCreateOneElementList(westyEntry.getGuid(), "numbers",
              "one", westyEntry);

      Assert.assertEquals("one", client.fieldReadArrayFirstElement(
              westyEntry.getGuid(), "numbers", westyEntry));

      client.fieldAppend(westyEntry.getGuid(), "numbers", "two",
              westyEntry);
      client.fieldAppend(westyEntry.getGuid(), "numbers", "three",
              westyEntry);
      client.fieldAppend(westyEntry.getGuid(), "numbers", "four",
              westyEntry);
      client.fieldAppend(westyEntry.getGuid(), "numbers", "five",
              westyEntry);

      List<String> expected = new ArrayList<String>(Arrays.asList("one",
              "two", "three", "four", "five"));
      ArrayList<String> actual = JSONUtils
              .JSONArrayToArrayList(client.fieldReadArray(
                      westyEntry.getGuid(), "numbers", westyEntry));
      Assert.assertEquals(expected, actual);

      client.fieldSetElement(westyEntry.getGuid(), "numbers", "frank", 2,
              westyEntry);

      expected = new ArrayList<String>(Arrays.asList("one", "two",
              "frank", "four", "five"));
      actual = JSONUtils.JSONArrayToArrayList(client.fieldReadArray(
              westyEntry.getGuid(), "numbers", westyEntry));
      Assert.assertEquals(expected, actual);

    } catch (Exception e) {
      failWithStackTrace("Unexpected exception during test: ", e);
    }
  }

  // @Test
  // public void test_310_BasicSelect() {
  // try {
  // JSONArray result = client.select("cats", "fred");
  // // best we can do since there will be one, but possibly more objects
  // // in results
  // Assert.assertThat(result.length(), greaterThanOrEqualTo(1));
  // } catch (Exception e) {
  // fail("Exception when we were not expecting it: " , e);
  // }
  // }
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
        GuidEntry testEntry = client.guidCreate(masterGuid, "geoTest-"
                + RandomString.randomString(12));
        client.setLocation(testEntry, 0.0, 0.0);
        
        waitSettle(); //See comment under the method header.
        
        // arun: added this but unclear why we should need this at all
        JSONArray location = client.getLocation(testEntry.getGuid(),
                testEntry);
        assert (location.getDouble(0) == 0.0 && location.getDouble(1) == 0.0);
      }
    } catch (Exception e) {
      failWithStackTrace("Exception while writing fields for GeoSpatialSelect: ", e);
    }

    try {

      JSONArray loc = new JSONArray();
      loc.put(1.0);
      loc.put(1.0);
      JSONArray result = client.selectNear(GNSProtocol.LOCATION_FIELD_NAME.toString(), loc, 2000000.0);
      // best we can do should be at least 5, but possibly more objects in
      // results
      Assert.assertThat(result.length(), greaterThanOrEqualTo(5));
    } catch (Exception e) {
      failWithStackTrace("Exception executing selectNear: ", e);
    }

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
      JSONArray result = client.selectWithin(GNSProtocol.LOCATION_FIELD_NAME.toString(), rect);
      // best we can do should be at least 5, but possibly more objects in
      // results
      Assert.assertThat(result.length(), greaterThanOrEqualTo(5));
    } catch (Exception e) {
      failWithStackTrace("Exception executing selectWithin: ", e);
    }
  }

  /**
   * Tests that selectQuery works.
   */
  @Test
  public void test_330_QuerySelect() {
    String fieldName = "testQuery";
    try {
      for (int cnt = 0; cnt < 5; cnt++) {
        GuidEntry testEntry = client.guidCreate(masterGuid,
                "queryTest-" + RandomString.randomString(12));
        JSONArray array = new JSONArray(Arrays.asList(25));
        client.fieldReplaceOrCreateList(testEntry.getGuid(), fieldName,
                array, testEntry);
      }
    } catch (Exception e) {
      failWithStackTrace("Exception while tryint to create the guids: ", e);
    }

    try {
      waitSettle(); //See comment under the method header for test_320_GeoSpatialSelect
      String query = "~" + fieldName + " : ($gt: 0)";
      JSONArray result = client.selectQuery(query);
      for (int i = 0; i < result.length(); i++) {
        System.out.print(result.get(i).toString() + "  ");
      }
      // best we can do should be at least 5, but possibly more objects in
      // results
      Assert.assertThat(result.length(), greaterThanOrEqualTo(5));
    } catch (Exception e) {
      failWithStackTrace("Exception executing selectNear: ", e);
    }

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
      JSONArray result = client.selectWithin(GNSProtocol.LOCATION_FIELD_NAME.toString(), rect);
      // best we can do should be at least 5, but possibly more objects in
      // results
      Assert.assertThat(result.length(), greaterThanOrEqualTo(5));
    } catch (Exception e) {
      failWithStackTrace("Exception executing selectWithin: ", e);
    }
  }

 
  /**
   * Tests fieldSetNull.
 * @throws Exception 
   */
  @Test
  public void test_400_SetFieldNull() throws Exception {
	  //CHECKED FOR VALIDITY
    String field = "fieldToSetToNull";
      GuidEntry westyEntry = client.guidCreate(masterGuid,
              "westy400" + RandomString.randomString(12));
      System.out.print("Created: " + westyEntry);
    try {
      client.fieldCreateOneElementList(westyEntry.getGuid(), field,
              "work", westyEntry);
    } catch (Exception e) {
      failWithStackTrace("Exception while creating the field: ", e);
    }
    try {
      // read my own field
      Assert.assertEquals("work", client.fieldReadArrayFirstElement(
              westyEntry.getGuid(), field, westyEntry));
    } catch (Exception e) {
      failWithStackTrace("Exception while reading the field " + field + ": ", e);
    }
    try {
      client.fieldSetNull(westyEntry.getGuid(), field, westyEntry);
    } catch (Exception e) {
      failWithStackTrace("Exception while setting field to null field: ", e);
    }
    try {
      Assert.assertEquals(null, client.fieldReadArrayFirstElement(
              westyEntry.getGuid(), field, westyEntry));
    } catch (Exception e) {
      failWithStackTrace("Exception while reading the field " + field + ": ", e);
    }
  }

  
  /**
   * A set of tests for testing JSONUpdate.
 * @throws Exception 
   */
  @Test
  public void test_405_JSONUpdate_Tests() throws Exception{
	  GuidEntry westyEntry = test_410_JSONUpdate();
	  test_420_NewRead(westyEntry);
	  test_430_NewUpdate(westyEntry);
	  
  }
  
  /**
   * Tests update using JSON.
 * @throws Exception 
   */
  public GuidEntry test_410_JSONUpdate() throws Exception {
	  //CHECKED FOR VALIDITY
      GuidEntry westyEntry = client.guidCreate(masterGuid,
              "westy410" + RandomString.randomString(12));
      System.out.print("Created: " + westyEntry);
    try {
      JSONObject json = new JSONObject();
      json.put("name", "frank");
      json.put("occupation", "busboy");
      json.put("location", "work");
      json.put("friends",
              new ArrayList<String>(Arrays.asList("Joe", "Sam", "Billy")));
      JSONObject subJson = new JSONObject();
      subJson.put("einy", "floop");
      subJson.put("meiny", "bloop");
      json.put("gibberish", subJson);
      client.update(westyEntry, json);
    } catch (Exception e) {
      failWithStackTrace("Exception while updating JSON: ", e);
    }

    try {
      JSONObject expected = new JSONObject();
      expected.put("name", "frank");
      expected.put("occupation", "busboy");
      expected.put("location", "work");
      expected.put("friends",
              new ArrayList<String>(Arrays.asList("Joe", "Sam", "Billy")));
      JSONObject subJson = new JSONObject();
      subJson.put("einy", "floop");
      subJson.put("meiny", "bloop");
      expected.put("gibberish", subJson);
      JSONObject actual = client.read(westyEntry);
      JSONAssert.assertEquals(expected, actual,
              JSONCompareMode.NON_EXTENSIBLE);
      System.out.println(actual);
    } catch (Exception e) {
      failWithStackTrace("Exception while reading JSON: ", e);
    }

    try {
      JSONObject json = new JSONObject();
      json.put("occupation", "rocket scientist");
      client.update(westyEntry, json);
    } catch (Exception e) {
      failWithStackTrace("Exception while changing \"occupation\" to \"rocket scientist\": ",
              e);
    }

    try {
      JSONObject expected = new JSONObject();
      expected.put("name", "frank");
      expected.put("occupation", "rocket scientist");
      expected.put("location", "work");
      expected.put("friends",
              new ArrayList<String>(Arrays.asList("Joe", "Sam", "Billy")));
      JSONObject subJson = new JSONObject();
      subJson.put("einy", "floop");
      subJson.put("meiny", "bloop");
      expected.put("gibberish", subJson);
      JSONObject actual = client.read(westyEntry);
      JSONAssert.assertEquals(expected, actual,
              JSONCompareMode.NON_EXTENSIBLE);
      System.out.println(actual);
    } catch (Exception e) {
      failWithStackTrace("Exception while reading change of \"occupation\" to \"rocket scientist\": ",
              e);
    }

    try {
      JSONObject json = new JSONObject();
      json.put("ip address", "127.0.0.1");
      client.update(westyEntry, json);
    } catch (Exception e) {
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
              new ArrayList<String>(Arrays.asList("Joe", "Sam", "Billy")));
      JSONObject subJson = new JSONObject();
      subJson.put("einy", "floop");
      subJson.put("meiny", "bloop");
      expected.put("gibberish", subJson);
      JSONObject actual = client.read(westyEntry);
      JSONAssert.assertEquals(expected, actual,
              JSONCompareMode.NON_EXTENSIBLE);
      System.out.println(actual);
    } catch (Exception e) {
      failWithStackTrace("Exception while reading JSON: ", e);
    }

    try {
      client.fieldRemove(westyEntry.getGuid(), "gibberish", westyEntry);
    } catch (Exception e) {
      failWithStackTrace("Exception during remove field \"gibberish\": ", e);
    }

    try {
      JSONObject expected = new JSONObject();
      expected.put("name", "frank");
      expected.put("occupation", "rocket scientist");
      expected.put("location", "work");
      expected.put("ip address", "127.0.0.1");
      expected.put("friends",
              new ArrayList<String>(Arrays.asList("Joe", "Sam", "Billy")));
      JSONObject actual = client.read(westyEntry);
      JSONAssert.assertEquals(expected, actual,
              JSONCompareMode.NON_EXTENSIBLE);
      System.out.println(actual);
    } catch (Exception e) {
      failWithStackTrace("Exception while reading JSON: ", e);
    }
    return westyEntry;
  }

  /**
   * Tests that dotted field reads work.
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
      client.update(westyEntry, json);
    } catch (Exception e) {
      failWithStackTrace("Exception while adding field \"flapjack\": ", e);
    }

    try {
      JSONObject expected = new JSONObject();
      expected.put("name", "frank");
      expected.put("occupation", "rocket scientist");
      expected.put("location", "work");
      expected.put("ip address", "127.0.0.1");
      expected.put("friends",
              new ArrayList<String>(Arrays.asList("Joe", "Sam", "Billy")));
      JSONObject subJson = new JSONObject();
      subJson.put("sammy", "green");
      JSONObject subsubJson = new JSONObject();
      subsubJson.put("right", "seven");
      subsubJson.put("left", "eight");
      subJson.put("sally", subsubJson);
      expected.put("flapjack", subJson);
      JSONObject actual = client.read(westyEntry);
      JSONAssert.assertEquals(expected, actual,
              JSONCompareMode.NON_EXTENSIBLE);
      System.out.println(actual);
    } catch (Exception e) {
      failWithStackTrace("Exception while reading JSON: ", e);
    }
    try {
      String actual = client.fieldRead(westyEntry.getGuid(),
              "flapjack.sally.right", westyEntry);
      Assert.assertEquals("seven", actual);
    } catch (Exception e) {
      failWithStackTrace("Exception while reading \"flapjack.sally.right\": ", e);
    }
    try {
      String actual = client.fieldRead(westyEntry.getGuid(),
              "flapjack.sally", westyEntry);
      String expected = "{ \"left\" : \"eight\" , \"right\" : \"seven\"}";
      JSONAssert.assertEquals(expected, actual,
              JSONCompareMode.NON_EXTENSIBLE);
    } catch (Exception e) {
      failWithStackTrace("Exception while reading \"flapjack.sally\": ", e);
    }

    try {
      String actual = client.fieldRead(westyEntry.getGuid(), "flapjack",
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
   */
  public void test_430_NewUpdate(GuidEntry westyEntry) {
	  //CHECKED FOR VALIDITY
    try {
      client.fieldUpdate(westyEntry.getGuid(), "flapjack.sally.right",
              "crank", westyEntry);
    } catch (Exception e) {
      failWithStackTrace("Exception while updating field \"flapjack.sally.right\": ", e);
    }
    try {
      JSONObject expected = new JSONObject();
      expected.put("name", "frank");
      expected.put("occupation", "rocket scientist");
      expected.put("location", "work");
      expected.put("ip address", "127.0.0.1");
      expected.put("friends",
              new ArrayList<String>(Arrays.asList("Joe", "Sam", "Billy")));
      JSONObject subJson = new JSONObject();
      subJson.put("sammy", "green");
      JSONObject subsubJson = new JSONObject();
      subsubJson.put("right", "crank");
      subsubJson.put("left", "eight");
      subJson.put("sally", subsubJson);
      expected.put("flapjack", subJson);
      JSONObject actual = client.read(westyEntry);
      JSONAssert.assertEquals(expected, actual,
              JSONCompareMode.NON_EXTENSIBLE);
      System.out.println(actual);
    } catch (Exception e) {
      failWithStackTrace("Exception while reading JSON: ", e);
    }
    try {
      client.fieldUpdate(westyEntry.getGuid(), "flapjack.sammy",
              new ArrayList<String>(Arrays.asList("One", "Ready", "Frap")),
              westyEntry);
    } catch (Exception e) {
      failWithStackTrace("Exception while updating field \"flapjack.sammy\": ", e);
    }
    try {
      JSONObject expected = new JSONObject();
      expected.put("name", "frank");
      expected.put("occupation", "rocket scientist");
      expected.put("location", "work");
      expected.put("ip address", "127.0.0.1");
      expected.put("friends",
              new ArrayList<String>(Arrays.asList("Joe", "Sam", "Billy")));
      JSONObject subJson = new JSONObject();
      subJson.put("sammy",
              new ArrayList<String>(Arrays.asList("One", "Ready", "Frap")));
      JSONObject subsubJson = new JSONObject();
      subsubJson.put("right", "crank");
      subsubJson.put("left", "eight");
      subJson.put("sally", subsubJson);
      expected.put("flapjack", subJson);
      JSONObject actual = client.read(westyEntry);
      JSONAssert.assertEquals(expected, actual,
              JSONCompareMode.NON_EXTENSIBLE);
      System.out.println(actual);
    } catch (Exception e) {
      failWithStackTrace("Exception while reading JSON: ", e);
    }
    try {
      JSONObject moreJson = new JSONObject();
      moreJson.put("name", "dog");
      moreJson.put("flyer", "shattered");
      moreJson.put("crash",
              new ArrayList<String>(Arrays.asList("Tango", "Sierra", "Alpha")));
      client.fieldUpdate(westyEntry.getGuid(), "flapjack", moreJson,
              westyEntry);
    } catch (Exception e) {
      failWithStackTrace("Exception while updating field \"flapjack\": ", e);
    }
    try {
      JSONObject expected = new JSONObject();
      expected.put("name", "frank");
      expected.put("occupation", "rocket scientist");
      expected.put("location", "work");
      expected.put("ip address", "127.0.0.1");
      expected.put("friends",
              new ArrayList<String>(Arrays.asList("Joe", "Sam", "Billy")));
      JSONObject moreJson = new JSONObject();
      moreJson.put("name", "dog");
      moreJson.put("flyer", "shattered");
      moreJson.put("crash",
              new ArrayList<String>(Arrays.asList("Tango", "Sierra", "Alpha")));
      expected.put("flapjack", moreJson);
      JSONObject actual = client.read(westyEntry);
      JSONAssert.assertEquals(expected, actual,
              JSONCompareMode.NON_EXTENSIBLE);
      System.out.println(actual);
    } catch (Exception e) {
      failWithStackTrace("Exception while reading JSON: ", e);
    }
  }


  
  /**
   * A set of tests for testing ByteFields
 * @throws IOException 
 * @throws ClientException 
   */
  public void test_435_ByteField_Tests() throws ClientException, IOException{
	  final String BYTE_TEST_FIELD = "testBytes" + RandomString.randomString(12);
	  byte[] byteTestValue;
	  byteTestValue = test_440_CreateBytesField(BYTE_TEST_FIELD);
	  test_441_ReadBytesField(BYTE_TEST_FIELD, byteTestValue);
	  
  }

  /**
   * Tests that updating a field with bytes.
 * @return 
 * @throws IOException 
 * @throws ClientException 
   */
  public byte[] test_440_CreateBytesField(String BYTE_TEST_FIELD) throws ClientException, IOException {
	  //CHECKED FOR VALIDITY
      byte[] byteTestValue = RandomUtils.nextBytes(16000);
      String encodedValue = Base64.encodeToString(byteTestValue, true);
      // System.out.println("Encoded string: " + encodedValue);
      client.fieldUpdate(masterGuid, BYTE_TEST_FIELD, encodedValue);
      return byteTestValue;
  }

  /**
   * Tests reading a field as bytes.
 * @param byteTestValue 
 * @param BYTE_TEST_FIELD 
   */
  public void test_441_ReadBytesField(String BYTE_TEST_FIELD, byte[] byteTestValue) {
	  //CHECKED FOR VALIDITY
    try {
      String string = client.fieldRead(masterGuid, BYTE_TEST_FIELD);
      // System.out.println("Read string: " + string);
      Assert.assertArrayEquals(byteTestValue, Base64.decode(string));
    } catch (Exception e) {
      failWithStackTrace("Exception while reading field: ", e);
    }
  }


  private static int numberTocreate = 100;
  
  /**
   * The test set for testing batch creates.
 * @throws Exception 
   */
  @Test
  public void test_500_Batch_Tests() throws Exception{
	  GuidEntry accountGuidForBatch = test_510_CreateBatchAccountGuid();
	  test_511_CreateBatch(accountGuidForBatch);
	  test_512_CheckBatch(accountGuidForBatch);
  }

  /**
   * Create an account for batch test.
 * @throws Exception 
   */
  public GuidEntry test_510_CreateBatchAccountGuid() throws Exception {
	  //CHECKED FOR VALIDITY
    // can change the number to create on the command line
	 GuidEntry accountGuidForBatch;
    if (System.getProperty("count") != null
            && !System.getProperty("count").isEmpty()) {
      numberTocreate = Integer.parseInt(System.getProperty("count"));
    }
      String batchAccountAlias = "batchTest510"
              + RandomString.randomString(12) + "@gns.name";
      accountGuidForBatch = GuidUtils.lookupOrCreateAccountGuid(client,
              batchAccountAlias, "password", true);
    return accountGuidForBatch;
  }

  /**
   * Create some guids with batch create.
   */
  public void test_511_CreateBatch(GuidEntry accountGuidForBatch) {
	//CHECKED FOR VALIDITY
    Set<String> aliases = new HashSet<>();
    for (int i = 0; i < numberTocreate; i++) {
      //Brendan: I added Integer.toString(i) to this to guarantee no collisions during creation.
      aliases.add("testGUID511" + Integer.toString(i)+ RandomString.randomString(12));
    }
    String result = null;
    long oldTimeout = client.getReadTimeout();
    try {
      client.setReadTimeout(20 * 1000); // 30 seconds
      client.guidBatchCreate(accountGuidForBatch, aliases);
      //result = client.guidBatchCreate(accountGuidForBatch, aliases);
      client.setReadTimeout(oldTimeout);
    } catch (Exception e) {
      failWithStackTrace("Exception while creating guids: ", e);
    }
    //Assert.assertEquals(GNSProtocol.OK_RESPONSE.toString(), result);
  }

  /**
   * Check the batch creation.
   */
  public void test_512_CheckBatch(GuidEntry accountGuidForBatch) {
	  //CHECKED FOR VALIDITY
    try {
      JSONObject accountRecord = client
              .lookupAccountRecord(accountGuidForBatch.getGuid());
      Assert.assertEquals(numberTocreate, accountRecord.getInt("guidCnt"));
    } catch (JSONException | ClientException | IOException e) {
      failWithStackTrace("Exception while fetching account record: ", e);
    }
  }

  
  /**
   * A test set for checking field indices.
 * @throws JSONException 
 * @throws IOException 
 * @throws ClientException 
   */
  @Test
  public void test_530_Index_Tests() throws ClientException, IOException, JSONException{
	  String createIndexTestField = test_540_CreateField();
	  test_541_CreateIndex(createIndexTestField);
	  test_610_SelectPass(createIndexTestField);
  }
  


  /**
   * Create a field for test index.
 * @throws JSONException 
 * @throws IOException 
 * @throws ClientException 
   */
  public String test_540_CreateField() throws ClientException, IOException, JSONException {
	  //CHECKED FOR VALIDITY
    String createIndexTestField = "testField" + RandomString.randomString(12);
      client.fieldUpdate(masterGuid, createIndexTestField,
              createGeoJSONPolygon(AREA_EXTENT));
      return createIndexTestField;
  }

  /**
   * Create an index.
   */
  public void test_541_CreateIndex(String createIndexTestField) {
	  /* TODO: Need to check that fieldCreateIndex always follows the fieldUpdate
	   * done in the previous test, or that it doesn't need to.
	   */
    try {
      client.fieldCreateIndex(masterGuid, createIndexTestField,
              "2dsphere");
    } catch (Exception e) {
      failWithStackTrace("Exception while creating index: ", e);
    }
  }

  /**
   * Insure that the indexed field can be used.
   */
  public void test_610_SelectPass(String createIndexTestField) {
    try {
      JSONArray result = client.selectQuery(buildQuery(
              createIndexTestField, AREA_EXTENT));
      for (int i = 0; i < result.length(); i++) {
        System.out.println(result.get(i).toString());
      }
      Assert.assertThat(result.length(), greaterThanOrEqualTo(1));
    } catch (Exception e) {
      failWithStackTrace("Exception executing second selectNear: ", e);
    }
  }

  
  
  private static final String TEST_HIGH_VALUE = "25";
  private static final String TEST_LOW_VALUE = "10";
  /**
   * A set of tests for testing Queries.
 * @throws NoSuchAlgorithmException 
 * @throws EncryptionException 
   */
  @Test
  public void test_550_Query_Tests() throws EncryptionException, NoSuchAlgorithmException{
	  String groupTestFieldName = "_SelectAutoGroupTestQueryField_" + RandomString.randomString(12);
	  GuidEntry groupOneGuid;
	  GuidEntry groupTwoGuid;
	  String queryOne = "~" + groupTestFieldName + " : {$gt: 20}";
	  String queryTwo = "~" + groupTestFieldName + " : 0";
	  test_551_QueryRemovePreviousTestFields(groupTestFieldName);
	  
	  if(!enable552) return;
	  
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
    Assert.assertThat(result.length(), equalTo(5));
    // look up the individual values
    for (int i = 0; i < result.length(); i++) {
      BasicGuidEntry guidInfo = new BasicGuidEntry(client.lookupGuidRecord(result.getString(i)));
      GuidEntry entry = GuidUtils.lookupGuidEntryFromDatabase(client, guidInfo.getEntityName());
      String value = client.fieldReadArrayFirstElement(entry, groupTestFieldName);
      Assert.assertEquals(TEST_HIGH_VALUE, value);
    }
  }

  /**
   * Remove any old fields from previous tests for group tests.
   */
  public void test_551_QueryRemovePreviousTestFields(String groupTestFieldName) {
    // find all the guids that have our field and remove it from them
    try {
      String query = "~" + groupTestFieldName + " : {$exists: true}";
      JSONArray result = client.selectQuery(query);
      for (int i = 0; i < result.length(); i++) {
        BasicGuidEntry guidInfo = new BasicGuidEntry(client.lookupGuidRecord(result.getString(i)));
        GuidEntry guidEntry = GuidUtils.lookupGuidEntryFromDatabase(client, guidInfo.getEntityName());
        System.out.println("Removing from " + guidEntry.getEntityName());
        client.fieldRemove(guidEntry, groupTestFieldName);
      }
    } catch (Exception e) {
      failWithStackTrace("Trying to remove previous test's fields: ", e);
    }
  }
  
  private static boolean enable552 = false;

  /**
   * Setup some guids for group testing.
 * @throws NoSuchAlgorithmException 
 * @throws EncryptionException 
   */
  public List<GuidEntry> test_552_QuerySetupGuids(String groupTestFieldName, String queryOne, String queryTwo) throws EncryptionException, NoSuchAlgorithmException {
	  if(!enable552) return new ArrayList<GuidEntry>(0);
    try {
      for (int cnt = 0; cnt < 5; cnt++) {
        GuidEntry testEntry = client.guidCreate(masterGuid, "queryTest-" + RandomString.randomString(6));
        JSONArray array = new JSONArray(Arrays.asList(Integer.parseInt(TEST_HIGH_VALUE)));
        client.fieldReplaceOrCreateList(testEntry, groupTestFieldName, array);
      }
      for (int cnt = 0; cnt < 5; cnt++) {
        GuidEntry testEntry = client.guidCreate(masterGuid, "queryTest-" + RandomString.randomString(6));
        JSONArray array = new JSONArray(Arrays.asList(Integer.parseInt(TEST_LOW_VALUE)));
        client.fieldReplaceOrCreateList(testEntry, groupTestFieldName, array);
      }
    } catch (Exception e) {
      failWithStackTrace("Exception while trying to create the guids: ", e);
    }
      // the HRN is a hash of the query
      String groupOneGuidName = Base64.encodeToString(SHA1HashFunction.getInstance().hash(queryOne), false);
      GuidEntry groupOneGuid = GuidUtils.lookupOrCreateGuidEntry(groupOneGuidName, client.getGNSProvider());
      //groupGuid = client.guidCreate(masterGuid, groupGuidName + RandomString.randomString(6));
      
      // the HRN is a hash of the query
      String groupTwoGuidName = Base64.encodeToString(SHA1HashFunction.getInstance().hash(queryTwo), false);
      GuidEntry groupTwoGuid = GuidUtils.lookupOrCreateGuidEntry(groupTwoGuidName, client.getGNSProvider());
      //groupTwoGuid = client.guidCreate(masterGuid, groupTwoGuidName + RandomString.randomString(6));
      
    List<GuidEntry> list = new ArrayList<GuidEntry>(2);
    list.add(groupOneGuid);
    list.add(groupTwoGuid);
    return list;
  }

  /**
   * Setup some grouos for group testing.
   */
  public void test_553_QuerySetupGroup(String groupTestFieldName, GuidEntry groupOneGuid) {
	  if(!enable552) return;

    try {
      String query = "~" + groupTestFieldName + " : {$gt: 20}";
      JSONArray result = client.selectSetupGroupQuery(masterGuid, groupOneGuid.getPublicKeyString(), query, 0); // make the min refresh 0 seconds so the test will never fail
      System.out.println("*****SETUP guid named " + groupOneGuid.getEntityName() + ": ");
      for (int i = 0; i < result.length(); i++) {
        System.out.println(result.get(i).toString());
      }
      // best we can do should be at least 5, but possibly more objects in results
      Assert.assertThat(result.length(), greaterThanOrEqualTo(5));
    } catch (Exception e) {
      failWithStackTrace("Exception executing selectSetupGroupQuery: ", e);
    }
  }

  /**
   * Setup some empty group guids for group testing.
   */
  public void test_554_QuerySetupSecondGroup(String groupTestFieldName, GuidEntry groupTwoGuid) {
	  if(!enable552) return;

    try {
      String query = "~" + groupTestFieldName + " : 0";
      JSONArray result = client.selectSetupGroupQuery(masterGuid, groupTwoGuid.getPublicKeyString(), query, 0); // make the min refresh 0 seconds so the test will never fail
      System.out.println("*****SETUP SECOND guid named " + groupTwoGuid.getEntityName() + ": (should be empty) ");
      for (int i = 0; i < result.length(); i++) {
        System.out.println(result.get(i).toString());
      }
      // should be nothing in this group now
      Assert.assertThat(result.length(), equalTo(0));
    } catch (Exception e) {
      failWithStackTrace("Exception executing second selectSetupGroupQuery: ", e);
    }
  }

  /**
   * Lookup the group members.
   */
  public void test_555_QueryLookupGroup(String groupTestFieldName, GuidEntry groupOneGuid) {
	  if(!enable552) return;

    try {
      JSONArray result = client.selectLookupGroupQuery(groupOneGuid.getGuid());
      checkSelectTheReturnValues(result, groupTestFieldName);
    } catch (Exception e) {
      failWithStackTrace("Exception executing selectLookupGroupQuery: ", e);
    }
  }

  /**
   * Lookup the group members again.
   */
  public void test_556_QueryLookupGroupAgain(String groupTestFieldName, GuidEntry groupOneGuid) {
	  if(!enable552) return;

    try {
      JSONArray result = client.selectLookupGroupQuery(groupOneGuid.getGuid());
      checkSelectTheReturnValues(result, groupTestFieldName);
    } catch (Exception e) {
      failWithStackTrace("Exception executing selectLookupGroupQuery: ", e);
    }
  }

  /**
   * Lookup the group members again.
   */
  public void test_557_LookupGroupAgain2(String groupTestFieldName, GuidEntry groupOneGuid) {
	  if(!enable552) return;

    try {
      JSONArray result = client.selectLookupGroupQuery(groupOneGuid.getGuid());
      checkSelectTheReturnValues(result, groupTestFieldName);
    } catch (Exception e) {
      failWithStackTrace("Exception executing selectLookupGroupQuery: ", e);
    }
  }

  /**
   * Lookup the group members again.
   */
  public void test_558_QueryLookupGroupAgain3(String groupTestFieldName, GuidEntry groupOneGuid) {
	  if(!enable552) return;

    try {
      JSONArray result = client.selectLookupGroupQuery(groupOneGuid.getGuid());
      checkSelectTheReturnValues(result, groupTestFieldName);
    } catch (Exception e) {
      failWithStackTrace("Exception executing selectLookupGroupQuery: ", e);
    }
  }

  /**
   * Change the value that the group is triggered on.
   */
  // Change all the testQuery fields except 1 to be equal to zero
  public void test_559_QueryAlterGroup(String groupTestFieldName, GuidEntry groupOneGuid) {
	  if(!enable552) return;

    try {
      JSONArray result = client.selectLookupGroupQuery(groupOneGuid.getGuid());
      // change ALL BUT ONE to be ZERO
      for (int i = 0; i < result.length() - 1; i++) {
        BasicGuidEntry guidInfo = new BasicGuidEntry(client.lookupGuidRecord(result.getString(i)));
        GuidEntry entry = GuidUtils.lookupGuidEntryFromDatabase(client, guidInfo.getEntityName());
        JSONArray array = new JSONArray(Arrays.asList(0));
        client.fieldReplaceOrCreateList(entry, groupTestFieldName, array);
      }
    } catch (Exception e) {
      failWithStackTrace("Exception while trying to alter the fields: ", e);
    }
  }

  /**
   * Lookup the group members again after the change.
   */
  public void test_560_QueryLookupGroupAfterAlterations(String groupTestFieldName, GuidEntry groupOneGuid) {
	  if(!enable552) return;

    // Westy - Added this to see if it helps with failures...
    try {
      Thread.sleep(100);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    try {
      JSONArray result = client.selectLookupGroupQuery(groupOneGuid.getGuid());
      // should only be one
      Assert.assertThat(result.length(), equalTo(1));
      // look up the individual values
      for (int i = 0; i < result.length(); i++) {
        BasicGuidEntry guidInfo = new BasicGuidEntry(client.lookupGuidRecord(result.getString(i)));
        GuidEntry entry = GuidUtils.lookupGuidEntryFromDatabase(client, guidInfo.getEntityName());
        String value = client.fieldReadArrayFirstElement(entry, groupTestFieldName);
        Assert.assertEquals(TEST_HIGH_VALUE, value);
      }
    } catch (Exception e) {
      failWithStackTrace("Exception executing selectLookupGroupQuery: ", e);
    }
  }

  /**
   * Check to see if the second group has members now... it should.
   */
  public void test_561_QueryLookupSecondGroup(String groupTestFieldName, GuidEntry groupTwoGuid) {
	  if(!enable552) return;

    try {
      JSONArray result = client.selectLookupGroupQuery(groupTwoGuid.getGuid());
      // should be 4 now
      Assert.assertThat(result.length(), equalTo(4));
      // look up the individual values
      for (int i = 0; i < result.length(); i++) {
        BasicGuidEntry guidInfo = new BasicGuidEntry(client.lookupGuidRecord(result.getString(i)));
        GuidEntry entry = GuidUtils.lookupGuidEntryFromDatabase(client, guidInfo.getEntityName());
        String value = client.fieldReadArrayFirstElement(entry, groupTestFieldName);
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

        client.update(masterGuid, attrValJSON);
        // just wait for 2 sec before sending search
        Thread.sleep(2000);

        String[] parsed = csIPPort.split(":");
        String csIP = parsed[0];
        int csPort = Integer.parseInt(parsed[1]);

        ContextServiceClient<Integer> csClient = new ContextServiceClient<Integer>(
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
        Assert.assertThat(resultSize, greaterThanOrEqualTo(1));

      } catch (Exception e) {
        failWithStackTrace("Exception during contextServiceTest: ", e);
      }
    }
  }

  /**
   * A basic test to insure that setting LNS Proxy minimally doesn't break.
   */
  // FIXME: Maybe add something in here to insure that we're actually using an LNS?
  @Test
  public void test_630_CheckLNSProxy() {
    try {
      //PaxosConfig.getActives() works here because the server and client use the same properties file.
      InetAddress lnsAddress = PaxosConfig.getActives().values().iterator().next().getAddress();
      client.setGNSProxy(new InetSocketAddress(lnsAddress, 24598));
    } catch (Exception e) {
      failWithStackTrace("Exception while setting proxy: ", e);
    }
    String guidString = null;
    try {
      guidString = client.lookupGuid(accountAlias);
    } catch (Exception e) {
      failWithStackTrace("Exception while looking up guid: ", e);
    }
    JSONObject json = null;
    if (guidString != null) {
      try {
        json = client.lookupAccountRecord(guidString);
      } catch (Exception e) {
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
    client.setGNSProxy(null);
  }

  private HashMap<String, String> readingOptionsFromNSProperties() {
    HashMap<String, String> propMap = new HashMap<String, String>();

    BufferedReader br = null;
    try {
      String sCurrentLine;

      String filename = new File(
              System.getProperty(DefaultProps.SERVER_COMMAND.key))
              .getParent()
              + "/ns.properties";
      if (!new File(filename).exists()) {
        return propMap;
      }

      br = new BufferedReader(new FileReader(filename));

      while ((sCurrentLine = br.readLine()) != null) {
        String[] parsed = sCurrentLine.split("=");

        if (parsed.length == 2) {
          propMap.put(parsed[0].trim(), parsed[1].trim());
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      try {
        if (br != null) {
          br.close();
        }
      } catch (IOException ex) {
        ex.printStackTrace();
      }
    }
    return propMap;
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
