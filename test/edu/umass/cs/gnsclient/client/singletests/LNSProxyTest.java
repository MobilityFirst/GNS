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
package edu.umass.cs.gnsclient.client.singletests;

import edu.umass.cs.gigapaxos.PaxosConfig;
import edu.umass.cs.gnscommon.CommandType;
import edu.umass.cs.gnscommon.GNSProtocol;
import edu.umass.cs.gnscommon.AclAccessType;
import edu.umass.cs.gnscommon.ResponseCode;
import edu.umass.cs.contextservice.client.ContextServiceClient;
import edu.umass.cs.gnsclient.client.GNSClient;
import edu.umass.cs.gnsclient.client.GNSClientCommands;
import edu.umass.cs.gnsclient.client.integrationtests.RunServer;
import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.gnsclient.client.util.GuidUtils;
import edu.umass.cs.gnsclient.client.util.JSONUtils;
import edu.umass.cs.gnscommon.utils.RandomString;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;
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
import static org.junit.Assert.*;
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
import java.util.Set;

import org.apache.commons.lang3.RandomUtils;
import org.json.JSONException;

/**
 * Functionality test for core elements in the client using the
 * GNSClientCommands.
 *
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class LNSProxyTest extends DefaultTest {

  private static final String DEFAULT_ACCOUNT_ALIAS = "support@gns.name";

  private static String accountAlias = DEFAULT_ACCOUNT_ALIAS; // REPLACE //
  // ALIAS
  private static final String PASSWORD = "password";
  private static GNSClientCommands client = null;
  private static GuidEntry masterGuid;
  private static GuidEntry subGuidEntry;
  private static GuidEntry westyEntry;
  private static GuidEntry samEntry;
  private static GuidEntry barneyEntry;
  private static GuidEntry mygroupEntry;
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

  private static final void fail(String message, Exception... e) {
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

      System.out.println(System
              .getProperty(DefaultProps.SERVER_COMMAND.key)
              + " "
              + options);
      ArrayList<String> output = RunServer.command(
              System.getProperty(DefaultProps.SERVER_COMMAND.key) + " "
              + options, ".");
      if (output != null) {
        for (String line : output) {
          System.out.println(line);
        }
      } else {
        fail("Server command failure: ; aborting all tests.");
      }
    }

    System.out.println("Starting client");

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
      fail("Failure setting up account guid; aborting all tests.");
    }

    while (System.currentTimeMillis() - t < WAIT_TILL_ALL_SERVERS_READY) {
      Thread.sleep(WAIT_TILL_ALL_SERVERS_READY
              - (System.currentTimeMillis() - t));
    }
  }

  /**
   *
   * @throws Exception
   */
  @AfterClass
  public static void tearDownAfterClass() throws Exception {

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
   *
   */
  public LNSProxyTest() {

  }

  private static final int RETRANSMISSION_INTERVAL = 100;
  // arun: this should be zero
  private static final int COORDINATION_WAIT = 00;

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

  //@Test

  /**
   *
   */
  public void test_010_CreateEntity() {
    String alias = "testGUID" + RandomString.randomString(12);
    GuidEntry guidEntry = null;
    try {
      guidEntry = client.guidCreate(masterGuid, alias);
    } catch (Exception e) {
      fail("Exception while creating guid: ", e);
    }
    assertNotNull(guidEntry);
    assertEquals(alias, guidEntry.getEntityName());
  }

  //@Test

  /**
   *
   */
  public void test_020_RemoveGuid() {
    String testGuidName = "testGUID" + RandomString.randomString(12);
    GuidEntry testGuid = null;
    try {
      testGuid = client.guidCreate(masterGuid, testGuidName);
    } catch (Exception e) {
      fail("Exception while creating testGuid: ", e);
    }
    waitSettle();
    try {
      client.guidRemove(masterGuid, testGuid.getGuid());
    } catch (Exception e) {
      fail("Exception while removing testGuid: ", e);
    }
    waitSettle();
    try {
      client.lookupGuidRecord(testGuid.getGuid());
      fail("Lookup testGuid should have throw an exception.");
    } catch (ClientException e) {

    } catch (IOException e) {
      fail("Exception while doing Lookup testGuid: ", e);
    }
  }

  //@Test

  /**
   *
   */
  public void test_030_RemoveGuidSansAccountInfo() {
    String testGuidName = "testGUID" + RandomString.randomString(12);
    GuidEntry testGuid = null;
    try {
      testGuid = client.guidCreate(masterGuid, testGuidName);
    } catch (Exception e) {
      fail("Exception while creating testGuid: ", e);
    }
    waitSettle();
    try {
      client.guidRemove(testGuid);
    } catch (Exception e) {
      fail("Exception while removing testGuid: ", e);
    }
    waitSettle();
    try {
      client.lookupGuidRecord(testGuid.getGuid());
      fail("Lookup testGuid should have throw an exception.");
    } catch (ClientException e) {

    } catch (IOException e) {
      fail("Exception while doing Lookup testGuid: ", e);
    }
  }

  //@Test

  /**
   *
   */
  public void test_040_LookupPrimaryGuid() {
    String testGuidName = "testGUID" + RandomString.randomString(12);
    GuidEntry testGuid = null;
    try {
      testGuid = client.guidCreate(masterGuid, testGuidName);
    } catch (Exception e) {
      fail("Exception while creating testGuid: ", e);
    }
    waitSettle();
    try {
      assertEquals(masterGuid.getGuid(),
              client.lookupPrimaryGuid(testGuid.getGuid()));
    } catch (Exception e) {
      fail("Exception while looking up primary guid for testGuid: ", e);
    }
  }

  //@Test

  /**
   *
   */
  public void test_050_CreateSubGuid() {
    try {
      subGuidEntry = client.guidCreate(masterGuid, "subGuid"
              + RandomString.randomString(12));
      System.out.print("Created: " + subGuidEntry);
    } catch (Exception e) {
      fail("Exception creating subguid: ", e);
    }
  }

  //@Test

  /**
   *
   */
  public void test_060_FieldNotFoundException() {
    try {
      client.fieldReadArrayFirstElement(subGuidEntry.getGuid(),
              "environment", subGuidEntry);
      fail("Should have thrown an exception.");
    } catch (FieldNotFoundException e) {
      System.out.print("This was expected: " + e);
    } catch (Exception e) {
      System.out.println("Exception testing field not found: " + e);
    }
  }

  //@Test

  /**
   *
   */
  public void test_070_FieldExistsFalse() {
    try {
      assertFalse(client.fieldExists(subGuidEntry.getGuid(),
              "environment", subGuidEntry));
    } catch (ClientException e) {
      // System.out.println("This was expected: " , e);
    } catch (Exception e) {
      System.out.println("Exception testing field exists false: " + e);
    }
  }

  //@Test

  /**
   *
   */
  public void test_080_CreateFieldForFieldExists() {
    try {
      client.fieldCreateOneElementList(subGuidEntry.getGuid(),
              "environment", "work", subGuidEntry);
    } catch (Exception e) {
      e.printStackTrace();
      fail("Exception during create field: ", e);
    }
  }

  //@Test

  /**
   *
   */
  public void test_090_FieldExistsTrue() {
    try {
      assertTrue(client.fieldExists(subGuidEntry.getGuid(),
              "environment", subGuidEntry));
    } catch (Exception e) {
      System.out.println("Exception testing field exists true: " + e);
    }
  }

  //@Test

  /**
   *
   */
  public void test_100_ACLCreateGuids() {
    try {
      westyEntry = client.guidCreate(masterGuid, "westy" + RandomString.randomString(6));
      samEntry = client.guidCreate(masterGuid, "sam" + RandomString.randomString(6));
      System.out.println("Created: " + westyEntry);
      System.out.println("Created: " + samEntry);
    } catch (Exception e) {
      fail("Exception registering guids in ACLCreateGuids: " + e);
      e.printStackTrace();
    }
    try {
      // remove default read access for this test
      client.aclRemove(AclAccessType.READ_WHITELIST, westyEntry, GNSProtocol.ENTIRE_RECORD.toString(), GNSProtocol.ALL_GUIDS.toString());
    } catch (Exception e) {
      fail("Exception while removing ACL in ACLCreateGuids: " + e);
      e.printStackTrace();
    }
    try {
      JSONArray expected = new JSONArray(new ArrayList<String>(Arrays.asList(masterGuid.getGuid())));
      JSONArray actual = client.aclGet(AclAccessType.READ_WHITELIST, westyEntry,
              GNSProtocol.ENTIRE_RECORD.toString(), westyEntry.getGuid());
      JSONAssert.assertEquals(expected, actual, true);
    } catch (Exception e) {
      fail("Exception while retrieving ACL in ACLCreateGuids: " + e);
      e.printStackTrace();
    }
  }

  //@Test

  /**
   *
   */
  public void test_101_ACLCreateFields() {
    try {
      client.fieldCreateOneElementList(westyEntry.getGuid(), "environment", "work", westyEntry);
      client.fieldCreateOneElementList(westyEntry.getGuid(), "ssn", "000-00-0000", westyEntry);
      client.fieldCreateOneElementList(westyEntry.getGuid(), "password", "666flapJack", westyEntry);
      client.fieldCreateOneElementList(westyEntry.getGuid(), "address", "100 Hinkledinkle Drive", westyEntry);
    } catch (IOException | ClientException e) {
      fail("Exception while creating fields in ACLCreateFields: " + e);
      e.printStackTrace();
    }
  }

  //@Test
  /**
   * This one insures that we can use GNSProtocol.ENTIRE_RECORD.toString() to read all the fields of another guid.
   */
  public void test_102_ACLReadAllFields() {
    try {
      JSONObject expected = new JSONObject();
      expected.put("environment", new JSONArray(new ArrayList<>(Arrays.asList("work"))));
      expected.put("password", new JSONArray(new ArrayList<>(Arrays.asList("666flapJack"))));
      expected.put("ssn", new JSONArray(new ArrayList<>(Arrays.asList("000-00-0000"))));
      expected.put("address", new JSONArray(new ArrayList<>(Arrays.asList("100 Hinkledinkle Drive"))));
      JSONObject actual = new JSONObject(client.fieldRead(westyEntry.getGuid(), GNSProtocol.ENTIRE_RECORD.toString(), masterGuid));
      JSONAssert.assertEquals(expected, actual, true);
    } catch (Exception e) {
      fail("Exception while reading all fields in ACLReadAllFields: " + e);
      e.printStackTrace();
    }
  }

  //@Test

  /**
   *
   */
  public void test_104_ACLReadMyFields() {
    try {
      // read my own field
      assertEquals("work",
              client.fieldReadArrayFirstElement(westyEntry.getGuid(), "environment", westyEntry));
      // read another one of my fields field
      assertEquals("000-00-0000",
              client.fieldReadArrayFirstElement(westyEntry.getGuid(), "ssn", westyEntry));

    } catch (Exception e) {
      fail("Exception while reading fields in ACLReadMyFields: " + e);
      e.printStackTrace();
    }
  }

  //@Test
  /**
   * This one insures that we can't read using GNSProtocol.ENTIRE_RECORD.toString() when the other guid hasn't given us access.
   */
  public void test_105_ACLNotReadOtherGuidAllFieldsTest() {
    try {
      try {
        String result = client.fieldRead(westyEntry.getGuid(), GNSProtocol.ENTIRE_RECORD.toString(), samEntry);
        fail("Result of read of all of westy's fields by sam is " + result
                + " which is wrong because it should have been rejected.");
      } catch (ClientException e) {
      }
    } catch (Exception e) {
      fail("Exception while reading fields in ACLNotReadOtherGuidAllFieldsTest: " + e);
      e.printStackTrace();
    }
  }

  //@Test

  /**
   *
   */
  public void test_106_ACLNotReadOtherGuidFieldTest() {
    try {
      try {
        String result = client.fieldReadArrayFirstElement(westyEntry.getGuid(), "environment",
                samEntry);
        fail("Result of read of westy's environment by sam is " + result
                + " which is wrong because it should have been rejected.");
      } catch (ClientException e) {
      }
    } catch (Exception e) {
      fail("Exception while reading fields in ACLNotReadOtherGuidFieldTest: " + e);
      e.printStackTrace();
    }
  }

  //@Test

  /**
   *
   */
  public void test_110_ACLPartOne() {
    try {
      try {
        client.aclAdd(AclAccessType.READ_WHITELIST, westyEntry, "environment", samEntry.getGuid());
      } catch (Exception e) {
        fail("Exception adding Sam to Westy's readlist: " + e);
        e.printStackTrace();
      }
      try {
        assertEquals("work",
                client.fieldReadArrayFirstElement(westyEntry.getGuid(), "environment", samEntry));
      } catch (Exception e) {
        fail("Exception while Sam reading Westy's field: " + e);
        e.printStackTrace();
      }
    } catch (Exception e) {
      fail("Exception when we were not expecting it in ACLPartOne: " + e);
      e.printStackTrace();
    }
  }

  //@Test

  /**
   *
   */
  public void test_120_ACLPartTwo() {
    try {
      String barneyName = "barney" + RandomString.randomString(6);
      try {
        client.lookupGuid(barneyName);
        fail(barneyName + " entity should not exist");
      } catch (ClientException e) {
      } catch (Exception e) {
        fail("Exception looking up Barney: " + e);
        e.printStackTrace();
      }
      barneyEntry = client.guidCreate(masterGuid, barneyName);
      // remove default read access for this test
      client.aclRemove(AclAccessType.READ_WHITELIST, barneyEntry,
              GNSProtocol.ENTIRE_RECORD.toString(), GNSProtocol.ALL_GUIDS.toString());
      client.fieldCreateOneElementList(barneyEntry.getGuid(), "cell", "413-555-1234", barneyEntry);
      client.fieldCreateOneElementList(barneyEntry.getGuid(), "address", "100 Main Street", barneyEntry);

      try {
        // let anybody read barney's cell field
        client.aclAdd(AclAccessType.READ_WHITELIST, barneyEntry, "cell",
                GNSProtocol.ALL_GUIDS.toString());
      } catch (Exception e) {
        fail("Exception creating ALLUSERS access for Barney's cell: " + e);
        e.printStackTrace();
      }

      try {
        assertEquals("413-555-1234",
                client.fieldReadArrayFirstElement(barneyEntry.getGuid(), "cell", samEntry));
      } catch (Exception e) {
        fail("Exception while Sam reading Barney' cell: " + e);
        e.printStackTrace();
      }

      try {
        assertEquals("413-555-1234",
                client.fieldReadArrayFirstElement(barneyEntry.getGuid(), "cell", westyEntry));
      } catch (Exception e) {
        fail("Exception while Westy reading Barney' cell: " + e);
        e.printStackTrace();
      }

      try {
        String result = client.fieldReadArrayFirstElement(barneyEntry.getGuid(), "address",
                samEntry);
        fail("Result of read of barney's address by sam is " + result
                + " which is wrong because it should have been rejected.");
      } catch (ClientException e) {
        if (e.getCode() == ResponseCode.ACCESS_ERROR) {
          System.out
                  .print("This was expected for null querier trying to ReadUnsigned "
                          + barneyEntry.getGuid()
                          + "'s address: "
                          + e);
        }
      } catch (Exception e) {
        fail("Exception while Sam reading Barney' address: " + e);
        e.printStackTrace();
      }

    } catch (Exception e) {
      fail("Exception when we were not expecting it in ACLPartTwo: " + e);
      e.printStackTrace();
    }
  }

  //@Test

  /**
   *
   */
  public void test_130_ACLALLFields() {
    //testACL();
    String superUserName = "superuser" + RandomString.randomString(6);
    try {
      try {
        client.lookupGuid(superUserName);
        fail(superUserName + " entity should not exist");
      } catch (ClientException e) {
      }

      GuidEntry superuserEntry = client.guidCreate(masterGuid, superUserName);

      // let superuser read any of barney's fields
      client.aclAdd(AclAccessType.READ_WHITELIST, barneyEntry, GNSProtocol.ENTIRE_RECORD.toString(), superuserEntry.getGuid());

      assertEquals("413-555-1234",
              client.fieldReadArrayFirstElement(barneyEntry.getGuid(), "cell", superuserEntry));
      assertEquals("100 Main Street",
              client.fieldReadArrayFirstElement(barneyEntry.getGuid(), "address", superuserEntry));

    } catch (Exception e) {
      fail("Exception when we were not expecting it in ACLALLFields: " + e);
      e.printStackTrace();
    }
  }

  //@Test

  /**
   *
   */
  public void test_140_ACLCreateDeeperField() {
    try {
      try {
        client.fieldUpdate(westyEntry.getGuid(), "test.deeper.field", "fieldValue", westyEntry);
      } catch (IOException | ClientException e) {
        fail("Problem updating field: " + e);
        e.printStackTrace();
      }
      try {
        client.aclAdd(AclAccessType.READ_WHITELIST, westyEntry, "test.deeper.field", GNSProtocol.ENTIRE_RECORD.toString());
      } catch (Exception e) {
        fail("Problem adding acl: " + e);
        e.printStackTrace();
      }
      try {
        JSONArray actual = client.aclGet(AclAccessType.READ_WHITELIST, westyEntry,
                "test.deeper.field", westyEntry.getGuid());
        JSONArray expected = new JSONArray(new ArrayList<String>(Arrays.asList(GNSProtocol.ENTIRE_RECORD.toString())));
        JSONAssert.assertEquals(expected, actual, true);
      } catch (Exception e) {
        fail("Problem reading acl: " + e);
        e.printStackTrace();
      }
    } catch (Exception e) {
      fail("Exception when we were not expecting it ACLCreateDeeperField: " + e);
      e.printStackTrace();
    }
  }

  //@Test

  /**
   *
   */
  public void test_170_DB() {
    // testCreateEntity();
    try {

      client.fieldCreateOneElementList(westyEntry.getGuid(), "cats",
              "whacky", westyEntry);

      waitSettle();
      assertEquals("whacky", client.fieldReadArrayFirstElement(
              westyEntry.getGuid(), "cats", westyEntry));

      client.fieldAppendWithSetSemantics(
              westyEntry.getGuid(),
              "cats",
              new JSONArray(Arrays.asList("hooch", "maya", "red", "sox",
                      "toby")), westyEntry);

      waitSettle();
      HashSet<String> expected = new HashSet<String>(Arrays.asList(
              "hooch", "maya", "red", "sox", "toby", "whacky"));
      HashSet<String> actual = JSONUtils.JSONArrayToHashSet(client
              .fieldReadArray(westyEntry.getGuid(), "cats", westyEntry));
      assertEquals(expected, actual);

      client.fieldClear(westyEntry.getGuid(), "cats", new JSONArray(
              Arrays.asList("maya", "toby")), westyEntry);
      waitSettle();
      expected = new HashSet<String>(Arrays.asList("hooch", "red", "sox",
              "whacky"));
      actual = JSONUtils.JSONArrayToHashSet(client.fieldReadArray(
              westyEntry.getGuid(), "cats", westyEntry));
      assertEquals(expected, actual);

      client.fieldReplaceFirstElement(westyEntry.getGuid(), "cats",
              "maya", westyEntry);
      waitSettle();
      assertEquals("maya", client.fieldReadArrayFirstElement(
              westyEntry.getGuid(), "cats", westyEntry));

      // try {
      // client.fieldCreateOneElementList(westyEntry, "cats", "maya");
      // fail("Should have got an exception when trying to create the field westy / cats.");
      // } catch (ClientException e) {
      // }
      // this one always fails... check it out
      // try {
      // client.fieldAppendWithSetSemantics(westyEntry.getGuid(), "frogs",
      // "freddybub",
      // westyEntry);
      // fail("Should have got an exception when trying to create the field westy / frogs.");
      // } catch (ClientException e) {
      // }
      client.fieldAppendWithSetSemantics(westyEntry.getGuid(), "cats",
              "fred", westyEntry);
      waitSettle();
      expected = new HashSet<String>(Arrays.asList("maya", "fred"));
      actual = JSONUtils.JSONArrayToHashSet(client.fieldReadArray(
              westyEntry.getGuid(), "cats", westyEntry));
      assertEquals(expected, actual);

      client.fieldAppendWithSetSemantics(westyEntry.getGuid(), "cats",
              "fred", westyEntry);
      waitSettle();
      expected = new HashSet<String>(Arrays.asList("maya", "fred"));
      actual = JSONUtils.JSONArrayToHashSet(client.fieldReadArray(
              westyEntry.getGuid(), "cats", westyEntry));
      assertEquals(expected, actual);
    } catch (Exception e) {
      fail("Exception when we were not expecting testing DB: ", e);
    }
  }

  //@Test

  /**
   *
   */
  public void test_180_DBUpserts() {
    HashSet<String> expected = null;
    HashSet<String> actual = null;
    try {

      client.fieldAppendOrCreate(westyEntry.getGuid(), "dogs", "bear",
              westyEntry);
      waitSettle();
      expected = new HashSet<String>(Arrays.asList("bear"));
      actual = JSONUtils.JSONArrayToHashSet(client.fieldReadArray(
              westyEntry.getGuid(), "dogs", westyEntry));
      assertEquals(expected, actual);

    } catch (Exception e) {
      fail("1) Looking for bear: ", e);
    }
    try {
      client.fieldAppendOrCreateList(westyEntry.getGuid(), "dogs",
              new JSONArray(Arrays.asList("wags", "tucker")), westyEntry);
      waitSettle();
      expected = new HashSet<String>(Arrays.asList("bear", "wags",
              "tucker"));
      actual = JSONUtils.JSONArrayToHashSet(client.fieldReadArray(
              westyEntry.getGuid(), "dogs", westyEntry));
      assertEquals(expected, actual);
    } catch (Exception e) {
      fail("2) Looking for bear, wags, tucker: ", e);
    }
    try {
      client.fieldReplaceOrCreate(westyEntry.getGuid(), "goats", "sue",
              westyEntry);
      waitSettle();
      expected = new HashSet<String>(Arrays.asList("sue"));
      actual = JSONUtils.JSONArrayToHashSet(client.fieldReadArray(
              westyEntry.getGuid(), "goats", westyEntry));
      assertEquals(expected, actual);
    } catch (Exception e) {
      fail("3) Looking for sue: ", e);
    }
    try {
      client.fieldReplaceOrCreate(westyEntry.getGuid(), "goats",
              "william", westyEntry);
      waitSettle();
      expected = new HashSet<String>(Arrays.asList("william"));
      actual = JSONUtils.JSONArrayToHashSet(client.fieldReadArray(
              westyEntry.getGuid(), "goats", westyEntry));
      assertEquals(expected, actual);
    } catch (Exception e) {
      fail("4) Looking for william: ", e);
    }
    try {
      client.fieldReplaceOrCreateList(westyEntry.getGuid(), "goats",
              new JSONArray(Arrays.asList("dink", "tink")), westyEntry);
      waitSettle();
      expected = new HashSet<String>(Arrays.asList("dink", "tink"));
      actual = JSONUtils.JSONArrayToHashSet(client.fieldReadArray(
              westyEntry.getGuid(), "goats", westyEntry));
      assertEquals(expected, actual);
    } catch (Exception e) {
      fail("5) Looking for dink, tink: ", e);
    }
  }

  //@Test

  /**
   *
   */
  public void test_190_Substitute() {
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
      fail("Exception during init: ", e);
    }
    try {
      client.fieldAppendOrCreateList(
              testEntry.getGuid(),
              field,
              new JSONArray(Arrays
                      .asList("Frank", "Joe", "Sally", "Rita")),
              testEntry);
    } catch (Exception e) {
      fail("Exception during create: ", e);
    }

    try {
      HashSet<String> expected = new HashSet<String>(Arrays.asList(
              "Frank", "Joe", "Sally", "Rita"));
      HashSet<String> actual = JSONUtils.JSONArrayToHashSet(client
              .fieldReadArray(testEntry.getGuid(), field, testEntry));
      assertEquals(expected, actual);
    } catch (Exception e) {
      fail("Exception when we were not expecting it: ", e);
    }

    try {
      client.fieldSubstitute(testEntry.getGuid(), field, "Christy",
              "Sally", testEntry);
    } catch (Exception e) {
      fail("Exception during substitute: ", e);
    }

    try {
      HashSet<String> expected = new HashSet<String>(Arrays.asList(
              "Frank", "Joe", "Christy", "Rita"));
      HashSet<String> actual = JSONUtils.JSONArrayToHashSet(client
              .fieldReadArray(testEntry.getGuid(), field, testEntry));
      assertEquals(expected, actual);
    } catch (Exception e) {
      fail("Exception when we were not expecting it: ", e);
    }
  }

  //@Test

  /**
   *
   */
  public void test_200_SubstituteList() {
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
      fail("Exception during init: ", e);
    }
    try {
      client.fieldAppendOrCreateList(
              testEntry.getGuid(),
              field,
              new JSONArray(Arrays
                      .asList("Frank", "Joe", "Sally", "Rita")),
              testEntry);
    } catch (Exception e) {
      fail("Exception during create: ", e);
    }

    this.waitSettle();
    try {
      HashSet<String> expected = new HashSet<String>(Arrays.asList(
              "Frank", "Joe", "Sally", "Rita"));
      HashSet<String> actual = JSONUtils.JSONArrayToHashSet(client
              .fieldReadArray(testEntry.getGuid(), field, testEntry));
      assertEquals(expected, actual);
    } catch (Exception e) {
      fail("Exception when we were not expecting it: ", e);
    }

    try {
      client.fieldSubstitute(testEntry.getGuid(), field, new JSONArray(
              Arrays.asList("BillyBob", "Hank")),
              new JSONArray(Arrays.asList("Frank", "Joe")), testEntry);
    } catch (Exception e) {
      fail("Exception during substitute: ", e);
    }

    try {
      HashSet<String> expected = new HashSet<String>(Arrays.asList(
              "BillyBob", "Hank", "Sally", "Rita"));
      HashSet<String> actual = JSONUtils.JSONArrayToHashSet(client
              .fieldReadArray(testEntry.getGuid(), field, testEntry));
      assertEquals(expected, actual);
    } catch (Exception e) {
      fail("Exception when we were not expecting it: ", e);
    }
  }

  //@Test

  /**
   *
   */
  public void test_210_GroupCreate() {
    String mygroupName = "mygroup" + RandomString.randomString(12);
    try {
      try {
        client.lookupGuid(mygroupName);
        fail(mygroupName + " entity should not exist");
      } catch (ClientException e) {
      }
      guidToDeleteEntry = client.guidCreate(masterGuid, "deleteMe"
              + RandomString.randomString(12));
      this.waitSettle();
      mygroupEntry = client.guidCreate(masterGuid, mygroupName);
    } catch (Exception e) {
      fail("Exception while creating guids: ", e);
    }
  }

  //@Test

  /**
   *
   */
  public void test_211_GroupAdd() {
    try {
      client.groupAddGuid(mygroupEntry.getGuid(), westyEntry.getGuid(),
              mygroupEntry);
      client.groupAddGuid(mygroupEntry.getGuid(), samEntry.getGuid(),
              mygroupEntry);
      client.groupAddGuid(mygroupEntry.getGuid(),
              guidToDeleteEntry.getGuid(), mygroupEntry);
    } catch (Exception e) {
      fail("Exception while adding to groups: ", e);
    }
    try {
      HashSet<String> expected = new HashSet<String>(Arrays.asList(
              westyEntry.getGuid(), samEntry.getGuid(),
              guidToDeleteEntry.getGuid()));
      HashSet<String> actual = JSONUtils.JSONArrayToHashSet(client
              .groupGetMembers(mygroupEntry.getGuid(), mygroupEntry));
      assertEquals(expected, actual);

      expected = new HashSet<String>(
              Arrays.asList(mygroupEntry.getGuid()));
      actual = JSONUtils.JSONArrayToHashSet(client.guidGetGroups(
              westyEntry.getGuid(), westyEntry));
      assertEquals(expected, actual);

    } catch (Exception e) {
      fail("Exception while getting members and groups: ", e);
    }
  }

  //@Test

  /**
   *
   */
  public void test_212_GroupRemoveGuid() {
    // now remove a guid and check for group updates
    try {
      client.guidRemove(masterGuid, guidToDeleteEntry.getGuid());
    } catch (Exception e) {
      fail("Exception while removing testGuid: ", e);
    }
    try {
      client.lookupGuidRecord(guidToDeleteEntry.getGuid());
      fail("Lookup testGuid should have throw an exception.");
    } catch (ClientException e) {

    } catch (IOException e) {
      fail("Exception while doing Lookup testGuid: ", e);
    }
  }

  private static GuidEntry groupAccessUserEntry = null;

  //@Test

  /**
   *
   */
  public void test_220_GroupAndACLCreateGuids() {
    // testGroup();
    String groupAccessUserName = "groupAccessUser"
            + RandomString.randomString(12);
    try {
      try {
        client.lookupGuid(groupAccessUserName);
        fail(groupAccessUserName + " entity should not exist");
      } catch (ClientException e) {
      }
    } catch (Exception e) {
      fail("Checking for existence of group user: ", e);
    }

    try {
      groupAccessUserEntry = client.guidCreate(masterGuid,
              groupAccessUserName);
      // remove all fields read by all
      client.aclRemove(AclAccessType.READ_WHITELIST,
              groupAccessUserEntry, GNSProtocol.ENTIRE_RECORD.toString(),
              GNSProtocol.ALL_GUIDS.toString());
    } catch (Exception e) {
      fail("Exception creating group user: ", e);
    }

    try {
      client.fieldCreateOneElementList(groupAccessUserEntry.getGuid(),
              "address", "23 Jumper Road", groupAccessUserEntry);
      client.fieldCreateOneElementList(groupAccessUserEntry.getGuid(),
              "age", "43", groupAccessUserEntry);
      client.fieldCreateOneElementList(groupAccessUserEntry.getGuid(),
              "hometown", "whoville", groupAccessUserEntry);
    } catch (Exception e) {
      fail("Exception creating group user fields: ", e);
    }
    try {
      client.aclAdd(AclAccessType.READ_WHITELIST, groupAccessUserEntry,
              "hometown", mygroupEntry.getGuid());
    } catch (Exception e) {
      fail("Exception adding mygroup to acl for group user hometown field: ",
              e);
    }
  }

  //@Test

  /**
   *
   */
  public void test_221_GroupAndACLTestBadAccess() {
    try {
      try {
        String result = client.fieldReadArrayFirstElement(
                groupAccessUserEntry.getGuid(), "address", westyEntry);
        fail("Result of read of groupAccessUser's age by sam is "
                + result
                + " which is wrong because it should have been rejected.");
      } catch (ClientException e) {
      }
    } catch (Exception e) {
      fail("Exception while attempting a failing read of groupAccessUser's age by sam: ",
              e);
    }
  }

  //@Test

  /**
   *
   */
  public void test_222_GroupAndACLTestGoodAccess() {
    try {
      assertEquals("whoville", client.fieldReadArrayFirstElement(
              groupAccessUserEntry.getGuid(), "hometown", westyEntry));
    } catch (Exception e) {
      fail("Exception while attempting read of groupAccessUser's hometown by westy: ",
              e);
    }
  }

  //@Test

  /**
   *
   */
  public void test_223_GroupAndACLTestRemoveGuid() {
    try {
      try {
        client.groupRemoveGuid(mygroupEntry.getGuid(),
                westyEntry.getGuid(), mygroupEntry);
      } catch (Exception e) {
        fail("Exception removing westy from mygroup: ", e);
      }

      HashSet<String> expected = new HashSet<String>(
              Arrays.asList(samEntry.getGuid()));
      HashSet<String> actual = JSONUtils.JSONArrayToHashSet(client
              .groupGetMembers(mygroupEntry.getGuid(), mygroupEntry));
      assertEquals(expected, actual);

    } catch (Exception e) {
      fail("Exception when we were not expecting it: ", e);
    }
  }

  private static String alias = "ALIAS-" + RandomString.randomString(4)
          + "@blah.org";

  //@Test

  /**
   *
   */
  public void test_230_AliasAdd() {
    try {
      // KEEP IN MIND THAT CURRENTLY ONLY ACCOUNT GUIDS HAVE ALIASES
      // add an alias to the masterGuid
      client.addAlias(masterGuid, alias);
      // lookup the guid using the alias
      assertEquals(masterGuid.getGuid(), client.lookupGuid(alias));
    } catch (Exception e) {
      fail("Exception while adding alias: ", e);
    }
  }

  //@Test

  /**
   *
   */
  public void test_231_AliasRemove() {
    try {
      // grab all the alias from the guid
      HashSet<String> actual = JSONUtils.JSONArrayToHashSet(client
              .getAliases(masterGuid));

      /* arun: This test has no reason to succeed because getAliases is
			 * not coordinated or forceCoordinateable.
			 */
			// make sure our new one is in there
			assertThat(actual, hasItem(alias));
			// now remove it
			client.removeAlias(masterGuid, alias);
		} catch (Exception e) {
			fail("Exception removing alias: ", e);
		}
	}

  //@Test

  /**
   *
   */
  public void test_232_AliasCheck() {
    try {
      // an make sure it is gone
      try {
        client.lookupGuid(alias);
        fail(alias + " should not exist");
      } catch (ClientException e) {
      }
    } catch (Exception e) {
      fail("Exception while checking alias: ", e);
    }
  }

  //@Test

  /**
   *
   */
  public void test_240_WriteAccess() {
    String fieldName = "whereAmI";
    try {
      try {
        client.aclAdd(AclAccessType.WRITE_WHITELIST, westyEntry,
                fieldName, samEntry.getGuid());
      } catch (Exception e) {
        fail("Exception adding Sam to Westy's writelist: ", e);
        e.printStackTrace();
      }
      this.waitSettle();
      // write my own field
      try {
        client.fieldReplaceFirstElement(westyEntry.getGuid(),
                fieldName, "shopping", westyEntry);
      } catch (Exception e) {
        fail("Exception while Westy's writing own field: ", e);
        e.printStackTrace();
      }
      this.waitSettle();
      // now check the value
      assertEquals("shopping", client.fieldReadArrayFirstElement(
              westyEntry.getGuid(), fieldName, westyEntry));
      // someone else write my field
      try {
        client.fieldReplaceFirstElement(westyEntry.getGuid(),
                fieldName, "driving", samEntry);
      } catch (Exception e) {
        fail("Exception while Sam writing Westy's field: ", e);
        e.printStackTrace();
      }
      this.waitSettle();
      // now check the value
      assertEquals("driving", client.fieldReadArrayFirstElement(
              westyEntry.getGuid(), fieldName, westyEntry));
      // do one that should fail
      try {
        client.fieldReplaceFirstElement(westyEntry.getGuid(),
                fieldName, "driving", barneyEntry);
        fail("Write by barney should have failed!");
      } catch (ClientException e) {
      } catch (Exception e) {
        e.printStackTrace();
        fail("Exception during read of westy's " + fieldName
                + " by sam: ", e);
      }
    } catch (Exception e) {
      fail("Exception when we were not expecting it: ", e);
      e.printStackTrace();
    }
  }

  //@Test

  /**
   *
   */
  public void test_250_UnsignedRead() {
    String unsignedReadFieldName = "allreadaccess";
    String standardReadFieldName = "standardreadaccess";
    try {
      client.fieldCreateOneElementList(westyEntry.getGuid(),
              unsignedReadFieldName, "funkadelicread", westyEntry);
      client.aclAdd(AclAccessType.READ_WHITELIST, westyEntry,
              unsignedReadFieldName, GNSProtocol.ALL_GUIDS.toString());
      assertEquals("funkadelicread", client.fieldReadArrayFirstElement(
              westyEntry.getGuid(), unsignedReadFieldName, null));

      client.fieldCreateOneElementList(westyEntry.getGuid(),
              standardReadFieldName, "bummer", westyEntry);
      // already did this above... doing it again gives us a paxos error
      // client.removeFromACL(AclAccessType.READ_WHITELIST, westyEntry,
      // GNSProtocol.ENTIRE_RECORD.toString(), GNSProtocol.ALL_GUIDS.toString());
      try {
        String result = client.fieldReadArrayFirstElement(
                westyEntry.getGuid(), standardReadFieldName, null);
        fail("Result of read of westy's "
                + standardReadFieldName
                + " as world readable was "
                + result
                + " which is wrong because it should have been rejected.");
      } catch (ClientException e) {
      }
    } catch (Exception e) {
      fail("Exception when we were not expecting it: ", e);
    }
  }

  //@Test

  /**
   *
   */
  public void test_260_UnsignedWrite() {
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
      assertEquals("funkadelicwrite", client.fieldReadArrayFirstElement(
              westyEntry.getGuid(), unsignedWriteFieldName, westyEntry));

      client.fieldCreateOneElementList(westyEntry.getGuid(),
              standardWriteFieldName, "bummer", westyEntry);
      try {
        client.fieldReplaceFirstElement(westyEntry.getGuid(),
                standardWriteFieldName, "funkadelicwrite", null);
        fail("Write of westy's field " + standardWriteFieldName
                + " as world readable should have been rejected.");
      } catch (ClientException e) {
      }
    } catch (Exception e) {
      fail("Exception when we were not expecting it: ", e);
    }
  }

  //@Test

  /**
   *
   */
  public void test_270_RemoveField() {
    String fieldToDelete = "fieldToDelete";
    try {
      client.fieldCreateOneElementList(westyEntry.getGuid(),
              fieldToDelete, "work", westyEntry);
    } catch (Exception e) {
      fail("Exception while creating the field: ", e);
    }
    try {
      // read my own field
      assertEquals("work", client.fieldReadArrayFirstElement(
              westyEntry.getGuid(), fieldToDelete, westyEntry));
    } catch (Exception e) {
      fail("Exception while reading the field " + fieldToDelete + ": ", e);
    }
    try {
      client.fieldRemove(westyEntry.getGuid(), fieldToDelete, westyEntry);
    } catch (Exception e) {
      fail("Exception while removing field: ", e);
    }

    try {
      String result = client.fieldReadArrayFirstElement(
              westyEntry.getGuid(), fieldToDelete, westyEntry);

      fail("Result of read of westy's " + fieldToDelete + " is " + result
              + " which is wrong because it should have been deleted.");
    } catch (ClientException e) {
    } catch (Exception e) {
      fail("Exception while removing field: ", e);
    }

  }

  //@Test

  /**
   *
   */
  public void test_280_ListOrderAndSetElement() {
    try {
      westyEntry = client.guidCreate(masterGuid,
              "westy" + RandomString.randomString(12));
    } catch (Exception e) {
      fail("Exception during creation of westyEntry: ", e);
    }
    try {

      client.fieldCreateOneElementList(westyEntry.getGuid(), "numbers",
              "one", westyEntry);

      assertEquals("one", client.fieldReadArrayFirstElement(
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
      assertEquals(expected, actual);

      client.fieldSetElement(westyEntry.getGuid(), "numbers", "frank", 2,
              westyEntry);

      expected = new ArrayList<String>(Arrays.asList("one", "two",
              "frank", "four", "five"));
      actual = JSONUtils.JSONArrayToArrayList(client.fieldReadArray(
              westyEntry.getGuid(), "numbers", westyEntry));
      assertEquals(expected, actual);

    } catch (Exception e) {
      fail("Unexpected exception during test: ", e);
    }
  }

  // //@Test
  // public void test_310_BasicSelect() {
  // try {
  // JSONArray result = client.select("cats", "fred");
  // // best we can do since there will be one, but possibly more objects
  // // in results
  // assertThat(result.length(), greaterThanOrEqualTo(1));
  // } catch (Exception e) {
  // fail("Exception when we were not expecting it: " , e);
  // }
  // }
  //@Test

  /**
   *
   */
  public void test_320_GeoSpatialSelect() {
    try {
      for (int cnt = 0; cnt < 5; cnt++) {
        GuidEntry testEntry = client.guidCreate(masterGuid, "geoTest-"
                + RandomString.randomString(12));
        client.setLocation(testEntry, 0.0, 0.0);
        // arun: added this but unclear why we should need this at all
        JSONArray location = client.getLocation(testEntry.getGuid(),
                testEntry);
        assert (location.getDouble(0) == 0.0 && location.getDouble(1) == 0.0);
      }
      // Thread.sleep(2000); // wait a bit to make sure everything is
      // updated
    } catch (Exception e) {
      fail("Exception while writing fields for GeoSpatialSelect: ", e);
    }

    try {

      JSONArray loc = new JSONArray();
      loc.put(1.0);
      loc.put(1.0);
      JSONArray result = client.selectNear(GNSProtocol.LOCATION_FIELD_NAME.toString(), loc, 2000000.0);
      // best we can do should be at least 5, but possibly more objects in
      // results
      assertThat(result.length(), greaterThanOrEqualTo(5));
    } catch (Exception e) {
      fail("Exception executing selectNear: ", e);
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
      assertThat(result.length(), greaterThanOrEqualTo(5));
    } catch (Exception e) {
      fail("Exception executing selectWithin: ", e);
    }
  }

  //@Test

  /**
   *
   */
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
      fail("Exception while tryint to create the guids: ", e);
    }

    try {
      String query = "~" + fieldName + " : ($gt: 0)";
      JSONArray result = client.selectQuery(query);
      for (int i = 0; i < result.length(); i++) {
        System.out.print(result.get(i).toString() + "  ");
      }
      // best we can do should be at least 5, but possibly more objects in
      // results
      assertThat(result.length(), greaterThanOrEqualTo(5));
    } catch (Exception e) {
      fail("Exception executing selectNear: ", e);
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
      assertThat(result.length(), greaterThanOrEqualTo(5));
    } catch (Exception e) {
      fail("Exception executing selectWithin: ", e);
    }
  }

  //@Test

  /**
   *
   */
  public void test_400_SetFieldNull() {
    String field = "fieldToSetToNull";
    try {
      westyEntry = client.guidCreate(masterGuid,
              "westy" + RandomString.randomString(12));
      System.out.print("Created: " + westyEntry);
    } catch (Exception e) {
      fail("Exception when we were not expecting it: ", e);
    }
    this.waitSettle();
    try {
      client.fieldCreateOneElementList(westyEntry.getGuid(), field,
              "work", westyEntry);
    } catch (Exception e) {
      fail("Exception while creating the field: ", e);
    }
    this.waitSettle();
    try {
      // read my own field
      assertEquals("work", client.fieldReadArrayFirstElement(
              westyEntry.getGuid(), field, westyEntry));
    } catch (Exception e) {
      fail("Exception while reading the field " + field + ": ", e);
    }
    try {
      client.fieldSetNull(westyEntry.getGuid(), field, westyEntry);
    } catch (Exception e) {
      fail("Exception while setting field to null field: ", e);
    }
    this.waitSettle();
    try {
      assertEquals(null, client.fieldReadArrayFirstElement(
              westyEntry.getGuid(), field, westyEntry));
    } catch (Exception e) {
      fail("Exception while reading the field " + field + ": ", e);
    }
  }

  //@Test

  /**
   *
   */
  public void test_410_JSONUpdate() {
    try {
      westyEntry = client.guidCreate(masterGuid,
              "westy" + RandomString.randomString(12));
      System.out.print("Created: " + westyEntry);
    } catch (Exception e) {
      fail("Exception when we were not expecting it: ", e);
    }
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
      fail("Exception while updating JSON: ", e);
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
      fail("Exception while reading JSON: ", e);
    }

    try {
      JSONObject json = new JSONObject();
      json.put("occupation", "rocket scientist");
      client.update(westyEntry, json);
    } catch (Exception e) {
      fail("Exception while changing \"occupation\" to \"rocket scientist\": ",
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
      fail("Exception while reading change of \"occupation\" to \"rocket scientist\": ",
              e);
    }

    try {
      JSONObject json = new JSONObject();
      json.put("ip address", "127.0.0.1");
      client.update(westyEntry, json);
    } catch (Exception e) {
      fail("Exception while adding field \"ip address\" with value \"127.0.0.1\": ",
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
      fail("Exception while reading JSON: ", e);
    }

    try {
      client.fieldRemove(westyEntry.getGuid(), "gibberish", westyEntry);
    } catch (Exception e) {
      fail("Exception during remove field \"gibberish\": ", e);
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
      fail("Exception while reading JSON: ", e);
    }
  }

  //@Test

  /**
   *
   */
  public void test_420_NewRead() {
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
      fail("Exception while adding field \"flapjack\": ", e);
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
      fail("Exception while reading JSON: ", e);
    }
    try {
      String actual = client.fieldRead(westyEntry.getGuid(),
              "flapjack.sally.right", westyEntry);
      assertEquals("seven", actual);
    } catch (Exception e) {
      fail("Exception while reading \"flapjack.sally.right\": ", e);
    }
    try {
      String actual = client.fieldRead(westyEntry.getGuid(),
              "flapjack.sally", westyEntry);
      String expected = "{ \"left\" : \"eight\" , \"right\" : \"seven\"}";
      JSONAssert.assertEquals(expected, actual,
              JSONCompareMode.NON_EXTENSIBLE);
    } catch (Exception e) {
      fail("Exception while reading \"flapjack.sally\": ", e);
    }

    try {
      String actual = client.fieldRead(westyEntry.getGuid(), "flapjack",
              westyEntry);
      String expected = "{ \"sammy\" : \"green\" , \"sally\" : { \"left\" : \"eight\" , \"right\" : \"seven\"}}";
      JSONAssert.assertEquals(expected, actual,
              JSONCompareMode.NON_EXTENSIBLE);
    } catch (Exception e) {
      fail("Exception while reading \"flapjack\": ", e);
    }
  }

  //@Test

  /**
   *
   */
  public void test_430_NewUpdate() {
    try {
      client.fieldUpdate(westyEntry.getGuid(), "flapjack.sally.right",
              "crank", westyEntry);
    } catch (Exception e) {
      fail("Exception while updating field \"flapjack.sally.right\": ", e);
    }
    this.waitSettle();
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
      fail("Exception while reading JSON: ", e);
    }
    try {
      client.fieldUpdate(westyEntry.getGuid(), "flapjack.sammy",
              new ArrayList<String>(Arrays.asList("One", "Ready", "Frap")),
              westyEntry);
    } catch (Exception e) {
      fail("Exception while updating field \"flapjack.sammy\": ", e);
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
      fail("Exception while reading JSON: ", e);
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
      fail("Exception while updating field \"flapjack\": ", e);
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
      fail("Exception while reading JSON: ", e);
    }
  }

  private static final String BYTE_TEST_FIELD = "testBytes";
  private static byte[] byteTestValue;

  //@Test

  /**
   *
   */
  public void test_440_CreateBytesField() {
    try {
      byteTestValue = RandomUtils.nextBytes(16000);
      String encodedValue = Base64.encodeToString(byteTestValue, true);
      // System.out.println("Encoded string: " + encodedValue);
      client.fieldUpdate(masterGuid, BYTE_TEST_FIELD, encodedValue);
    } catch (IOException | ClientException e) {
      fail("Exception during create field: ", e);
    }
  }

  //@Test

  /**
   *
   */
  public void test_441_ReadBytesField() {
    try {
      String string = client.fieldRead(masterGuid, BYTE_TEST_FIELD);
      // System.out.println("Read string: " + string);
      assertArrayEquals(byteTestValue, Base64.decode(string));
    } catch (Exception e) {
      fail("Exception while reading field: ", e);
    }
  }

  private static int numberTocreate = 100;
  private static GuidEntry accountGuidForBatch = null;

  //@Test

  /**
   *
   */
  public void test_510_CreateBatchAccountGuid() {
    // can change the number to create on the command line
    if (System.getProperty("count") != null
            && !System.getProperty("count").isEmpty()) {
      numberTocreate = Integer.parseInt(System.getProperty("count"));
    }
    try {
      String batchAccountAlias = "batchTest"
              + RandomString.randomString(12) + "@gns.name";
      accountGuidForBatch = GuidUtils.lookupOrCreateAccountGuid(client,
              batchAccountAlias, "password", true);
    } catch (Exception e) {
      fail("Exception when we were not expecting it: ", e);
    }
  }

  //@Test

  /**
   *
   */
  public void test_511_CreateBatch() {
    Set<String> aliases = new HashSet<>();
    for (int i = 0; i < numberTocreate; i++) {
      aliases.add("testGUID" + RandomString.randomString(12));
    }
    String result = null;
    long oldTimeout = client.getReadTimeout();
    try {
      client.setReadTimeout(15 * 1000); // 30 seconds
      result = client.guidBatchCreate(accountGuidForBatch, aliases);
      client.setReadTimeout(oldTimeout);
    } catch (Exception e) {
      fail("Exception while creating guids: ", e);
    }
    assertEquals(GNSProtocol.OK_RESPONSE.toString(), result);
  }

  //@Test

  /**
   *
   */
  public void test_512_CheckBatch() {
    try {
      JSONObject accountRecord = client
              .lookupAccountRecord(accountGuidForBatch.getGuid());
      assertEquals(numberTocreate, accountRecord.getInt("guidCnt"));
    } catch (JSONException | ClientException | IOException e) {
      fail("Exception while fetching account record: ", e);
    }
  }

  private static String createIndexTestField;

  //@Test

  /**
   *
   */
  public void test_540_CreateField() {
    createIndexTestField = "testField" + RandomString.randomString(12);
    try {
      client.fieldUpdate(masterGuid, createIndexTestField,
              createGeoJSONPolygon(AREA_EXTENT));
    } catch (Exception e) {
      e.printStackTrace();
      fail("Exception during create field: ", e);
    }
  }

  //@Test

  /**
   *
   */
  public void test_550_CreateIndex() {
    try {
      client.fieldCreateIndex(masterGuid, createIndexTestField,
              "2dsphere");
    } catch (Exception e) {
      fail("Exception while creating index: ", e);
    }
  }

  //@Test

  /**
   *
   */
  public void test_610_SelectPass() {
    try {
      JSONArray result = client.selectQuery(buildQuery(
              createIndexTestField, AREA_EXTENT));
      for (int i = 0; i < result.length(); i++) {
        System.out.println(result.get(i).toString());
      }
      // best we can do should be at least 5, but possibly more objects in
      // results
      assertThat(result.length(), greaterThanOrEqualTo(1));
    } catch (Exception e) {
      fail("Exception executing second selectNear: ", e);
    }
  }

  // test to check context service triggers.
  // these two attributes right now are supported by CS

  /**
   *
   */
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
        assertThat(resultSize, greaterThanOrEqualTo(1));

      } catch (Exception e) {
        e.printStackTrace();
        fail("Exception during contextServiceTest: ", e);
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
      fail("Exception while setting proxy: " + e);
    }
    String guidString = null;
    try {
      guidString = client.lookupGuid(accountAlias);
    } catch (Exception e) {
      fail("Exception while looking up guid: " + e);
    }
    JSONObject json = null;
    if (guidString != null) {
      try {
        json = client.lookupAccountRecord(guidString);
      } catch (Exception e) {
        fail("Exception while looking up account record: " + e);
      }
    }
    assertNotNull("Account record is null", json);
    try {
      assertEquals("Account name doesn't match",
              accountAlias, json.getString(GNSProtocol.ACCOUNT_RECORD_USERNAME.toString()));
    } catch (JSONException e) {
      fail("Exception while looking up account name: " + e);
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
   */
  @Test
  public void test_9999_Stop() {
    try {
      client.close();
    } catch (Exception e) {
      fail("Exception during stop: ", e);
    }
  }

  /**
   *
   * @param args
   */
  public static void main(String[] args) {
    Result result = JUnitCore.runClasses(LNSProxyTest.class);
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
