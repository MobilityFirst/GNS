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
import edu.umass.cs.gigapaxos.PaxosConfig;
import edu.umass.cs.gnsclient.client.GNSClient;
import edu.umass.cs.gnsclient.client.GNSClientCommands;
import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.gnsclient.client.util.GuidUtils;
import edu.umass.cs.gnscommon.utils.RandomString;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
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
import edu.umass.cs.reconfiguration.ReconfigurationConfig;
import edu.umass.cs.reconfiguration.reconfigurationutils.DefaultNodeConfig;
import edu.umass.cs.utils.DefaultTest;
import edu.umass.cs.utils.Util;


/**
 * Functionality test for core elements in the client using the
 * GNSClientCommands.
 *
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ServerConnectTest extends DefaultTest {

  private static final String DEFAULT_ACCOUNT_ALIAS = "support@gns.name";

  private static String accountAlias = DEFAULT_ACCOUNT_ALIAS; // REPLACE //
  // ALIAS
  private static final String PASSWORD = "password";
  private static GNSClientCommands client = null;
  private static GuidEntry masterGuid;

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

  private static final void failWithStackTrace(String message, Exception... e) {
    if (e != null && e.length > 0) {
      e[0].printStackTrace();
    }
    org.junit.Assert.fail(message);
  }

  /**
   *
   * @throws Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    // Run the server.

    if (System.getProperty("startServer") != null
            && System.getProperty("startServer").equals("true")) {

      // clear explicitly if gigapaxos
      if (useGPScript()) {
        RunCommand
                .command(
                        "kill -s TERM `ps -ef | grep GNS.jar | grep -v grep | "
                        + "grep -v ServerConnectTest  | grep -v \"context\" | awk '{print $2}'`",
                        ".");
        System.out.println(System
                .getProperty(DefaultProps.SERVER_COMMAND.key)
                + " "
                + getGigaPaxosOptions() + " forceclear all");

        RunCommand.command(
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
      ArrayList<String> output = RunCommand.command(
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

	ArrayList<String> output = RunCommand.command("cat " + logFile + " | grep \"java.util.logging.FileHandler.pattern\" | sed 's/java.util.logging.FileHandler.pattern = //g'", ".", false);
	String logFiles = output.get(0) + "*";

	System.out.println("Waiting for servers to be ready...");
	output = RunCommand.command("cat " + gpConfFile + " | grep \"reconfigurator\\.\" | wc -l ", ".", false);
	int numRC = Integer.parseInt(output.get(0));
	output = RunCommand.command("cat " + gpConfFile + " | grep \"active\\.\" | wc -l ", ".", false);
	int numAR = Integer.parseInt(output.get(0));
	int numServers = numRC + numAR;
	output = RunCommand.command("cat " + logFiles + " | grep \"server ready\" | wc -l ", ".", false);
	int numServersUp = Integer.parseInt(output.get(0));
	while (numServersUp < numServers){
    		Thread.sleep(5000);
		output = RunCommand.command("cat " + logFiles + " | grep \"server ready\" | wc -l ", ".", false);
		numServersUp = Integer.parseInt(output.get(0));
		System.out.println(Integer.toString(numServersUp) + " out of " + Integer.toString(numServers) + " servers are ready.");

	}
    System.out.println("Starting client");

    client = new GNSClientCommands(new GNSClient());
    // Make all the reads be coordinated
    client.setForceCoordinatedReads(true);
    // arun: connectivity check embedded in GNSClient constructor
      System.out.println("Client created and connected to server.");
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
          RunCommand.command(command, ".");
        } catch (Exception e) {
          System.out.println(" failed to stop all servers with [" + command + "]");
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

    if (client != null) {
      client.close();
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
  public ServerConnectTest() {

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

  /**
   * Creates a guid.
   */
  @Test
  public void test_010_CreateEntity() {
    String alias = "testGUID" + RandomString.randomString(12);
    GuidEntry guidEntry = null;
    try {
      guidEntry = client.guidCreate(masterGuid, alias);
    } catch (Exception e) {
      failWithStackTrace("Exception while creating guid: ", e);
    }
    assertNotNull(guidEntry);
    assertEquals(alias, guidEntry.getEntityName());
  }

  /**
   * Removes a guid.
   */
  @Test
  public void test_020_RemoveGuid() {
    String testGuidName = "testGUID" + RandomString.randomString(12);
    GuidEntry testGuid = null;
    try {
      testGuid = client.guidCreate(masterGuid, testGuidName);
    } catch (Exception e) {
      failWithStackTrace("Exception while creating testGuid: ", e);
    }
    waitSettle();
    try {
      client.guidRemove(masterGuid, testGuid.getGuid());
    } catch (Exception e) {
      failWithStackTrace("Exception while removing testGuid: ", e);
    }
    waitSettle();
    try {
      client.lookupGuidRecord(testGuid.getGuid());
      failWithStackTrace("Lookup testGuid should have throw an exception.");
    } catch (ClientException e) {

    } catch (IOException e) {
      failWithStackTrace("Exception while doing Lookup testGuid: ", e);
    }
  }


  /**
   * Stops the client.
   */
  @Test
  public void test_9999_Stop() {
    try {
      client.close();
    } catch (Exception e) {
      failWithStackTrace("Exception during stop: ", e);
    }
  }

  /**
   *
   * @param args
   */
  public static void main(String[] args) {
    Result result = JUnitCore.runClasses(ServerConnectTest.class);
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
