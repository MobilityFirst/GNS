/*
 *
 *  Copyright (c) 2015 University of Massachusetts
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you
 *  may not use this file except in compliance with the License. You
 *  may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 *  implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 *
 *  Initial developer(s): Westy, Emmanuel Cecchet
 *
 */
package edu.umass.cs.gnsclient.client.testing;

import edu.umass.cs.gnsclient.client.BasicUniversalTcpClient;
import edu.umass.cs.gnsclient.client.GuidEntry;
import edu.umass.cs.gnsclient.client.util.GuidUtils;
import edu.umass.cs.gnsclient.client.util.ServerSelectDialog;
import edu.umass.cs.gnscommon.utils.ThreadUtils;
import edu.umass.cs.gnscommon.utils.RandomString;
import java.net.InetSocketAddress;
import java.awt.HeadlessException;
import java.util.Random;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

/**
 * This tests tells us if we can get a change in latency for packet reads and rights,
 * by forcing a reconfiguration and measuring the latency of reads before and after
 * a record moves from one active replica to a closer one.
 *
 * It works by creating a guid that is NOT in an active replica close to you and then
 * sending a series of reads and writes using that guid. In time a reconfiguration will happen
 * and the record will be replicated at the closer active replica. If the -debug option is
 * specified output will be printed to the console showing the RTT and which active replica
 * the record is being served from. The -closeAR option is used to specify the active replica
 * which is close to you. The rest of the command line options are below.
 *
 *
 * <code>java -cp GNSClient.jar edu.umass.cs.gnsclient.client.testing.ReplicaLatencyTest</code>
 *
 * The above incantation will show the help text listing all the arguments.
 *
 * Some important ones are with example arguments are:
 *
 * -alias fred@cs.umass.edu - specifies the account alias (your email) used to created your GNS account guid
 *
 * -host gnserve.net - the host where the GNS(LNS) server is running
 *
 * -port 24398 - the port that the LNS server is listening on
 *
 * -debug - if you don't specify this no timing and source output will be produced
 *
 * -closeAR - (REQUIRED) - the name of the active replica that is close to you
 *
 * Here's a full example command:
 *
 * java -cp GNSClient.jar edu.umass.cs.gnsclient.client.testing.ReplicaLatencyTest -alias fred@cs.umass.edu -host kittens.name -port 24398 -debug -closeAR useast1_ActiveReplica
 */
public class ReplicaLatencyTest {

  private static String accountAlias = "boo@hoo.com";
  private static BasicUniversalTcpClient client = null;
  private static GuidEntry masterGuid;
  private static GuidEntry subGuidEntry;

  /**
   * Creates a ThroughputStress with the given arguments.
   *
   * @param alias
   * @param host
   * @param port
   */
  public ReplicaLatencyTest(String alias, String host, String port) {
    InetSocketAddress address;
    if (alias != null) {
      accountAlias = alias;
    }

    if (client == null) {
      if (host != null && port != null) {
        address = new InetSocketAddress(host, Integer.parseInt(port));
      } else {
        address = ServerSelectDialog.selectServer();
      }
      client = new BasicUniversalTcpClient(address.getHostName(), address.getPort());
      try {
        masterGuid = GuidUtils.lookupOrCreateAccountGuid(client, accountAlias, "password", true);
      } catch (Exception e) {
        System.out.println("Exception when we were not expecting it: " + e);
        e.printStackTrace();
        System.exit(1);
      }
    }
  }

  /**
   * The main routine run from the command line.
   *
   * @param args
   * @throws Exception
   */
  public static void main(String args[]) throws Exception {
    try {
      CommandLine parser = initializeOptions(args);
      if (parser.hasOption("help") || args.length == 0) {
        printUsage();
        System.exit(1);
      }
      String alias = parser.getOptionValue("alias");
      String host = parser.getOptionValue("host");
      String port = parser.getOptionValue("port");
      boolean debug = parser.hasOption("debug");
      String closeActiveReplica = parser.getOptionValue("closeAR");
      ReplicaLatencyTest test = new ReplicaLatencyTest(alias, host, port);
      // Need this on to read the which replica is responding
      client.setEnableInstrumentation(true);
      test.findSlowGuid(closeActiveReplica);
      // Now maybe turn on output
      client.setDebuggingEnabled(debug);
      // send the reads and writes
      test.readsAndWrites(closeActiveReplica);
      //test.removeSubGuid();
      client.stop();
      System.exit(0);
    } catch (HeadlessException e) {
      System.out.println("When running headless you'll need to specify the host and port on the command line");
      printUsage();
      System.exit(1);
    }
  }

  private final String guidHRN = "ReplicationGuid";

  /**
   * Creates the guid where do all the data access.
   */
  public void createSubGuid() {
    try {
      subGuidEntry = GuidUtils.lookupOrCreateGuid(client, masterGuid, guidHRN + RandomString.randomString(6));
      System.out.println("Created: " + subGuidEntry);
    } catch (Exception e) {
      System.out.println("Exception when we were not expecting it: " + e);
      e.printStackTrace();
      System.exit(1);
    }
  }

  /**
   * Removes the guid when we're done.
   */
  public void removeSubGuid() {
    try {
      client.guidRemove(masterGuid, subGuidEntry.getGuid());
      System.out.println("Removed: " + subGuidEntry);
    } catch (Exception e) {
      System.out.println("Exception when we were not expecting it: " + e);
      e.printStackTrace();
      System.exit(1);
    }
  }

  Random random = new Random();

  /**
   * This is our worker task that executes a field update on the server.
   */
  public void updateOperation() {
    try {
      client.fieldUpdateAsynch(subGuidEntry, "environment", random.nextInt(100));
      //System.out.print(".");
    } catch (Exception e) {
      System.out.println("Problem running field update: " + e);
    }

  }

  public void readsAndWrites(String closeActiveReplica) {
    boolean successLatch = false;
    int countdown = 10; // makes it do 10 more requests after success
    do {
      for (int i = 0; i < 10; i++) {
        updateOperation();
        ThreadUtils.sleep(100);
      }
      ThreadUtils.sleep(100);
      //System.out.print(".");
      performRead();
      if (!successLatch && closeActiveReplica.equals(client.getLastResponder())) {
        System.out.println("SUCCESS!");
        successLatch = true;
      }
      if (successLatch) {
        countdown--;
      }
    } while (countdown > 0);
  }

  private void findSlowGuid(String closeActiveReplica) {
    System.out.println("Getting a guid that isn't located at " + closeActiveReplica + ". This could take a few seconds.");
    do {
      createSubGuid();
      updateOperation();
      ThreadUtils.sleep(500);
      performRead();
    } while (closeActiveReplica.equals(client.getLastResponder()));
    System.out.println("Got " + subGuidEntry.getGuid());
  }

  public void performRead() {
    try {
      client.fieldRead(subGuidEntry, "environment");
    } catch (Exception e) {
      System.out.println("Problem sending update: " + e);
    }
  }

  // command line arguments
  // COMMAND LINE STUFF
  private static HelpFormatter formatter = new HelpFormatter();
  private static Options commandLineOptions;

  private static CommandLine initializeOptions(String[] args) throws ParseException {
    Option help = new Option("help", "Prints Usage");
    Option alias = OptionBuilder.withArgName("alias").hasArg()
            .withDescription("the alias (HRN) to use for the account")
            .create("alias");
    Option host = OptionBuilder.withArgName("host").hasArg()
            .withDescription("the GNS host")
            .create("host");
    Option port = OptionBuilder.withArgName("port").hasArg()
            .withDescription("the GNS port")
            .create("port");
    Option debug = new Option("debug", "show output");
    Option closeActiveReplica = OptionBuilder.withArgName("closeAR").hasArg()
            .withDescription("the active replica close to you")
            .create("closeAR");

    commandLineOptions = new Options();
    commandLineOptions.addOption(alias);
    commandLineOptions.addOption(host);
    commandLineOptions.addOption(port);
    commandLineOptions.addOption(debug);
    commandLineOptions.addOption(closeActiveReplica);
    commandLineOptions.addOption(help);

    CommandLineParser parser = new GnuParser();
    return parser.parse(commandLineOptions, args);
  }

  private static void printUsage() {
    formatter.printHelp("java -cp GNSClient.jar edu.umass.cs.gnsclient.client.testing.ReplicaLatencyTest <options>", commandLineOptions);
  }

}
