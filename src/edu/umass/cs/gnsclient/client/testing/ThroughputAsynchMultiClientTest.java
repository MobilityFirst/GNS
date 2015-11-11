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
import edu.umass.cs.gnscommon.GnsProtocol;
import static edu.umass.cs.gnscommon.GnsProtocol.GUID;
import static edu.umass.cs.gnscommon.GnsProtocol.GUIDCNT;
import static edu.umass.cs.gnscommon.GnsProtocol.LOOKUP_ACCOUNT_RECORD;
import edu.umass.cs.gnsclient.client.GuidEntry;
import edu.umass.cs.gnsclient.client.tcp.packet.CommandPacket;
import edu.umass.cs.gnsclient.client.util.Format;
import edu.umass.cs.gnsclient.client.util.GuidUtils;
import edu.umass.cs.gnsclient.client.util.ServerSelectDialog;
import edu.umass.cs.gnscommon.utils.ThreadUtils;
import edu.umass.cs.gnsclient.exceptions.GnsException;
import java.net.InetSocketAddress;
import java.awt.HeadlessException;
import java.io.IOException;
import java.util.Collections;
import java.util.Date;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * Client side read throughput test using asynchronous send.
 * Can be used to verify server throughput.
 * Uses an asynchronous method to send reads from the client at prescribed rates.
 * It verifies that response packets are sent back to the client and measures
 * the latency and errors associated with those packets.
 *
 * It is run on the command line like this:
 *
 * <code>java -cp GNSClient.jar edu.umass.cs.gnsclient.client.testing.ThroughputAsynchMultiClientTest</code>
 *
 * Using the above incantation with an argument of -help will show the help text listing all the arguments.
 *
 * Some important ones are with example arguments are:
 *
 * -alias fred@cs.umass.edu - specifies the account alias (your email) used to created your GNS account guid
 *
 * -host pear.cs.umass.edu - the host where the GNS(LNS) server is running
 *
 * -port 24403 - the port that the server is listening on (24403 is AR, 24398 is LNS)
 *
 * -rate - the number of how many client requests per second to generate
 *
 * -inc - specifies the N increase for rate every 5 seconds (if not given rate is uniform)
 *
 * -clients - specifies the number of clients to use
 *
 * -requests - specifies the number of requests to send per batch sent by each client
 *
 * Here's a full example command:
 *
 * java -cp dist/GNSClient.jar edu.umass.cs.gnsclient.client.testing.ThroughputAsynchMultiClientTest -host 10.0.1.50 -port 24403 -rate 1000 -inc 500 -clients 10 -requests 20
 *
 * This means to start at 1000 requests per second and increment the rate by 500 every 5 seconds
 * You can also specific a fixed rate using just the -rate option
 *
 * Output looks like this:
 * {pre}
 * 21:40:50 EDT Attempted rate/s 1000.....
 * 21:40:55 EDT Actual rate/s: 977.8994719342851 Average latency: 13.09 Outstanding packets: 0 Errors: 0
 * 21:40:55 EDT Attempted rate/s 1500........
 * 21:41:00 EDT Actual rate/s: 1459.5660749506903 Average latency: 12.54 Outstanding packets: 1 Errors: 0
 * 21:41:00 EDT Attempted rate/s 2000..........
 * 21:41:05 EDT Actual rate/s: 1914.257228315055 Average latency: 12.4 Outstanding packets: 1 Errors: 0
 * 21:41:05 EDT Attempted rate/s 2500............
 * 21:41:10 EDT Actual rate/s: 2382.370458606313 Average latency: 12.74 Outstanding packets: 1 Errors: 0
 * 21:41:10 EDT Attempted rate/s 3000...............
 * 21:41:15 EDT Actual rate/s: 2853.745541022592 Average latency: 13.31 Outstanding packets: 1 Errors: 0
 * {\pre}
 * Outstanding packets should be close to zero if the server is keeping up.
 */
public class ThroughputAsynchMultiClientTest {

  private static final int CYCLE_TIME = 10000;
  private static String accountAlias = "boo@hoo.com";
  private static final int DEFAULT_NUMBER_OF_CLIENTS = 10;
  private static final int DEFAULT_NUMBER_REQUESTS_PER_CLIENT = 10;
  private static int numberOfClients;
  private static int numberOfGuids;
  private static BasicUniversalTcpClient clients[];
  private static GuidEntry masterGuid;
  private static String subGuids[];
  private static boolean read = true;
  private static String updateAlias = null;
  private static String updateField = null;
  private static String updateValue = null;

  // expected max resolution of the millisecond clock
  private static long minSleepInterval = 10;

  private static CommandPacket commmandPackets[];

  private static ExecutorService execPool;

  Set<Integer> chosen;

  private static boolean disableSSL = true;

  //private static final Logger log = Logger.getLogger(ThroughputAsynchMultiClientTest.class.getName());
  /**
   * Creates a ThroughputStress with the given arguments.
   *
   * @param alias
   * @param host
   * @param port
   */
  public ThroughputAsynchMultiClientTest(String alias, String host, String port) {
    InetSocketAddress address;
    if (alias != null) {
      accountAlias = alias;
    }

    if (host != null && port != null) {
      address = new InetSocketAddress(host, Integer.parseInt(port));
    } else {
      address = ServerSelectDialog.selectServer();
    }
    clients = new BasicUniversalTcpClient[numberOfClients];
    subGuids = new String[numberOfGuids];
    commmandPackets = new CommandPacket[numberOfGuids];
    execPool = Executors.newFixedThreadPool(numberOfClients);
    chosen = Collections.newSetFromMap(new ConcurrentHashMap<Integer, Boolean>());
    for (int i = 0; i < numberOfClients; i++) {
      clients[i] = new BasicUniversalTcpClient(address.getHostName(), address.getPort(), disableSSL);
    }
    try {
      masterGuid = GuidUtils.lookupOrCreateAccountGuid(clients[0], accountAlias, "password", true);
    } catch (Exception e) {
      System.out.println("Exception when we were not expecting it: " + e);
      e.printStackTrace();
      System.exit(1);
    }
    for (int i = 0; i < numberOfClients; i++) {
      clients[i].setEnableInstrumentation(true); // important
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
      if (parser.hasOption("help")) {
        printUsage();
        System.exit(1);
      }

      String alias = parser.getOptionValue("alias");
      String host = parser.getOptionValue("host");
      String port = parser.getOptionValue("port");

      if (parser.hasOption("op")
              && ("update".equals(parser.getOptionValue("op"))
              || "write".equals(parser.getOptionValue("op")))) {
        read = false;
        System.out.println("Testing updates");
      } else {
        read = true;
        System.out.println("Testing reads");
      }

      int rate = (parser.hasOption("rate")) ? Integer.parseInt(parser.getOptionValue("rate")) : 1;
      int increment = (parser.hasOption("inc")) ? Integer.parseInt(parser.getOptionValue("inc")) : 10;
      numberOfClients = (parser.hasOption("clients")) ? Integer.parseInt(parser.getOptionValue("clients"))
              : DEFAULT_NUMBER_OF_CLIENTS;
      int requestsPerClient = (parser.hasOption("requests")) ? Integer.parseInt(parser.getOptionValue("requests"))
              : DEFAULT_NUMBER_REQUESTS_PER_CLIENT;
      numberOfGuids = (parser.hasOption("guids")) ? Integer.parseInt(parser.getOptionValue("guids")) : 1;

      updateAlias = parser.hasOption("updateAlias") ? parser.getOptionValue("updateAlias") : null;
      updateField = parser.hasOption("updateField") ? parser.getOptionValue("updateField") : "environment";
      updateValue = parser.hasOption("updateValue") ? parser.getOptionValue("updateValue") : "8675309";

      ThroughputAsynchMultiClientTest test = new ThroughputAsynchMultiClientTest(alias, host, port);

      test.createSubGuidsAndWriteValue();

      // prebuild a packet for each client
      for (int i = 0; i < numberOfGuids; i++) {
        if (read) {
          commmandPackets[i] = clients[0].createTestReadCommand(subGuids[i], updateField, masterGuid);
        } else {
          JSONObject json = new JSONObject();
          json.put(updateField, updateValue);
          commmandPackets[i] = clients[0].createTestUpdateCommand(subGuids[i], json, masterGuid);
        }
      }
      if (parser.hasOption("inc")) {
        test.ramp(rate, increment, requestsPerClient);
      } else if (parser.hasOption("rate")) {
        test.ramp(rate, 0, requestsPerClient);
      } else {
        // should really do this earlier
        printUsage();
        System.exit(1);
      }

      // cleanup
      test.removeSubGuid();
      for (int i = 0; i < numberOfClients; i++) {
        clients[i].stop();
      }
      System.exit(0);
    } catch (HeadlessException e) {
      System.out.println("When running headless you'll need to specify the host and port on the command line");
      printUsage();
      System.exit(1);
    }
  }

  /**
   * Creates the guids where do all the data access.
   * If the user specifies an updateAlias we make one
   * guid using that alias and use that for all clients. Otherwise we
   * make a bunch of random guids.
   */
  public void createSubGuidsAndWriteValue() {
    try {
      if (updateAlias != null) {
        // if it was specified find or create the guid
        subGuids[0] = GuidUtils.lookupOrCreateGuid(clients[0], masterGuid, updateAlias, true).getGuid();
        //subGuidEntries[0] = clients[0].guidCreate(masterGuid, updateAlias);
        //System.out.println("Created: " + subGuidEntries[0]);
      }
      JSONArray randomGuids = null;
      if (updateAlias == null) {
        try {
          JSONObject command = clients[0].createCommand(LOOKUP_ACCOUNT_RECORD, GUID,
                  masterGuid.getGuid(), GUIDCNT, numberOfGuids);
          String result = clients[0].checkResponse(command, clients[0].sendCommand(command));
          if (!result.startsWith(GnsProtocol.BAD_RESPONSE)) {
            randomGuids = new JSONArray(result);
          } else {
            System.out.println("Problem reading random guids " + result);
            System.exit(-1);
          }
        } catch (JSONException | IOException | GnsException e) {
          System.out.println("Problem reading random guids " + e);
          System.exit(-1);
        }
      }
      if (randomGuids.length() == 0) {
        System.out.println("No guids found in account guid " + masterGuid.getEntityName() + "; exiting.");
        System.exit(-1);
      }
      if (randomGuids.length() < numberOfGuids) {
        System.out.println(randomGuids.length() + " guids found in account guid " + masterGuid.getEntityName()
                + " which is not enough"
                + "; exiting.");
        System.exit(-1);
      }
      System.out.println("Using " + numberOfGuids + " guids");

      for (int i = 0; i < numberOfGuids; i++) {
        if (updateAlias != null) {
          // if it was specified copy the single one
          subGuids[i] = subGuids[0];
        } else {
          // otherwise make a new random one
          subGuids[i] = randomGuids.getString(i);
          //subGuids[i] = clients[i].guidCreate(masterGuid, "subGuid" + Utils.randomString(6)).getGuid();
          //System.out.println("Using: " + subGuids[i]);
        }
      }
//      for (int i = 0; i < numberOfClients; i++) {
//        if (updateAlias != null) {
//          // if it was specified copy the single one
//          subGuidEntries[i] = subGuidEntries[0];
//        } else {
//          // otherwise make a new random one
//          subGuidEntries[i] = clients[i].guidCreate(masterGuid, "subGuid" + Utils.randomString(6));
//          System.out.println("Created: " + subGuidEntries[i]);
//        }
//      }
    } catch (Exception e) {
      System.out.println("Exception creating the subguid: " + e);
      e.printStackTrace();
      System.exit(1);
    }

    if (read) {
      try {
        // if the user specified one guid we just need to update that one
        if (updateAlias != null) {
          clients[0].fieldUpdate(subGuids[0], updateField, updateValue, masterGuid);
//        } else {
//          // otherwise write the value into all guids
//          System.out.println("Initializing fields.");
//          for (int i = 0; i < numberOfClients; i++) {
//
//            clients[i].fieldUpdate(subGuids[i], updateField, updateValue, masterGuid);
//            System.out.print(".");
//          }
        }
      } catch (Exception e) {
        System.out.println("Exception writing the initial value: " + e);
        e.printStackTrace();
        System.exit(1);
      }
    }
  }

  /**
   * Removes the guid when we're done.
   */
  public void removeSubGuid() {
    try {
      // if the user specified one guid we just need to remove that one
      if (updateAlias != null) {
        clients[0].guidRemove(masterGuid, subGuids[0]);
//      } else {
//        for (int i = 0; i < numberOfGuids; i++) {
//          clients[0].guidRemove(masterGuid, subGuids[i]);
//          System.out.println("Removed: " + subGuids[i]);
//        }
      }
    } catch (Exception e) {
      System.out.println("Exception when we were not expecting it: " + e);
      e.printStackTrace();
      System.exit(1);
    }
  }

  /**
   * This generates GNS accesses starting at approximately the rate specified (per second) and
   * increasing buy increment per second every 5 seconds.
   *
   * @param startRate
   * @param increment
   * @param requestsPerClient
   */
  private void ramp(int startRate, int increment, int requestsPerClient) {
    for (int ratePerSec = startRate;; ratePerSec = ratePerSec + increment) {
      for (int i = 0; i < numberOfClients; i++) {
        clients[0].resetInstrumentation();
      }
      System.out.print(Format.formatDateTimeOnly(new Date()) + " Attempted rate/s " + ratePerSec);
      // the calculated delay after sending a burst of requests
      int delayInMilleSeconds = numberOfClients * requestsPerClient * 1000 / ratePerSec;
      long loopStartTime = System.currentTimeMillis();
      int numberSent = 0;
      long elapsedTimeInMilleSeconds;
      int sleepResolutionIssues = 0;
      do { // submit tasks at the prescribed rate for 5 seconds
        long initTime = System.currentTimeMillis();
        try {
          // send a bunch
          for (int i = 0; i < numberOfClients; i++) {
            //for (int j = 0; j < requestsPerClient; j++) {
            execPool.submit(new WorkerTask(i, requestsPerClient));
            //clients[i].sendAsynchCommand(readPacket);
            numberSent += requestsPerClient;
            if (numberSent % 1000 == 0) {
              System.out.print(".");
            }
            //}
          }
        } catch (Exception e) {
          System.out.println("Problem running field read: " + e);
        }
        // calculate and sleep long enough to satisfy our rate requirements
        long sleepTime = delayInMilleSeconds - (System.currentTimeMillis() - initTime);
        if (sleepTime > minSleepInterval) {
          ThreadUtils.sleep(sleepTime);
        } else {
          sleepResolutionIssues++;
        }
        // keep looping until CYCLE_TIME milleseconds have elapsed
        elapsedTimeInMilleSeconds = System.currentTimeMillis() - loopStartTime;
      } while (elapsedTimeInMilleSeconds < CYCLE_TIME);

      // calculate some total stats from the clients
      int outstandingPacketCount = 0;
      int totalErrors = 0;
      double latencySum = 0;
      for (int i = 0; i < numberOfClients; i++) {
        outstandingPacketCount += clients[i].outstandingPacketCount();
        totalErrors += clients[i].getTotalErrors();
        latencySum += clients[i].getMovingAvgLatency();
      }
      System.out.println("\n" + Format.formatDateTimeOnly(new Date()) + " Actual rate/s: " + numberSent / (elapsedTimeInMilleSeconds / 1000.0)
              + " Average latency: " + Format.formatTime(latencySum / numberOfClients)
              + " Outstanding packets: " + outstandingPacketCount
              + " Errors: " + totalErrors
              + (sleepResolutionIssues > 0 ? " Sleep Issues: " + sleepResolutionIssues : "")
      //+ "\n" + DelayProfiler.getStats()
      );
      if (outstandingPacketCount > 200000) {
        System.out.println("Backing off before networking freezes.");
        break;
        //ratePerSec = startRate;
      }
    }
  }

  public class WorkerTask implements Runnable {

    private final int clientNumber;
    private final int requests;
    Random rand = new Random();

    public WorkerTask(int clientNumber, int requests) {
      this.clientNumber = clientNumber;
      this.requests = requests;
    }

    @Override
    public void run() {
      try {
        for (int j = 0; j < requests; j++) {
          int index;
          if (chosen.size() >= numberOfGuids) {
            chosen.clear();
            System.out.print(" +++ ");
          }
          do {
            index = rand.nextInt(numberOfGuids);
          } while (chosen.contains(index));
          chosen.add(index);
          clients[clientNumber].sendAsynchCommand(commmandPackets[index]);
        }
      } catch (Exception e) {
        System.out.println("Problem running field read: " + e);
      }
    }
  }

  // command line arguments
  // COMMAND LINE STUFF
  private static HelpFormatter formatter = new HelpFormatter();
  private static Options commandLineOptions;

  private static CommandLine initializeOptions(String[] args) throws ParseException {
    Option help = new Option("help", "Prints Usage");
    Option alias = OptionBuilder.withArgName("alias").hasArg()
            .withDescription("the alias (HRN) to use")
            .create("alias");
    Option host = OptionBuilder.withArgName("host").hasArg()
            .withDescription("the host")
            .create("host");
    Option port = OptionBuilder.withArgName("port").hasArg()
            .withDescription("the port")
            .create("port");
    Option operation = OptionBuilder.withArgName("op").hasArg()
            .withDescription("the operation to perform (read or update)")
            .create("op");
    Option rate = OptionBuilder.withArgName("rate").hasArg()
            .withDescription("the rate in ops per second")
            .create("rate");
    Option inc = OptionBuilder.withArgName("inc").hasArg()
            .withDescription("the increment used with rate")
            .create("inc");
    Option clients = OptionBuilder.withArgName("clients").hasArg()
            .withDescription("number of clients used to send (default 10)")
            .create("clients");
    Option requestsPerClient = OptionBuilder.withArgName("requests").hasArg()
            .withDescription("number of requests sent by each client each time")
            .create("requests");
    Option guidsPerRequest = OptionBuilder.withArgName("guids").hasArg()
            .withDescription("number of guids for each request")
            .create("guids");
    Option updateAlias = new Option("updateAlias", true, "Alias of guid to update/read");
    Option updateField = new Option("updateField", true, "Field to read/update");
    Option updateValue = new Option("updateValue", true, "Value to use in read/update");

    commandLineOptions = new Options();
    commandLineOptions.addOption(alias);
    commandLineOptions.addOption(host);
    commandLineOptions.addOption(port);
    commandLineOptions.addOption(operation);
    commandLineOptions.addOption(rate);
    commandLineOptions.addOption(inc);
    commandLineOptions.addOption(clients);
    commandLineOptions.addOption(requestsPerClient);
    commandLineOptions.addOption(guidsPerRequest);
    commandLineOptions.addOption(help);
    commandLineOptions.addOption(updateAlias);
    commandLineOptions.addOption(updateField);
    commandLineOptions.addOption(updateValue);

    CommandLineParser parser = new GnuParser();
    return parser.parse(commandLineOptions, args);
  }

  private static void printUsage() {
    formatter.printHelp("java -cp GNSClient.jar edu.umass.cs.gnsclient.client.testing.ThroughputAsynchMultiClientTest <options>", commandLineOptions);
  }

}
