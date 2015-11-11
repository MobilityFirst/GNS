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
import edu.umass.cs.gnscommon.utils.RandomString;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.awt.HeadlessException;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

/**
 * Client side read throughput test.
 *
 * It is run on the command line like this:
 *
 * <code>java -cp GNSClient.jar edu.umass.cs.gnsclient.client.testing.ReadThroughputSynchTest</code>

 The above incantation will show the help text listing all the arguments.

 Some important ones are with example arguments are:

 -alias fred@cs.umass.edu - specifies the account alias (your email) used to created your GNS account guid

 -host pear.cs.umass.edu - the host where the GNS(LNS) server is running

 -port 24398 - the port that the LNS server is listening on

 -rate - a uniform number of how many client requests per second to generate

 -ramp - an alternative way to specify a rate of requests that starts at the number of give and increases by 1 every 5 seconds


 Here's a full example command:

 java -cp GNSClient.jar edu.umass.cs.gnsclient.client.testing.ReadThroughputSynchTest -alias fred@cs.umass.edu -host pear.cs.umass.edu -port 24398 -rate 1
 */
public class ReadThroughputSynchTest {

  private static String accountAlias = "boo@hoo.com";
  private static BasicUniversalTcpClient client = null;
  private static GuidEntry masterGuid;
  private static GuidEntry subGuidEntry;
  private final ExecutorService execPool;
  private static final String valueToRead = "8675309";

  /**
   * Creates a ThroughputStress with the given arguments.
   *
   * @param alias
   * @param host
   * @param port
   */
  public ReadThroughputSynchTest(String alias, String host, String port) {
    InetSocketAddress address;
    if (alias != null) {
      accountAlias = alias;
    }
    execPool = Executors.newFixedThreadPool(1000);;

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
      if (parser.hasOption("help")) {
        printUsage();
        System.exit(1);
      }
      String alias = parser.getOptionValue("alias");
      String host = parser.getOptionValue("host");
      String port = parser.getOptionValue("port");
      int startRate = parser.hasOption("ramp") ? Integer.parseInt(parser.getOptionValue("ramp")) : -1;
      int rate = (parser.hasOption("rate")) ? Integer.parseInt(parser.getOptionValue("rate")) : 1;
      ReadThroughputSynchTest test = new ReadThroughputSynchTest(alias, host, port);

      client.setEnableInstrumentation(true);
      test.createSubGuidAndWriteValue();
      if (startRate != -1) {
        test.ramp(startRate);
      } else {
        test.uniformFast(rate);
      }

      // cleanup
      test.removeSubGuid();
      client.stop();
      System.exit(0);
    } catch (HeadlessException e) {
      System.out.println("When running headless you'll need to specify the host and port on the command line");
      printUsage();
      System.exit(1);
    }
  }

  /**
   * Creates the guid where do all the data access.
   */
  public void createSubGuidAndWriteValue() {
    try {
      subGuidEntry = client.guidCreate(masterGuid, "subGuid" + RandomString.randomString(6));
      System.out.println("Created: " + subGuidEntry);
    } catch (Exception e) {
      System.out.println("Exception creating the subguid: " + e);
      e.printStackTrace();
      System.exit(1);
    }
    try {
      client.fieldUpdate(subGuidEntry, "environment", valueToRead);
    } catch (Exception e) {
      System.out.println("Exception writing the initial value: " + e);
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

  /**
   * This is our worker task that executes a field update on the server.
   */
  public class WorkerTask implements Runnable {

    @Override
    public void run() {
      try {
        String result = client.fieldRead(subGuidEntry, "environment");
        if (!valueToRead.equals(result)) {
          System.out.println("Wrong value read: " + result);
        }
        System.out.print(Long.toString(client.getLastRTT()));
        System.out.print(" ");
      } catch (Exception e) {
        System.out.println("Problem running field read: " + e);
      }
    }
  }

  /**
   * This generates GNS accesses at approximately the rate specified.
   *
   * @param ratePerSec
   */
  public void uniformFast(int ratePerSec) {
    int delayInNanoSeconds = 1000000000 / ratePerSec;
    long lastSubmissionTime = 0;
    do {
      if (System.nanoTime() - lastSubmissionTime >= delayInNanoSeconds) {
        execPool.submit(new WorkerTask());
        lastSubmissionTime = System.nanoTime();
      }
    } while (true);
  }

  /**
   * This generates GNS accesses starting at approximately the rate specified (per second) and
   * increasing buy 1 per second every 5 seconds up to 1000 per second.
   *
   * @param startRate
   */
  public void ramp(int startRate) {
    for (int ratePerSec = startRate; ratePerSec < 1000; ratePerSec = ratePerSec + 10) {
      System.out.print(" " + ratePerSec + " ");
      int delayInNanoSeconds = 1000000000 / ratePerSec;
      long startTime = System.currentTimeMillis();
      List<Future> futures = new ArrayList<Future>();
      long lastSubmissionTime = 0;
      int numberSent = 0;
      do { // submit tasks at the prescribe rate for 5 seconds
        if (System.nanoTime() - lastSubmissionTime >= delayInNanoSeconds) {
          futures.add(execPool.submit(new WorkerTask()));
          lastSubmissionTime = System.nanoTime();
          numberSent++;
        }
        //ThreadUtils.sleep(delayInMillesconds);
        // keep looping until 5 seconds have elapsed
      } while (System.currentTimeMillis() - startTime < 5000);
      System.out.println("Rate/s: " + numberSent / 5.0);
      // and then check them for completion
      for (Future f : futures) {
        try {
          f.get();
        } catch (Exception e) {
          System.out.println("Problem waiting for worker to complete: " + e);
        }
      }
      futures.clear();
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
    Option rate = OptionBuilder.withArgName("rate").hasArg()
            .withDescription("the rate")
            .create("rate");
    Option ramp = OptionBuilder.withArgName("ramp").hasArg()
            .withDescription("the starting ramp rate")
            .create("ramp");

    commandLineOptions = new Options();
    commandLineOptions.addOption(alias);
    commandLineOptions.addOption(host);
    commandLineOptions.addOption(port);
    commandLineOptions.addOption(rate);
    commandLineOptions.addOption(ramp);
    commandLineOptions.addOption(help);

    CommandLineParser parser = new GnuParser();
    return parser.parse(commandLineOptions, args);
  }

  private static void printUsage() {
    formatter.printHelp("java -cp GNSClient.jar edu.umass.cs.gnsclient.client.testing.ThroughputStress <options>", commandLineOptions);
  }

}
