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
import java.awt.HeadlessException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

/**
 * Simple latency test.
 */
public class SimpleLatencyTest {

  private static String accountAlias = "boo@hoo.com";
  private static BasicUniversalTcpClient client;
  private static GuidEntry guid;
  
  /**
   * Creates a SimpleLatencyTest with the given arguments.
   *
   * @param alias
   * @param host
   * @param port
   */
  public SimpleLatencyTest(String alias, String host, int port) {
    client = new BasicUniversalTcpClient(host, port);
    try {
      guid = GuidUtils.lookupOrCreateAccountGuid(client, alias, "password", true);
      client.setEnableInstrumentation(true); // important
      client.fieldUpdate(guid, "environment", "sample");
      System.out.println("Latency: " + Long.toString(client.getLastRTT()));
      System.out.println("CcpProc Time: " + Long.toString(client.getLastCCPProcessingTime()));
      System.out.println("CcpRT Time: " + Long.toString(client.getLastCCPRoundTripTime()));
      System.out.println("Ccp O/S: " + Long.toString(client.getLastCCPOpsPerSecond()));
      System.out.println("Ccp Req Count: " + Long.toString(client.getLastCPPRequestCount()));
      //System.out.println("Lookupt Time: " + Long.toString(client.getLastLookupTime()));
      System.out.println("Responder: " + client.getLastResponder());
   
    } catch (Exception e) {
      System.out.println("Exception: " + e);
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
      if (parser.hasOption("help") || args.length == 0
              || !parser.hasOption("host") 
              || !parser.hasOption("port")) {
        printUsage();
        System.out.println("-host and -port are required!");
        System.exit(1);
      }
      String alias = parser.getOptionValue("alias");
      String host = parser.getOptionValue("host");
      String portString = parser.getOptionValue("port");
      SimpleLatencyTest test = new SimpleLatencyTest(alias != null ? alias : accountAlias, host, Integer.parseInt(portString));
      // Need this on to read the which replica is responding
      System.exit(0);
    } catch (HeadlessException e) {
      System.out.println("When running headless you'll need to specify the host and port on the command line");
      printUsage();
      System.exit(1);
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

    commandLineOptions = new Options();
    commandLineOptions.addOption(alias);
    commandLineOptions.addOption(host);
    commandLineOptions.addOption(port);
    commandLineOptions.addOption(help);

    CommandLineParser parser = new GnuParser();
    return parser.parse(commandLineOptions, args);
  }

  private static void printUsage() {
    formatter.printHelp("java -cp GNSClient.jar edu.umass.cs.gnsclient.client.testing.ReplicaLatencyTest <options>", commandLineOptions);
  }

}
