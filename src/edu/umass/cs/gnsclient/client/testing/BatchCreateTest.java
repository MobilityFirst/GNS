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

import edu.umass.cs.gnscommon.GnsProtocol;
import static edu.umass.cs.gnscommon.GnsProtocol.*;
import edu.umass.cs.gnsclient.client.GuidEntry;
import edu.umass.cs.gnsclient.client.UniversalTcpClientExtended;
import edu.umass.cs.gnsclient.client.util.GuidUtils;
import edu.umass.cs.gnsclient.client.util.ServerSelectDialog;
import edu.umass.cs.gnsclient.exceptions.GnsException;
import java.awt.HeadlessException;
import java.io.IOException;
import java.net.InetSocketAddress;
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
 * Simple guid creation test.
 *
 * Usage:
 * java -cp dist/gns-1.16-2015-7-27.jar edu.umass.cs.gnsclient.client.testing.BatchCreateTest -host kittens.name -port 24398 -guidCnt 100
 */
public class BatchCreateTest {

  private static final int MAX_BATCH_SIZE = 4000;
  private static final String ACCOUNT_ALIAS = "batch@gns.name"; // REPLACE THIS WITH YOUR ACCOUNT ALIAS
  private static final String PASSWORD = "password";
  private static UniversalTcpClientExtended client;
  /**
   * The address of the GNS server we will contact
   */
  private static InetSocketAddress address = null;
  private static GuidEntry masterGuid;

  private static boolean disableSSL = false;

  /**
   * Creates a SimpleLatencyTest with the given arguments.
   *
   * @param alias
   * @param host
   * @param portString
   */
  public BatchCreateTest(String alias, String host, String portString, int guidCnt, int writeTo) {
    if (address == null) {
      if (host != null && portString != null) {
        address = new InetSocketAddress(host, Integer.parseInt(portString));
      } else {
        address = ServerSelectDialog.selectServer();
      }
    }
    client = new UniversalTcpClientExtended(address.getHostName(), address.getPort(), disableSSL);

    try {
      masterGuid = GuidUtils.lookupOrCreateAccountGuid(client, alias, PASSWORD, true);
    } catch (Exception e) {
      System.out.println("Exception when we were not expecting it: " + e);
      e.printStackTrace();
      System.exit(1);
    }
    JSONObject command = null;
    String result = null;
    try {
      command = client.createCommand(GnsProtocol.ADMIN,
              GnsProtocol.PASSKEY, "shabiz");
      result = client.checkResponse(command, client.sendCommand(command));
      if (!result.equals(GnsProtocol.OK_RESPONSE)) {
        System.out.println("Admin command saw bad reponse " + result);
        return;
      }
    } catch (GnsException | IOException e) {
      System.out.println("Problem sending admin command " + command + e);
    }
    int oldTimeout = client.getReadTimeout();
    try {
      client.setReadTimeout(2 * 60 * 1000); // set the timeout to 2 minutes

      while (guidCnt > 0) {
        System.out.print("Creating " + Math.min(guidCnt, MAX_BATCH_SIZE));
        command = client.createCommand(GnsProtocol.BATCH_TEST,
                GnsProtocol.NAME, alias,
                GnsProtocol.GUIDCNT, Math.min(guidCnt, MAX_BATCH_SIZE));
        result = client.checkResponse(command, client.sendCommand(command));
        if (!result.equals(GnsProtocol.OK_RESPONSE)) {
          System.out.println();
          System.out.println("Batch test command saw bad reponse " + result);
          return;
        }
        guidCnt = guidCnt - MAX_BATCH_SIZE;
        System.out.println("... " + guidCnt + " left to create");
      }
    } catch (GnsException | IOException e) {
      System.out.println("Problem sending command " + command + e);
    } finally {
      client.setReadTimeout(oldTimeout);
    }
    JSONArray randomGuids = null;
    if (writeTo > 0) {
      try {
        command = client.createCommand(LOOKUP_ACCOUNT_RECORD, GUID, masterGuid.getGuid(), GUIDCNT, writeTo);
        result = client.checkResponse(command, client.sendCommand(command));
        if (!result.startsWith(GnsProtocol.BAD_RESPONSE)) {
          randomGuids = new JSONArray(result);
          //System.out.println("Random guids " + result);
        } else {
          System.out.println("Problem reading random guids " + result);
        }
      } catch (JSONException | IOException | GnsException e) {
        System.out.println("Problem reading random guids " + command + e);
      }
      try {
        if (randomGuids != null) {
          for (int i = 0; i < randomGuids.length(); i++) {
            client.fieldUpdate(randomGuids.getString(i), "environment", 8675309, masterGuid);
            result = client.fieldRead(randomGuids.getString(i), "environment", masterGuid);
            //System.out.println("Read " + result);
          }
        }
      } catch (Exception e) {
        System.out.println("Problem reading / writing to guids " + command + e);
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
      if (parser.hasOption("help") //|| args.length == 0
              //|| !parser.hasOption("host")
              //|| !parser.hasOption("port")
              ) {
        printUsage();
        //System.out.println("-host and -port are required!");
        System.exit(1);
      }
      String alias = parser.getOptionValue("alias");
      String host = parser.getOptionValue("host");
      String portString = parser.getOptionValue("port");
      int guidCnt = Integer.parseInt(parser.getOptionValue("guidCnt", "10000"));
      int writeTo = Integer.parseInt(parser.getOptionValue("writeTo", "0"));
      new BatchCreateTest(alias != null ? alias : ACCOUNT_ALIAS, host, portString, guidCnt, writeTo);
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
    Option guidCnt = OptionBuilder.withArgName("guidCnt").hasArg()
            .withDescription("number of guids to create (default 10000)")
            .create("guidCnt");
    Option writeTo = OptionBuilder.withArgName("writeTo").hasArg()
            .withDescription("number of guids to write and read from (default 0)")
            .create("writeTo");

    commandLineOptions = new Options();
    commandLineOptions.addOption(alias);
    commandLineOptions.addOption(host);
    commandLineOptions.addOption(port);
    commandLineOptions.addOption(guidCnt);
    commandLineOptions.addOption(writeTo);
    commandLineOptions.addOption(help);

    CommandLineParser parser = new GnuParser();
    return parser.parse(commandLineOptions, args);
  }

  private static void printUsage() {
    formatter.printHelp("java -cp GNSClient.jar edu.umass.cs.gnsclient.client.testing.BatchCreateTest <options>", commandLineOptions);
  }

}
