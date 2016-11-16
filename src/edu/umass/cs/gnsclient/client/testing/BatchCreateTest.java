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

import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.gnsclient.client.util.GuidUtils;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;

import java.io.IOException;

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

import static edu.umass.cs.gnsclient.client.CommandUtils.*;
import edu.umass.cs.gnsclient.client.GNSClientCommands;
import edu.umass.cs.gnscommon.packets.CommandPacket;
import edu.umass.cs.gnscommon.utils.RandomString;
import edu.umass.cs.gnscommon.CommandType;

import edu.umass.cs.gnscommon.GNSProtocol;
import java.util.HashSet;
import java.util.Set;

/**
 * Simple guid creation test.
 *
 * Creates one account guid and uses that to batch create some number of subguids.
 * Writes to a random sample of the created guids and then deletes everything.
 *
 *
 * Usage:
 * ./scripts/client/runClientSingleNode edu.umass.cs.gnsclient.client.testing.BatchCreateTest -guidCnt 100
 */
public class BatchCreateTest {

  private static final int MAX_BATCH_SIZE = 4000;
  private static final String DEFAULT_ACCOUNT_ALIAS = "batch@gns.name"; // REPLACE THIS WITH YOUR ACCOUNT ALIAS
  private static final String PASSWORD = "password";
  private static GNSClientCommands client;
  /**
   * The address of the GNS server we will contact
   */
  //private static InetSocketAddress address = null;
  private static GuidEntry masterGuid;

  //private static boolean disableSSL = false;
  /**
   * Creates a SimpleLatencyTest with the given arguments.
   *
   * @param accountGuidAlias
   * @param numberToCreate
   * @param writeTo
   */
  public BatchCreateTest(String accountGuidAlias, int numberToCreate, int writeTo) {
    try {
      client = new GNSClientCommands(null);
    } catch (IOException e) {
      System.out.println("Unable to create client: " + e);
      e.printStackTrace();
      System.exit(1);
    }

    try {
      masterGuid = GuidUtils.lookupOrCreateAccountGuid(client, accountGuidAlias, PASSWORD, true);
    } catch (Exception e) {
      System.out.println("Exception when we were not expecting it: " + e);
      e.printStackTrace();
      System.exit(1);
    }

    Set<GuidEntry> createdSubGuids = new HashSet<>();
    String result;
    long startTime = System.currentTimeMillis();
    int guidCnt = numberToCreate;
    long oldTimeout = client.getReadTimeout();
    try {
      client.setReadTimeout(2 * 60 * 1000); // set the timeout to 2 minutes

      while (guidCnt > 0) {
        int numberTocreate = Math.min(guidCnt, MAX_BATCH_SIZE);
        System.out.println("Creating " + numberTocreate);
        Set<String> aliases = new HashSet<>();
        for (int i = 0; i < numberTocreate; i++) {
          aliases.add("testGUID" + RandomString.randomString(6));
        }
        //Actually create them on the server
        result = client.guidBatchCreate(masterGuid, aliases);
        System.out.println("result = " + result);

//        //make one non batch for comparison
//        client.guidCreate(masterGuid, "NOTBATCHALIAS");

        // Store all the created aliases so we can remove them later
        for (String createdAlias : aliases) {
          createdSubGuids.add(GuidUtils.lookupGuidEntryFromDatabase(client, createdAlias));
        }
        
//        //make one non batch for comparison
//        createdSubGuids.add(GuidUtils.lookupGuidEntryFromDatabase(client, "NOTBATCHALIAS"));

        guidCnt -= MAX_BATCH_SIZE;
        if (numberToCreate > MAX_BATCH_SIZE && guidCnt > 0) {
          System.out.println("... " + guidCnt + " left to create");
        } else {
          System.out.println();
        }
      }
    } catch (Exception e) {
      System.out.println("Problem creating batch: " + e);
    } finally {
      client.setReadTimeout(oldTimeout);
    }
    System.out.println("Creating " + numberToCreate + " guids took "
            + ((System.currentTimeMillis() - startTime) / 1000) + " seconds");

    try {
      JSONObject accountRecord = client.lookupAccountRecord(masterGuid.getGuid());
      System.out.println("# guids in account guid = " + accountRecord.getString("guidCnt"));
    } catch (ClientException | IOException | JSONException e) {
      System.out.println("Problem looking up account record: " + e);
    }
    
    String prefix = "";
    for (GuidEntry guidEntry : createdSubGuids) {
      try {
        //System.out.println("Looking up: " + guidEntry.getGuid());
        JSONObject guidRecord = client.lookupGuidRecord(guidEntry.getGuid());
        System.out.print(prefix);
        System.out.print(guidRecord.getString(GNSProtocol.GUID_RECORD_GUID.toString()));
        prefix = ", ";
      } catch (IOException | ClientException | JSONException e) {
        System.out.println("Problem looking up guid: " + e);
      }
    }
    System.out.println();

    JSONObject command = null;
    JSONArray randomGuids = null;
    if (writeTo > 0) {
      try {
        command = createCommand(CommandType.LookupRandomGuids,
                GNSProtocol.GUID.toString(), masterGuid.getGuid(), GNSProtocol.GUIDCNT.toString(), writeTo);
        result = client.execute(new CommandPacket((long)(Math.random()*Long.MAX_VALUE), command)).getResultString();
        //checkResponse(client.sendCommandAndWait(command));
        if (!result.startsWith(GNSProtocol.BAD_RESPONSE.toString())) {
          randomGuids = new JSONArray(result);
          //System.out.println("Random guids " + result);
        } else {
          System.out.println("Problem reading random guids " + result);
        }
      } catch (JSONException | IOException | ClientException e) {
        System.out.println("Problem reading random guids " + command + e);
      }
      try {
        if (randomGuids != null) {
          for (int i = 0; i < randomGuids.length(); i++) {
            client.fieldUpdate(randomGuids.getString(i), "environment", 8675309, masterGuid);
            result = client.fieldRead(randomGuids.getString(i), "environment", masterGuid);
          }
        }
      } catch (Exception e) {
        System.out.println("Problem reading / writing to guids " + command + e);
      }
    }

    for (GuidEntry guidEntryToRemove : createdSubGuids) {
      try {
        client.guidRemove(masterGuid, guidEntryToRemove.getGuid());
      } catch (Exception e) {
        System.out.println("Problem removing guid: " + e);
      }
    }
    try {
      client.accountGuidRemove(masterGuid);
    } catch (Exception e) {
      System.out.println("Problem removing account guid: " + e);
    }
  }

  /**
   * The main routine run from the command line.
   *
   * @param args
   * @throws Exception
   */
  public static void main(String args[]) throws Exception {
    CommandLine parser = initializeOptions(args);
    if (parser.hasOption("help")) {
      printUsage();
      System.exit(1);
    }
    String alias = parser.getOptionValue("alias");
    int guidCnt = Integer.parseInt(parser.getOptionValue("guidCnt", "10000"));
    int writeTo = Integer.parseInt(parser.getOptionValue("writeTo", "0"));
    new BatchCreateTest(alias != null ? alias : DEFAULT_ACCOUNT_ALIAS, guidCnt, writeTo);
    System.exit(0);
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
    Option guidCnt = OptionBuilder.withArgName("guidCnt").hasArg()
            .withDescription("number of guids to create (default 10000)")
            .create("guidCnt");
    Option writeTo = OptionBuilder.withArgName("writeTo").hasArg()
            .withDescription("number of guids to write and read from (default 0)")
            .create("writeTo");

    commandLineOptions = new Options();
    commandLineOptions.addOption(alias);
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
