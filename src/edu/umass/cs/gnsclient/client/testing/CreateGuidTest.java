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

import edu.umass.cs.gnsclient.client.GNSClientCommands;
import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.gnsclient.client.util.GuidUtils;
import edu.umass.cs.gnscommon.utils.RandomString;

import java.awt.HeadlessException;
import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

/**
 * Simple guid creation test.
 *
 * Usage:
 * java -Djavax.net.ssl.trustStorePassword=qwerty -Djavax.net.ssl.trustStore=conf/trustStore/node100.jks -cp dist/gns-1.16-2015-7-27.jar edu.umass.cs.gnsclient.client.testing.CreateGuidTest -host kittens.name -port 24398
 */
public class CreateGuidTest {

  private static final String ACCOUNT_ALIAS = "support@gns.name"; // REPLACE THIS WITH YOUR ACCOUNT ALIAS
  private static final String PASSWORD = "password";
  private static GNSClientCommands client = null;
  private static GuidEntry masterGuid;

  /**
   * Creates a SimpleLatencyTest with the given arguments.
   *
   * @param alias
   */
  public CreateGuidTest(String alias) {
    if (client == null) {
      try {
        client = new GNSClientCommands(null);
      } catch (IOException e) {
        System.out.println("Unable to create client: " + e);
        e.printStackTrace();
        System.exit(1);
      }
      try {
        masterGuid = GuidUtils.lookupOrCreateAccountGuid(client, ACCOUNT_ALIAS, PASSWORD, true);
      } catch (Exception e) {
        System.out.println("Exception when we were not expecting it: " + e);
        e.printStackTrace();
        System.exit(1);
      }
    }
    CreateEntity();
  }

  private void CreateEntity() {
    String alias = "testGUID" + RandomString.randomString(6);
    GuidEntry guidEntry = null;
    try {
      guidEntry = client.guidCreate(masterGuid, alias);
    } catch (Exception e) {
      System.out.println("Exception while creating guid: " + e);
    }
    if (guidEntry != null) {
      System.out.println(guidEntry.getEntityName());
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
      new CreateGuidTest(alias != null ? alias : ACCOUNT_ALIAS);
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

    commandLineOptions = new Options();
    commandLineOptions.addOption(alias);
    commandLineOptions.addOption(help);

    CommandLineParser parser = new GnuParser();
    return parser.parse(commandLineOptions, args);
  }

  private static void printUsage() {
    formatter.printHelp("java -cp GNSClient.jar edu.umass.cs.gnsclient.client.testing.CreateGuidTest <options>", commandLineOptions);
  }

}
