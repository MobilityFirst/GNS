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
package edu.umass.cs.gnsclient.console;

import edu.umass.cs.gnsclient.client.GNSClient;
import edu.umass.cs.gnsclient.client.util.KeyPairUtils;
import java.io.IOException;
import java.io.PrintWriter;

import jline.ConsoleReader;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

/**
 * This class defines a GnsCli
 *
 * @author <a href="mailto:cecchet@cs.umass.edu">Emmanuel Cecchet</a>
 * @version 1.0
 */
public class GnsCli {

  /**
   * Starts the GNS command line interface (CLI) console
   *
   * @param args optional argument is -silent for no console output
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    try {
      CommandLine parser = initializeOptions(args);
      if (parser.hasOption("help")) {
        printUsage();
        //System.out.println("-host and -port are required!");
        System.exit(1);
      }
      boolean silent = parser.hasOption("silent");
      boolean noDefaults = parser.hasOption("noDefaults");
      ConsoleReader consoleReader = new ConsoleReader(System.in,
              new PrintWriter(System.out, true));
      ConsoleModule module = new ConsoleModule(consoleReader);
      if (noDefaults) {
        module.setUseGnsDefaults(false);
        KeyPairUtils.removeDefaultGns();
      }
      module.setSilent(silent);
      if (!silent) {
        module.printString("GNS Client Version: " + GNSClient.readBuildVersion() + "\n");
      }
      module.handlePrompt();
      System.exit(0);
    } catch (IOException e) {
      e.printStackTrace();
      System.exit(-1);
    }
  }

  // command line arguments
  // COMMAND LINE STUFF
  private static HelpFormatter formatter = new HelpFormatter();
  private static Options commandLineOptions;

  private static CommandLine initializeOptions(String[] args) throws ParseException {
    Option help = new Option("help", "Prints Usage");
    Option silent = new Option("silent", "Disables output");
    Option noDefaults = new Option("noDefaults", "Don't use server and guid defaults");

    commandLineOptions = new Options();
    commandLineOptions.addOption(help);
    commandLineOptions.addOption(silent);
    commandLineOptions.addOption(noDefaults);

    CommandLineParser parser = new GnuParser();
    return parser.parse(commandLineOptions, args);
  }

  private static void printUsage() {
    formatter.printHelp("java -jar <JAR> <options>", commandLineOptions);
  }

}
