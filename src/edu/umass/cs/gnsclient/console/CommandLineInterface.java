
package edu.umass.cs.gnsclient.console;

import edu.umass.cs.gnsclient.client.GNSClientConfig;
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


public class CommandLineInterface {


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
        module.printString("GNS Client Version: " + GNSClientConfig.readBuildVersion() + "\n");
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
  private static final HelpFormatter FORMATTER = new HelpFormatter();
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
    return parser.parse(commandLineOptions, args, false);
  }

  private static void printUsage() {
    FORMATTER.printHelp("java -jar <JAR> <options>", commandLineOptions);
  }


}
