
package edu.umass.cs.gnsclient.client.testing;

import edu.umass.cs.gnsclient.client.GNSClientCommands;
import edu.umass.cs.gnsclient.client.util.GUIDUtilsHTTPClient;
import edu.umass.cs.gnsclient.client.util.GuidEntry;
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


public class CreateGuidTest {

  private static final String ACCOUNT_ALIAS = "support@gns.name"; // REPLACE THIS WITH YOUR ACCOUNT ALIAS
  private static final String PASSWORD = "password";
  private static GNSClientCommands client = null;
  private static GuidEntry masterGuid;


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
        masterGuid = GUIDUtilsHTTPClient.lookupOrCreateAccountGuid(client, ACCOUNT_ALIAS, PASSWORD, true);
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
    formatter.printHelp("java -cp GNSClient.jar edu.umass.cs.gnsclient.client.testing.CreateGuidTest <options>", commandLineOptions);
  }

}
