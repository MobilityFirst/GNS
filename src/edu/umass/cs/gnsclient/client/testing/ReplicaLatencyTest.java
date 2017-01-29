
package edu.umass.cs.gnsclient.client.testing;

import edu.umass.cs.gnsclient.client.GNSClientCommands;
import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.gnsclient.client.util.GuidUtils;
import edu.umass.cs.gnscommon.utils.ThreadUtils;
import edu.umass.cs.gnscommon.utils.RandomString;
import edu.umass.cs.utils.Util;

import java.awt.HeadlessException;
import java.io.IOException;
import java.util.Random;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;


public class ReplicaLatencyTest {

  private static String accountAlias = "boo@hoo.com";
  private static GNSClientCommands client = null;
  private static GuidEntry masterGuid;
  private static GuidEntry subGuidEntry;


  public ReplicaLatencyTest(String alias, String host, String port) {
    if (alias != null) {
      accountAlias = alias;
    }

    if (client == null) {
      try {
        client = new GNSClientCommands(null);
      } catch (IOException e) {
        System.out.println("Unable to create client: " + e);
        e.printStackTrace();
        System.exit(1);
      }
      try {
        masterGuid = GuidUtils.lookupOrCreateAccountGuid(client, accountAlias, "password", true);
      } catch (Exception e) {
        System.out.println("Exception when we were not expecting it: " + e);
        e.printStackTrace();
        System.exit(1);
      }
    }
  }


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
      //client.setEnableInstrumentation(true);
      test.findSlowGuid(closeActiveReplica);
      // send the reads and writes
      test.readsAndWrites(closeActiveReplica);
      //test.removeSubGuid();
      client.close();
      System.exit(0);
    } catch (HeadlessException e) {
      System.out.println("When running headless you'll need to specify the host and port on the command line");
      printUsage();
      System.exit(1);
    }
  }

  private final String guidHRN = "ReplicationGuid";


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


  public void updateOperation() {
    try {
    	Util.suicide("Disabled");
      //client.fieldUpdateAsynch(subGuidEntry, "environment", random.nextInt(100));
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
      //FIXME
      if (!successLatch && closeActiveReplica.equals(null)) {
        //if (!successLatch && closeActiveReplica.equals(client.getLastResponder())) {
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
      //FIXME:
    } while (false);
    //} while (closeActiveReplica.equals(client.getLastResponder()));
    System.out.println("Got " + subGuidEntry.getGuid());
  }


  private void performRead() {
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
