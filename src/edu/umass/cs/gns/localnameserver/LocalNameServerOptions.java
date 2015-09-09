/*
 * Copyright (C) 2015
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.localnameserver;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import static edu.umass.cs.gns.gnsApp.clientCommandProcessor.commandSupport.GnsProtocolDefs.HELP;
import edu.umass.cs.gns.ping.PingManager;
import static edu.umass.cs.gns.util.Logging.DEFAULTCONSOLELEVEL;
import static edu.umass.cs.gns.util.ParametersAndOptions.CONFIG_FILE;
import static edu.umass.cs.gns.util.ParametersAndOptions.isOptionTrue;
import edu.umass.cs.protocoltask.ProtocolExecutor;
import edu.umass.cs.reconfiguration.ReconfigurationConfig;
import java.util.Map;
import java.util.logging.Level;

/**
 * The command line options for LocalNameServer.
 * 
 * @author westy
 */
public class LocalNameServerOptions {

  // If you change this list, change it below in getAllOptions as well.

  public static final String NS_FILE = "nsfile";
  public static final String PORT = "port";
  public static final String FILE_LOGGING_LEVEL = "fileLoggingLevel";
  public static final String CONSOLE_OUTPUT_LEVEL = "consoleOutputLevel";
  public static final String DEBUG = "debug";
  public static final String DEBUG_PING = "debugPing";
  public static final String DEBUG_MISC = "debugMisc";
  public static final String DISABLE_SSL = "disableSSL";

  /**
   * Returns all the command line options.
   * 
   * @return the command line options
   */
  public static Options getAllOptions() {
    Option help = new Option(HELP, "Prints usage");
    Option configFile = new Option(CONFIG_FILE, true, "Configuration file with list of parameters and values (an alternative to using command-line options)");
    Option nsFile = new Option(NS_FILE, true, "File with node configuration of all name servers");
    Option port = new Option(PORT, true, "Port");
    Option fileLoggingLevel = new Option(FILE_LOGGING_LEVEL, true, "Verbosity level of log file. Should be one of SEVERE, WARNING, INFO, CONFIG, FINE, FINER, FINEST.");
    Option consoleOutputLevel = new Option(CONSOLE_OUTPUT_LEVEL, true, "Verbosity level of console output. Should be one of SEVERE, WARNING, INFO, CONFIG, FINE, FINER, FINEST.");
    Option debug = new Option(DEBUG, "Enables debugging output");
    Option debugPing = new Option(DEBUG_PING, "Enables debugging output for PingManager");
    Option debugMisc = new Option(DEBUG_MISC, "Enables debugging output for miscellaneous subsystems");
    Option disableSSL = new Option(DISABLE_SSL, "disables SSL authentication of LNS to server commands");
    
    Options commandLineOptions = new Options();
    commandLineOptions.addOption(configFile);
    commandLineOptions.addOption(help);
    commandLineOptions.addOption(nsFile);
    commandLineOptions.addOption(port);
    commandLineOptions.addOption(debug);
    commandLineOptions.addOption(debugPing);
    commandLineOptions.addOption(debugMisc);
    commandLineOptions.addOption(fileLoggingLevel);
    commandLineOptions.addOption(consoleOutputLevel);
    commandLineOptions.addOption(disableSSL);

    return commandLineOptions;
  }

  private static boolean initialized = false;
  
  /**
   * Controls whether SSL is used for comms with the servers.
   */
  public static boolean disableSSL = false;

  /**
   * Initializes global parameter options from command line and config file options
   * that are not handled elsewhere.
   *
   * @param allValues
   */
  public static synchronized void initializeFromOptions(Map<String, String> allValues) {
    if (initialized) {
      return;
    }

    initialized = true;
    if (allValues == null) {
      return;
    }
    
    if (!allValues.containsKey(DISABLE_SSL)) {
      disableSSL = false;
      ReconfigurationConfig.setClientPortOffset(100);
      System.out.println("LNS: SSL is enabled");
    } else {
      disableSSL = true;
      System.out.println("LNS: SSL is disabled");
    }

    if (isOptionTrue(DEBUG, allValues)) {
      LocalNameServer.debuggingEnabled = true;
      System.out.println("******** DEBUGGING IS ENABLED IN LocalNameServer *********");
    }
    
    if (isOptionTrue(DEBUG_PING, allValues)) {
      PingManager.debuggingEnabled = true;
      System.out.println("******** DEBUGGING IS ENABLED IN PingManager *********");
    }
    
    if (isOptionTrue(DEBUG_MISC, allValues)) {
      System.out.println("******** DEBUGGING IS ENABLED IN ProtocolExecutor *********");
      ProtocolExecutor.getLogger().setLevel(Level.INFO);
    } else {
      ProtocolExecutor.getLogger().setLevel(Level.WARNING);
    }

    if (allValues.containsKey(CONSOLE_OUTPUT_LEVEL)) {
      String levelString = allValues.get(CONSOLE_OUTPUT_LEVEL);
      try {
        Level level = Level.parse(levelString);
        // until a better way comes along
        LocalNameServer.LOG.setLevel(level);
      } catch (Exception e) {
        LocalNameServer.LOG.setLevel(DEFAULTCONSOLELEVEL);
        System.out.println("Could not parse " + levelString
                + "; set LocalNameServer log level to default level " + DEFAULTCONSOLELEVEL);
      }
    }
  }
 
}
