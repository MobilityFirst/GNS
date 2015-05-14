/*
 * Copyright (C) 2015
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.newApp;

import static edu.umass.cs.gns.clientsupport.Defs.HELP;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nsdesign.Config;
import edu.umass.cs.gns.reconfiguration.AbstractReconfiguratorDB;
import edu.umass.cs.gns.reconfiguration.DerbyPersistentReconfiguratorDB;
import edu.umass.cs.gns.reconfiguration.ReconfigurationConfig;
import edu.umass.cs.gns.reconfiguration.Reconfigurator;
import static edu.umass.cs.gns.util.Logging.DEFAULTCONSOLELEVEL;
import static edu.umass.cs.gns.util.ParametersAndOptions.CONFIG_FILE;
import java.util.Map;
import java.util.logging.Level;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

/**
 * The command line options for AppReconfigurableNode.
 *
 * @author westy
 */
public class AppReconfigurableNodeOptions {

  // If you change this list, change it below in getAllOptions as well.
  public static final String ID = "id";
  public static final String NS_FILE = "nsfile";
  public static final String FILE_LOGGING_LEVEL = "fileLoggingLevel";
  public static final String CONSOLE_OUTPUT_LEVEL = "consoleOutputLevel";
  public static final String DEBUG = "debug";
  public static final String TEST = "test";
  public static final String DEMAND_PROFILE_CLASS = "demandProfileClass";

  public static Options getAllOptions() {
    Option help = new Option(HELP, "Prints usage");
    Option configFile = new Option(CONFIG_FILE, true, "Configuration file with list of parameters and values (an alternative to using command-line options)");
    Option nodeId = new Option(ID, true, "Node ID");
    Option nsFile = new Option(NS_FILE, true, "File with node configuration of all name servers");
    Option fileLoggingLevel = new Option(FILE_LOGGING_LEVEL, true, "Verbosity level of log file. Should be one of SEVERE, WARNING, INFO, CONFIG, FINE, FINER, FINEST.");
    Option consoleOutputLevel = new Option(CONSOLE_OUTPUT_LEVEL, true, "Verbosity level of console output. Should be one of SEVERE, WARNING, INFO, CONFIG, FINE, FINER, FINEST.");
    Option debug = new Option(DEBUG, "Enables debugging output");
    Option test = new Option(TEST, "Runs multiple test nodes on one machine");
    Option demandProfileClass = new Option(DEMAND_PROFILE_CLASS, "The class to use for the demand profile");

    Options commandLineOptions = new Options();
    commandLineOptions.addOption(configFile);
    commandLineOptions.addOption(help);
    commandLineOptions.addOption(nodeId);
    commandLineOptions.addOption(nsFile);
    commandLineOptions.addOption(fileLoggingLevel);
    commandLineOptions.addOption(consoleOutputLevel);
    commandLineOptions.addOption(debug);
    commandLineOptions.addOption(test);
    commandLineOptions.addOption(demandProfileClass);

    return commandLineOptions;

  }

  private static boolean initialized = false;

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

    if (allValues.containsKey(DEBUG)) {
      // For backwards compatibility until Config goes away
      Config.debuggingEnabled = true;
    }

    if (allValues.containsKey(FILE_LOGGING_LEVEL)) {
      GNS.fileLoggingLevel = allValues.get(FILE_LOGGING_LEVEL);

    }
    if (allValues.containsKey(CONSOLE_OUTPUT_LEVEL)) {
      String levelString = allValues.get(CONSOLE_OUTPUT_LEVEL);
      GNS.consoleOutputLevel = levelString;
      try {
        Level level = Level.parse(levelString);
        // until a better way comes along
        Reconfigurator.log.setLevel(level);
        AbstractReconfiguratorDB.log.setLevel(level);
        DerbyPersistentReconfiguratorDB.log.setLevel(level);
        //PaxosInstanceStateMachine
        //DerbyPaxosLogger
        System.out.println("Set Reconfiguration log level to " + levelString);
      } catch (Exception e) {
        Reconfigurator.log.setLevel(DEFAULTCONSOLELEVEL);
        System.out.println("Could not parse " + levelString
                + "; set Reconfigurator log level to default level " + DEFAULTCONSOLELEVEL);
      }
    }

    boolean demandProfileSet = false;
    if (allValues.containsKey(DEMAND_PROFILE_CLASS)) {
      String className = allValues.get(DEMAND_PROFILE_CLASS);
      try {
        Class klass = Class.forName(className);
        ReconfigurationConfig.setDemandProfile(klass);
        demandProfileSet = true;
      } catch (ClassNotFoundException e) {
        System.out.println("Demand profile class " + className + " not found");
      }
    }
    if (!demandProfileSet) {
      // FIXME: Make this the value of DEFAULT_DEMAND_PROFILE_TYPE?
      ReconfigurationConfig.setDemandProfile(LocationBasedDemandProfile.class);
    }
    System.out.println("Set demand profile: " + ReconfigurationConfig.getDemandProfile());

  }

}
