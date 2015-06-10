/*
 * Copyright (C) 2015
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.newApp;

import static edu.umass.cs.gns.newApp.clientCommandProcessor.commandSupport.GnsProtocolDefs.HELP;
import edu.umass.cs.gigapaxos.PaxosManager;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.reconfiguration.AbstractReconfiguratorDB;
import edu.umass.cs.reconfiguration.ActiveReplica;
import edu.umass.cs.reconfiguration.DerbyPersistentReconfiguratorDB;
import edu.umass.cs.reconfiguration.ReconfigurationConfig;
import edu.umass.cs.reconfiguration.Reconfigurator;
import static edu.umass.cs.gns.util.ParametersAndOptions.CONFIG_FILE;
import static edu.umass.cs.gns.util.ParametersAndOptions.isOptionTrue;

import edu.umass.cs.nio.NIOTransport;
import edu.umass.cs.protocoltask.ProtocolExecutor;
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

  // "Global" parameters
  /**
   * The port used by Mongo.
   */
  public static int mongoPort = 27017;
  /**
   * Controls whether DNS reads can read fields from group guids.
   */
  public static boolean allowGroupGuidIndirection = true;

  // parameter related to replication of records
  public static double normalizingConstant = 0.5;
  public static int minReplica = 3;
  public static int maxReplica = 100;

  /**
   * Fixed timeout after which a query retransmitted.
   */
  public static int queryTimeout = GNS.DEFAULT_QUERY_TIMEOUT;

  public static boolean replicateAll = false;
  //  Abhigyan: parameters related to retransmissions.
  //  If adaptive timeouts are used, see more parameters in util.AdaptiveRetransmission.java
  /**
   * Maximum time a local name server waits for a response from name server query is logged as failed after this.
   */
  public static int maxQueryWaitTime = GNS.DEFAULT_MAX_QUERY_WAIT_TIME;
  public static boolean noEmail = false;
  /**
   * Set to true if you want the DNS server to not lookup records using DNS (will only lookup records in the GNS).
   */
  public static boolean dnsGnsOnly = false;
  /**
   * Name of the GNS server to forward GNS requests for Local Name server.
   */
  public static String gnsServerIP = null;
  /**
   * Set to true if you want the DNS server to not lookup records using DNS (will only lookup records in the GNS).
   */
  public static boolean dnsOnly = false;

  public static boolean debuggingEnabled = false;

  public static boolean standAloneApp = false;

  // Command line and config file options
  // If you change this list, change it below in getAllOptions as well.
  public static final String ID = "id";
  public static final String NS_FILE = "nsfile";
  public static final String FILE_LOGGING_LEVEL = "fileLoggingLevel";
  public static final String CONSOLE_OUTPUT_LEVEL = "consoleOutputLevel";
  public static final String DEBUG = "debug"; // for backwards compat
  public static final String DEBUG_APP = "debugAPP";
  public static final String DEBUG_AR = "debugAR";
  public static final String DEBUG_RECON = "debugRecon";
  public static final String DEBUG_PAXOS = "debugPaxos";
  public static final String DEBUG_MISC = "debugMisc";
  public static final String TEST = "test";
  public static final String STANDALONE = "standAlone";
  public static final String DEMAND_PROFILE_CLASS = "demandProfileClass";
  // for CCP
  public static final String DNS_GNS_ONLY = "dnsGnsOnly";
  public static final String DNS_ONLY = "dnsOnly";
  public static final String GNS_SERVER_IP = "gnsServerIP";

  public static Options getAllOptions() {
    Option help = new Option(HELP, "Prints usage");
    Option configFile = new Option(CONFIG_FILE, true, "Configuration file with list of parameters and values (an alternative to using command-line options)");
    Option nodeId = new Option(ID, true, "Node ID");
    Option nsFile = new Option(NS_FILE, true, "File with node configuration of all name servers");
    Option fileLoggingLevel = new Option(FILE_LOGGING_LEVEL, true, "Verbosity level of log file. Should be one of SEVERE, WARNING, INFO, CONFIG, FINE, FINER, FINEST.");
    Option consoleOutputLevel = new Option(CONSOLE_OUTPUT_LEVEL, true, "Verbosity level of console output. Should be one of SEVERE, WARNING, INFO, CONFIG, FINE, FINER, FINEST.");
    Option debug = new Option(DEBUG, "Enables debugging for everything");
    Option debugApp = new Option(DEBUG_APP, "Enables debugging output for the app");
    Option debugAR = new Option(DEBUG_AR, "Enables debugging output for the Active Replica");
    Option debugRecon = new Option(DEBUG_RECON, "Enables debugging output for the Reconfigurator");
    Option debugPaxos = new Option(DEBUG_PAXOS, "Enables debugging output for Paxos");
    Option debugMisc = new Option(DEBUG_MISC, "Enables debugging output for all miscellaneous utilities");
    Option test = new Option(TEST, "Runs multiple test nodes on one machine");
    Option standAlone = new Option(STANDALONE, "Runs the app as a standalone module");
    Option demandProfileClass = new Option(DEMAND_PROFILE_CLASS, true, "The class to use for the demand profile");
    // for CCP
    Option dnsGnsOnly = new Option(DNS_GNS_ONLY, "With this option DNS server only does lookup in GNS server.");
    Option dnsOnly = new Option(DNS_ONLY, "With this option name server forwards requests to DNS and GNS servers.");
    Option gnsServerIP = new Option(GNS_SERVER_IP, "gns server to use");

    Options commandLineOptions = new Options();
    commandLineOptions.addOption(configFile);
    commandLineOptions.addOption(help);
    commandLineOptions.addOption(nodeId);
    commandLineOptions.addOption(nsFile);
    commandLineOptions.addOption(fileLoggingLevel);
    commandLineOptions.addOption(consoleOutputLevel);
    commandLineOptions.addOption(debug);
    commandLineOptions.addOption(debugApp);
    commandLineOptions.addOption(debugAR);
    commandLineOptions.addOption(debugRecon);
    commandLineOptions.addOption(debugPaxos);
    commandLineOptions.addOption(debugMisc);
    commandLineOptions.addOption(test);
    commandLineOptions.addOption(standAlone);
    commandLineOptions.addOption(demandProfileClass);
    // for CCP
    commandLineOptions.addOption(dnsGnsOnly);
    commandLineOptions.addOption(dnsOnly);
    commandLineOptions.addOption(gnsServerIP);

    return commandLineOptions;

  }

  private static boolean initialized = false;

  /**
   * Initializes parameter options from command line and config file options
   * for AppReconfigurableNode.
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

    if (isOptionTrue(DEBUG, allValues) || isOptionTrue(DEBUG_APP, allValues)) {
      debuggingEnabled = true;
      System.out.println("******** DEBUGGING IS ENABLED IN THE APP *********");
    }

    if (isOptionTrue(DEBUG_AR, allValues)) {
      System.out.println("******** DEBUGGING IS ENABLED IN THE ACTIVE REPLICA *********");
      // For backwards compatibility until Config goes away
      ActiveReplica.log.setLevel(Level.INFO);
    } else {
      ActiveReplica.log.setLevel(Level.WARNING);
    }

    if (isOptionTrue(DEBUG_RECON, allValues)) {
      System.out.println("******** DEBUGGING IS ENABLED IN THE RECONFIGURATOR *********");
      // For backwards compatibility until Config goes away
      Reconfigurator.getLogger().setLevel(Level.INFO);
      AbstractReconfiguratorDB.log.setLevel(Level.INFO);
      DerbyPersistentReconfiguratorDB.log.setLevel(Level.INFO);
    } else {
      Reconfigurator.getLogger().setLevel(Level.WARNING);
      AbstractReconfiguratorDB.log.setLevel(Level.WARNING);
      DerbyPersistentReconfiguratorDB.log.setLevel(Level.WARNING);
    }

    if (isOptionTrue(DEBUG_PAXOS, allValues)) {
      System.out.println("******** DEBUGGING IS ENABLED IN PAXOS *********");
      // For backwards compatibility until Config goes away
      PaxosManager.getLogger().setLevel(Level.INFO);
    } else {
      PaxosManager.getLogger().setLevel(Level.WARNING);
    }
    
    if (isOptionTrue(DEBUG_MISC, allValues)) {
      System.out.println("******** DEBUGGING IS ENABLED IN THE NIOTransport *********");
      System.out.println("******** DEBUGGING IS ENABLED IN THE ProtocolExecutor *********");
      // For backwards compatibility until Config goes away
      ProtocolExecutor.getLogger().setLevel(Level.INFO);
      NIOTransport.getLogger().setLevel(Level.INFO);
    } else {
      ProtocolExecutor.getLogger().setLevel(Level.WARNING);
      NIOTransport.getLogger().setLevel(Level.WARNING);
    }

    if (allValues.containsKey(FILE_LOGGING_LEVEL)) {
      GNS.fileLoggingLevel = allValues.get(FILE_LOGGING_LEVEL);

    }
    if (allValues.containsKey(CONSOLE_OUTPUT_LEVEL)) {
      String levelString = allValues.get(CONSOLE_OUTPUT_LEVEL);
      GNS.consoleOutputLevel = levelString;
    }
    if (allValues.containsKey(STANDALONE)) {
      standAloneApp = true;
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
    
    ReconfigurationConfig.setReconfigureInPlace(false);
    System.out.println("Reconfigure in place is: " + ReconfigurationConfig.shouldReconfigureInPlace());

    // CCP options
    if (allValues.containsKey(DNS_GNS_ONLY)) {
      dnsGnsOnly = true;
    }
    if (allValues.containsKey(DNS_ONLY)) {
      dnsOnly = true;
    }
    if (allValues.containsKey(GNS_SERVER_IP)) {
      gnsServerIP = allValues.get(GNS_SERVER_IP);
    }
  }

}
