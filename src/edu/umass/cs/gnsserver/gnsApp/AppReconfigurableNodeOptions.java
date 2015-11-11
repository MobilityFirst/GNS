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
 *  Initial developer(s): Abhigyan Sharma, Westy
 *
 */
package edu.umass.cs.gnsserver.gnsApp;

import static edu.umass.cs.gnscommon.GnsProtocol.HELP;
import edu.umass.cs.gigapaxos.PaxosManager;
import edu.umass.cs.gnsserver.main.GNS;
import edu.umass.cs.gnsserver.utils.Logging;
import edu.umass.cs.reconfiguration.ReconfigurationConfig;
import edu.umass.cs.reconfiguration.Reconfigurator;
import static edu.umass.cs.gnsserver.utils.ParametersAndOptions.CONFIG_FILE;
import static edu.umass.cs.gnsserver.utils.ParametersAndOptions.isOptionTrue;
import edu.umass.cs.nio.NIOTransport;
import static edu.umass.cs.nio.SSLDataProcessingWorker.SSL_MODES.*;
import edu.umass.cs.protocoltask.ProtocolExecutor;
import java.util.Map;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
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

  /**
   * The minimum number of replicas. Used by {@link LocationBasedDemandProfile}.
   */
  public static int minReplica = 3;
  /**
   * The maximum number of replicas. Used by {@link LocationBasedDemandProfile}.
   */
  public static int maxReplica = 100;
  /**
   * Determines the number of replicas based on ratio of lookups to writes.
   * Used by {@link LocationBasedDemandProfile}.
   */
  public static double normalizingConstant = 0.5;

  /**
   * Fixed timeout after which a query is retransmitted.
   */
  public static int queryTimeout = GNS.DEFAULT_QUERY_TIMEOUT;

  //  Abhigyan: parameters related to retransmissions.
  //  If adaptive timeouts are used, see more parameters in util.AdaptiveRetransmission.java
  /**
   * Maximum time a local name server waits for a response from name server query is logged as failed after this.
   */
  public static int maxQueryWaitTime = GNS.DEFAULT_MAX_QUERY_WAIT_TIME;

  /**
   * If this is true sending of email by the verification mechanism is disabled.
   */
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
  /**
   * Set this to true to enable additional debugging output.
   */
  public static boolean debuggingEnabled = false;
  /**
   * If this is true the app will handle all operations locally (ie., it won't send request to reconfigurators).
   */
  public static boolean standAloneApp = false;
  /**
   * If this is true SSL will not be used for communications between servers.
   */
  public static boolean disableSSL = false;
  /**
   * Number of active code worker.
   */
  public static int activeCodeWorkerCount = 1;
  /**
   * How long (in seconds) to blacklist active code.
   */
  public static long activeCodeBlacklistSeconds = 10;

  // Command line and config file options
  // If you change this list, change it below in getAllOptions as well.
  /**
   * ID
   */
  public static final String ID = "id";
  /**
   * NS_FILE
   */
  public static final String NS_FILE = "nsfile";
  /**
   * FILE_LOGGING_LEVEL
   */
  public static final String FILE_LOGGING_LEVEL = "fileLoggingLevel";
  /**
   * CONSOLE_OUTPUT_LEVEL
   */
  public static final String CONSOLE_OUTPUT_LEVEL = "consoleOutputLevel";
  /**
   * DEBUG
   */
  public static final String DEBUG = "debug"; // for backwards compat
  /**
   * DEBUG_APP
   */
  public static final String DEBUG_APP = "debugAPP";
  /**
   * DEBUG_AR
   */
  public static final String DEBUG_AR = "debugAR";
  /**
   * DEBUG_RECON
   */
  public static final String DEBUG_RECON = "debugRecon";
  /**
   * DEBUG_PAXOS
   */
  public static final String DEBUG_PAXOS = "debugPaxos";
  /**
   * DEBUG_NIO
   */
  public static final String DEBUG_NIO = "debugNio";
  /**
   * DEBUG_MISC
   */
  public static final String DEBUG_MISC = "debugMisc";
  /**
   * TEST. This option is used to create multiple nodes on a single host
   */
  public static final String TEST = "test";
  /**
   * STANDALONE
   */
  public static final String STANDALONE = "standalone";
  /**
   * DEMAND_PROFILE_CLASS
   */
  public static final String DEMAND_PROFILE_CLASS = "demandProfileClass";
  // for CCP
  /**
   * DNS_GNS_ONLY
   */
  public static final String DNS_GNS_ONLY = "dnsGnsOnly";
  /**
   * DNS_ONLY
   */
  public static final String DNS_ONLY = "dnsOnly";
  /**
   * GNS_SERVER_IP
   */
  public static final String GNS_SERVER_IP = "gnsServerIP";
  /**
   * DISABLE_SSL
   */
  public static final String DISABLE_SSL = "disableSSL";
   /**
   * DISABLE_EMAIL_VERIFICATION
   */
  public static final String DISABLE_EMAIL_VERIFICATION = "disableEmailVerification";
  
  private static final String ACTIVE_CODE_WORKER_COUNT = "activeCodeWorkerCount";

  /**
   * Returns all the options.
   *
   * @return all the options
   */
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
    Option debugNio = new Option(DEBUG_NIO, "Enables debugging output for Nio");
    Option debugMisc = new Option(DEBUG_MISC, "Enables debugging output for all miscellaneous utilities");
    Option test = new Option(TEST, "Runs multiple test nodes on one machine");
    Option standAlone = new Option(STANDALONE, "Runs the app as a standalone module");
    Option demandProfileClass = new Option(DEMAND_PROFILE_CLASS, true, "The class to use for the demand profile");
    // for CCP
    Option dnsGnsOnly = new Option(DNS_GNS_ONLY, "With this option DNS server only does lookup in GNS server.");
    Option dnsOnly = new Option(DNS_ONLY, "With this option name server forwards requests to DNS and GNS servers.");
    Option gnsServerIP = new Option(GNS_SERVER_IP, "gns server to use");
    Option disableSSL = new Option(DISABLE_SSL, "disables SSL authentication of client to server commands");
    Option disableEmailVerification = new Option(DISABLE_EMAIL_VERIFICATION, "disables email verification of new account guids");

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
    commandLineOptions.addOption(debugNio);
    commandLineOptions.addOption(debugMisc);
    commandLineOptions.addOption(test);
    commandLineOptions.addOption(standAlone);
    commandLineOptions.addOption(demandProfileClass);
    // for CCP
    commandLineOptions.addOption(dnsGnsOnly);
    commandLineOptions.addOption(dnsOnly);
    commandLineOptions.addOption(gnsServerIP);
    commandLineOptions.addOption(disableSSL);
    commandLineOptions.addOption(disableEmailVerification);

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

    // make sure this has been initialized
    GNS.getLogger();

    if (!allValues.containsKey(DISABLE_SSL)) {
      disableSSL = false;
      ReconfigurationConfig.setClientPortOffset(100);
      ReconfigurationConfig.setClientSSLMode(SERVER_AUTH);
      ReconfigurationConfig.setServerSSLMode(MUTUAL_AUTH);
      System.out.println("NS: SSL is enabled");
    } else {
      disableSSL = true;
      ReconfigurationConfig.setClientPortOffset(0);
      ReconfigurationConfig.setClientSSLMode(CLEAR);
      ReconfigurationConfig.setServerSSLMode(CLEAR);
      System.out.println("NS: SSL is disabled");
    }
    
     if (isOptionTrue(DISABLE_EMAIL_VERIFICATION, allValues)) {
       System.out.println("******** Email Verification is OFF *********");
       GNS.enableEmailAccountVerification = false;
     }
    

    if (isOptionTrue(DEBUG, allValues) || isOptionTrue(DEBUG_APP, allValues)) {
      debuggingEnabled = true;
      System.out.println("******** DEBUGGING IS ENABLED IN THE APP *********");
    }

    if (isOptionTrue(DEBUG_AR, allValues)) {
      System.out.println("******** DEBUGGING IS ENABLED IN THE ACTIVE REPLICA *********");
      // For backwards compatibility until Config goes away
    } else {
    }

    if (isOptionTrue(DEBUG_RECON, allValues)) {
      System.out.println("******** DEBUGGING IS ENABLED IN THE RECONFIGURATOR *********");
      // For backwards compatibility until Config goes away

      ConsoleHandler handler = Logging.getConsoleHandler();
      handler.setLevel(Level.FINEST);
      Logger log = Reconfigurator.getLogger();
      log.addHandler(handler);
      log.setLevel(Level.FINER);
    } else {
      Reconfigurator.getLogger().setLevel(Level.INFO);
    }

    if (isOptionTrue(DEBUG_PAXOS, allValues)) {
      System.out.println("******** DEBUGGING IS ENABLED IN PAXOS *********");
      // For backwards compatibility until Config goes away
      PaxosManager.getLogger().setLevel(Level.INFO);
    } else {
      PaxosManager.getLogger().setLevel(Level.WARNING);
    }

    if (isOptionTrue(DEBUG_NIO, allValues)) {
      System.out.println("******** DEBUGGING IS ENABLED IN THE NIOTransport *********");
      //For backwards compatibility until Config goes away
      ConsoleHandler handler = Logging.getConsoleHandler();
      handler.setLevel(Level.FINEST);
      Logger log = NIOTransport.getLogger();
      log.addHandler(handler);
      log.setLevel(Level.INFO);
    } else {
      NIOTransport.getLogger().setLevel(Level.WARNING);
    }

    if (isOptionTrue(DEBUG_MISC, allValues)) {
      System.out.println("******** DEBUGGING IS ENABLED IN THE ProtocolExecutor *********");
      // For backwards compatibility until Config goes away
//      ConsoleHandler handler = new ConsoleHandler();
//      handler.setLevel(Level.FINEST);
//      Logger log = ProtocolExecutor.getLogger();
//      log.addHandler(handler);
      ProtocolExecutor.getLogger().setLevel(Level.INFO);
    } else {
      ProtocolExecutor.getLogger().setLevel(Level.WARNING);
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
    
    if (allValues.containsKey(ACTIVE_CODE_WORKER_COUNT)) {
    	activeCodeWorkerCount = Integer.parseInt(allValues.get(ACTIVE_CODE_WORKER_COUNT));
    }

  }

}
