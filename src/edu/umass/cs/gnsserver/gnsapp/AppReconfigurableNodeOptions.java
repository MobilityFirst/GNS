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
 *  Initial developer(s): Westy
 *
 */
package edu.umass.cs.gnsserver.gnsapp;

import edu.umass.cs.gigapaxos.PaxosConfig;
//import static edu.umass.cs.gnscommon.GNSCommandProtocol.HELP;
import edu.umass.cs.gnsserver.main.GNSConfig;
import edu.umass.cs.gnsserver.main.GNSConfig.GNSC;
import static edu.umass.cs.gnsserver.utils.ParametersAndOptions.CONFIG_FILE;
import static edu.umass.cs.gnsserver.utils.ParametersAndOptions.isOptionTrue;
import edu.umass.cs.utils.Config;

import java.util.Map;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

/**
 * The command line options for AppReconfigurableNode.
 *
 * @author westy, arun
 */
public class AppReconfigurableNodeOptions {

  public static void load() {
	  // arun: the commented lines are not necessary
    //PaxosConfig.load();
    //PaxosConfig.load(ReconfigurationConfig.RC.class);
    PaxosConfig.load(AppReconfigurableNodeOptions.AppConfig.class);
  }

  static {
    load();
  }

  public static enum AppConfig implements Config.DefaultValueEnum {

    NOSQL_RECORDS_CLASS("edu.umass.cs.gnsserver.database.MongoRecords");

    final Object defaultValue;

    AppConfig(Object defaultValue) {
      this.defaultValue = defaultValue;
    }

    @Override
    public Object getDefaultValue() {
      return this.defaultValue;
    }
  }

  private static Class<?> noSqlRecordsclass = getNoSqlRecordsClass();

  private static Class<?> getClassSuppressExceptions(String className) {
    Class<?> clazz = null;
    try {
      if (className != null && !"null".equals(className)) {
        clazz = Class.forName(className);
      }
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }
    return clazz;
  }

  /**
   * @return DB class
   */
  public static Class<?> getNoSqlRecordsClass() {
    if (noSqlRecordsclass == null) {
    	// arun: in-memory DB => DiskMap
    	noSqlRecordsclass = getClassSuppressExceptions(Config
    			.getGlobalBoolean(GNSC.ENABLE_DISKMAP)
    			|| 
    			// in-memory DB force-implies DiskMap
    			Config.getGlobalBoolean(GNSConfig.GNSC.IN_MEMORY_DB) ? "edu.umass.cs.gnsserver.database.DiskMapRecords"
    					: Config.getGlobalString(AppConfig.NOSQL_RECORDS_CLASS));
    }
    return noSqlRecordsclass;
  }

  // FIXME: Port the rest of these to the config style above
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
   * If this is true the app will handle all operations locally (ie., it won't send request to reconfigurators).
   */
  public static boolean standAloneApp = false;

  /**
   * Enable active code.
   */
  public static boolean enableActiveCode = false;
  /**
   * Number of active code worker.
   */
  public static int activeCodeWorkerCount = 1;
  /**
   * How long (in seconds) to blacklist active code.
   */
  public static long activeCodeBlacklistSeconds = 10;

  // context service options
  public static boolean enableContextService = false;

  // ip port of one node read from config files.
  public static String contextServiceIpPort = "";

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
// 
  /**
   * TEST. This option is used to create multiple nodes on a single host
   */
  public static final String TEST = "test";
  /**
   * STANDALONE
   */
  public static final String STANDALONE = "standalone";
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
   * DISABLE_EMAIL_VERIFICATION
   */
  public static final String DISABLE_EMAIL_VERIFICATION = "disableEmailVerification";

  private static final String ACTIVE_CODE_WORKER_COUNT = "activeCodeWorkerCount";

  private static final String ENABLE_ACTIVE_CODE = "enableActiveCode";

  public static final String ENABLE_CONTEXT_SERVICE = "enableContextService";

  public static final String CONTEXT_SERVICE_IP_PORT = "contextServiceHostPort";

  /**
   * Returns all the options.
   *
   * @return all the options
   */
  public static Options getAllOptions() {
    Option help = new Option("help", "Prints usage");
    Option configFile = new Option(CONFIG_FILE, true, "Configuration file with list of parameters and values (an alternative to using command-line options)");
    Option nodeId = new Option(ID, true, "Node ID");
    Option nsFile = new Option(NS_FILE, true, "File with node configuration of all name servers");
    Option test = new Option(TEST, "Runs multiple test nodes on one machine");
    Option standAlone = new Option(STANDALONE, "Runs the app as a standalone module");
    // for CCP
    Option dnsGnsOnly = new Option(DNS_GNS_ONLY, "With this option DNS server only does lookup in GNS server.");
    Option dnsOnly = new Option(DNS_ONLY, "With this option name server forwards requests to DNS and GNS servers.");
    Option gnsServerIP = new Option(GNS_SERVER_IP, "gns server to use");
    Option disableEmailVerification = new Option(DISABLE_EMAIL_VERIFICATION, "disables email verification of new account guids");

    // for CS
    Option enableContextService = new Option(ENABLE_CONTEXT_SERVICE, true, "if true enables context service on nameserver. Set in ns properties file");
    Option contextServiceHostPort = new Option(CONTEXT_SERVICE_IP_PORT, true, "must be set if enableContextService is set to true. It gives the host port information of one context service node. Similar to LNS "
            + "information of GNS");

    Options commandLineOptions = new Options();
    commandLineOptions.addOption(configFile);
    commandLineOptions.addOption(help);
    commandLineOptions.addOption(nodeId);
    commandLineOptions.addOption(nsFile);
    commandLineOptions.addOption(test);
    commandLineOptions.addOption(standAlone);
    // for CCP
    commandLineOptions.addOption(dnsGnsOnly);
    commandLineOptions.addOption(dnsOnly);
    commandLineOptions.addOption(gnsServerIP);
    commandLineOptions.addOption(disableEmailVerification);

    //context service options
    commandLineOptions.addOption(enableContextService);
    commandLineOptions.addOption(contextServiceHostPort);

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

    if (isOptionTrue(DISABLE_EMAIL_VERIFICATION, allValues)) {
      System.out.println("******** Email Verification is OFF *********");
      GNSConfig.enableEmailAccountVerification = false;
    }

    // APP options
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

    if (allValues.containsKey(ENABLE_ACTIVE_CODE)) {
      enableActiveCode = true;
    }

    if (isOptionTrue(ENABLE_CONTEXT_SERVICE, allValues)) {
      enableContextService = true;
    }

    if (allValues.containsKey(CONTEXT_SERVICE_IP_PORT)) {
      contextServiceIpPort = allValues.get(CONTEXT_SERVICE_IP_PORT);
    }
  }

}
