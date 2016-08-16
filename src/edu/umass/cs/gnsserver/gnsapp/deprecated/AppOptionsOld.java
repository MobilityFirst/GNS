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
package edu.umass.cs.gnsserver.gnsapp.deprecated;

import static edu.umass.cs.gnsserver.utils.ParametersAndOptions.CONFIG_FILE;
import static edu.umass.cs.gnsserver.utils.ParametersAndOptions.isOptionTrue;

import java.util.Map;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import edu.umass.cs.gnsserver.gnsapp.LocationBasedDemandProfile;

/**
 * The command line options for AppReconfigurableNode.
 *
 * @author westy, arun
 */
// FIXME: Port the rest of these to the config style above or delete them if they are unused.
@Deprecated
public class AppOptionsOld {

  //FIXME: The code that implements this has disappeared. See MOB-803
  /**
   * Controls whether reads can read fields from group guids.
   */
  public static boolean allowGroupGuidIndirection = true;

  //FIXME: Do this have an equivalent in gigapaxos we can use.
  /**
   * The minimum number of replicas. Used by {@link LocationBasedDemandProfile}.
   */
  public static int minReplica = 3;
  //FIXME: Do this have an equivalent in gigapaxos we can use.
  /**
   * The maximum number of replicas. Used by {@link LocationBasedDemandProfile}.
   */
  public static int maxReplica = 100;
  //FIXME: Do this have an equivalent in gigapaxos we can use.
  /**
   * Determines the number of replicas based on ratio of lookups to writes.
   * Used by {@link LocationBasedDemandProfile}.
   */
  public static double normalizingConstant = 0.5;
  
//  /**
//   * Set to true if you want the DNS server to not lookup records using DNS (will only lookup records in the GNS).
//   */
//  public static boolean dnsGnsOnly = false;
//  /**
//   * Name of the GNS server to forward GNS requests for Local Name server.
//   */
//  public static String gnsServerIP = null;
//  /**
//   * Set to true if you want the DNS server to not lookup records using DNS (will only lookup records in the GNS).
//   */
//  public static boolean dnsOnly = false;
  /**
   * If this is true the app will handle all operations locally (ie., it won't send request to reconfigurators).
   */
  public static boolean standAloneApp = false;

  //FIXME: The owner of this should move it into GNSConfig
  /**
   * Enable active code.
   */
  public static boolean enableActiveCode = true;

  //FIXME: The owner of this should move it into GNSConfig
  /**
   * FIXME: this hardcoded path should be able to 
   */
  public static String activeConfigFile = "activeConfig";
  
  // context service options
  public static boolean enableContextService = false;

  //FIXME: The owner of this should move it into GNSConfig
  // ip port of one node read from config files.
  public static String contextServiceIpPort = "";


  // Command line and config file options
  // If you change this list, change it below in getAllOptions as well.
//  /**
//   * ID
//   */
//  public static final String ID = "id";
//  /**
//   * NS_FILE
//   */
//  public static final String NS_FILE = "nsfile";
// 
//  /**
//   * TEST. This option is used to create multiple nodes on a single host
//   */
//  public static final String TEST = "test";
//  /**
//   * STANDALONE
//   */
//  public static final String STANDALONE = "standalone";
  /**
   * DNS_GNS_ONLY
   */
//  public static final String DNS_GNS_ONLY = "dnsGnsOnly";
//  /**
//   * DNS_ONLY
//   */
//  public static final String DNS_ONLY = "dnsOnly";
//  /**
//   * GNS_SERVER_IP
//   */
//  public static final String GNS_SERVER_IP = "gnsServerIP";

  private static final String ENABLE_ACTIVE_CODE = "enableActiveCode";
  
  public static final String ACTIVE_CONFIG_FILE = "activeConfigFile";
  
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
    //Option nodeId = new Option(ID, true, "Node ID");

//    Option dnsGnsOnly = new Option(DNS_GNS_ONLY, "With this option DNS server only does lookup in GNS server.");
//    Option dnsOnly = new Option(DNS_ONLY, "With this option name server forwards requests to DNS and GNS servers.");
//    Option gnsServerIP = new Option(GNS_SERVER_IP, "gns server to use");

    Option enableContextService = new Option(ENABLE_CONTEXT_SERVICE, "if true enables context service on nameserver. Set in ns properties file");
    Option contextServiceHostPort = new Option(CONTEXT_SERVICE_IP_PORT, "must be set if enableContextService is set to true. It gives the host port information of one context service node. Similar to LNS "
            + "information of GNS");

    Options commandLineOptions = new Options();
    commandLineOptions.addOption(configFile);
    commandLineOptions.addOption(help);
    //commandLineOptions.addOption(nodeId);

//    commandLineOptions.addOption(dnsGnsOnly);
//    commandLineOptions.addOption(dnsOnly);
//    commandLineOptions.addOption(gnsServerIP);

    //context service options
    commandLineOptions.addOption(enableContextService);
    commandLineOptions.addOption(contextServiceHostPort);
    Option enableActiveCode = new Option(ENABLE_ACTIVE_CODE, "set to true to enable active code service on the server side");
    // active code
    commandLineOptions.addOption(enableActiveCode);
    
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

//    if (isOptionTrue(DISABLE_EMAIL_VERIFICATION, allValues)) {
//      System.out.println("******** Email Verification is OFF *********");
//      GNSConfig.enableEmailAccountVerification = false;
//    }

//    // APP options
//    if (allValues.containsKey(DNS_GNS_ONLY)) {
//      dnsGnsOnly = true;
//    }
//    if (allValues.containsKey(DNS_ONLY)) {
//      dnsOnly = true;
//    }
//    if (allValues.containsKey(GNS_SERVER_IP)) {
//      gnsServerIP = allValues.get(GNS_SERVER_IP);
//    }

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
