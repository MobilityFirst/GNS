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

  //FIXME: The owner of this should move it into GNSConfig
  /**
   * Enable active code.
   */
  public static boolean enableActiveCode = false;
  //FIXME: The owner of this should move it into GNSConfig
  /**
   * Number of active code worker.
   */
  public static int activeCodeWorkerCount = 1;
  //FIXME: The owner of this should move it into GNSConfig
  /**
   * How long (in seconds) to blacklist active code.
   */
  public static long activeCodeBlacklistSeconds = 10;

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
    
    
    Options commandLineOptions = new Options();
    commandLineOptions.addOption(configFile);
    commandLineOptions.addOption(help);

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

    if (allValues.containsKey(ACTIVE_CODE_WORKER_COUNT)) {
      activeCodeWorkerCount = Integer.parseInt(allValues.get(ACTIVE_CODE_WORKER_COUNT));
    }

    if (allValues.containsKey(ENABLE_ACTIVE_CODE)) {
      enableActiveCode = true;
    }
  }
  
}
