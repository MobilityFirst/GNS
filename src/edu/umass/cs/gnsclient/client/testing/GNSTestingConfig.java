/* Copyright (c) 2015 University of Massachusetts
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 * 
 * Initial developer(s): V. Arun */
package edu.umass.cs.gnsclient.client.testing;

import java.io.IOException;
import edu.umass.cs.gigapaxos.testing.TESTPaxosConfig;
import edu.umass.cs.reconfiguration.testing.TESTReconfigurationConfig;
import edu.umass.cs.utils.Config;

/**
 * @author V. Arun
 *
 * Configuration parameters for testing GNS performance and
 * functionality. Mainly for holding testing parameters other than those
 * already in gigaapxos' {@link TESTPaxosConfig} and
 * {@link TESTReconfigurationConfig}.
 */
public class GNSTestingConfig {

  /**
   * Load.
   */
  protected static void load() {
    // testing specific config parameters
    try {
      Config.register(GNSTC.class, TESTING_CONFIG_FILE_KEY,
              DEFAULT_TESTING_CONFIG_FILE);
    } catch (IOException e) {
      // ignore as defaults will be used
    }
  }

  static {
    load();
  }

  /**
   *
   */
  public static final String TESTING_CONFIG_FILE_KEY = "testingConfig";
  /**
   *
   */
  public static final String DEFAULT_TESTING_CONFIG_FILE = "testing.properties";

  /**
   * GNS testing config parameters.
   */
  public static enum GNSTC implements Config.ConfigurableEnum {

    /**
     * Number of GUIDs per account GUID.
     */
    NUM_GUIDS_PER_ACCOUNT(100),
    /**
     * Use only account GUIDs, no sub-GUIDs.
     */
    ACCOUNT_GUIDS_ONLY(false),
    /**
     *
     */
    ACCOUNT_GUID_PREFIX("ACCOUNT_GUID"),
    /**
     * If enabled, the GNS will cache and return the same value for reads.
     *
     * Code-breaking if enabled. Meant only for instrumentation.
     */
    EXECUTE_NOOP_ENABLED(false),

	  /**
	   * Will cleanup all created state (but not paxos logs) by default.
	   */
	  CLEANUP (true),

	  ;

    final Object defaultValue;

    GNSTC(Object defaultValue) {
      this.defaultValue = defaultValue;
    }

    /**
     *
     * @return the default value
     */
    @Override
    public Object getDefaultValue() {
      return this.defaultValue;
    }

    /**
     *
     * @return the config file key
     */
    @Override
    public String getConfigFileKey() {
      return "testingConfig";
    }

    /**
     *
     * @return the default config file
     */
    @Override
    public String getDefaultConfigFile() {
      return "testing.properties";
    }
  }
}
