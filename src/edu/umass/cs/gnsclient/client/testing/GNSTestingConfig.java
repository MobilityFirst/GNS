
package edu.umass.cs.gnsclient.client.testing;

import java.io.IOException;
import edu.umass.cs.gigapaxos.testing.TESTPaxosConfig;
import edu.umass.cs.reconfiguration.testing.TESTReconfigurationConfig;
import edu.umass.cs.utils.Config;


public class GNSTestingConfig {


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


  public static final String TESTING_CONFIG_FILE_KEY = "testingConfig";

  public static final String DEFAULT_TESTING_CONFIG_FILE = "testing.properties";


  public static enum GNSTC implements Config.ConfigurableEnum {


    NUM_GUIDS_PER_ACCOUNT(100),

    ACCOUNT_GUIDS_ONLY(false),

    ACCOUNT_GUID_PREFIX("ACCOUNT_GUID"),

    EXECUTE_NOOP_ENABLED(false),;

    final Object defaultValue;

    GNSTC(Object defaultValue) {
      this.defaultValue = defaultValue;
    }


    @Override
    public Object getDefaultValue() {
      return this.defaultValue;
    }


    @Override
    public String getConfigFileKey() {
      return "testingConfig";
    }


    @Override
    public String getDefaultConfigFile() {
      return "testing.properties";
    }
  }
}
