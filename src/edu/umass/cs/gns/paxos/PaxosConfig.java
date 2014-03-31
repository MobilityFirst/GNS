package edu.umass.cs.gns.paxos;

import edu.umass.cs.gns.main.GNS;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Configuration parameters for paxos package.
 *
 * Created by abhigyan on 3/7/14.
 */
public class PaxosConfig {


  // configuration parameters
  private boolean debugMode = true;


  /** Paxos logs are stored in this folder */
  private String paxosLogFolder = DEFAULT_PAXOS_LOG_FOLDER;

  /** Interval (in milliseconds) at which failure detector sends ping messages to all nodes */
  private int failureDetectionPingMillis = DEFAULT_FD_PING_MILLIS;

  /** Interval (in milliseconds) after which a node is declared as failed is no response is received */
  private int failureDetectionTimeoutMillis = DEFAULT_FD_TIMEOUT_MILLIS;

  // in config file, use these parameter names

  private static final String PARAM_PAXOS_LOG_FOLDER = "paxosLogFolder";

  private static final String PARAM_FD_PING_MILLIS = "failureDetectionPingMillis";

  private static final String PARAM_FD_TIMEOUT_MILLIS = "failureDetectionTimeoutMillis";


  // default values of parameters

  private static final String DEFAULT_PAXOS_LOG_FOLDER = "paxosLog";

  private static final int DEFAULT_FD_PING_MILLIS = 10000;

  private static final int DEFAULT_FD_TIMEOUT_MILLIS = 30000;


  /**
   * This constructor will initialize all parameters to default values.
   */
  public PaxosConfig() {

  }

  /**
   * This constructor will read parameter values given in a config file. If a parameter is not specified in the
   * config file, it will be initialized to its default value. Config file should be java Properties format.
   */
  public PaxosConfig(String configFile) {
    File f = new File(configFile);
    if (!f.exists()) {
      GNS.getLogger().severe(" Config file does not exist. Quit. Filename =  " + configFile);
      System.exit(2);
    }

    try {

      Properties prop = new Properties();

      InputStream input = new FileInputStream(configFile);
      // load a properties file
      prop.load(input);

      if (prop.containsKey(PARAM_PAXOS_LOG_FOLDER))  paxosLogFolder = prop.getProperty(PARAM_PAXOS_LOG_FOLDER);

      if (prop.containsKey(PARAM_FD_PING_MILLIS))  failureDetectionPingMillis = Integer.parseInt(prop.getProperty(PARAM_FD_PING_MILLIS));

      if (prop.containsKey(DEFAULT_FD_PING_MILLIS)) failureDetectionTimeoutMillis = Integer.parseInt(prop.getProperty(PARAM_FD_TIMEOUT_MILLIS));

      // todo create parsing option for debugMode
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public String getPaxosLogFolder() {
    return paxosLogFolder;
  }

  public void setPaxosLogFolder(String paxosLogFolder) {
    this.paxosLogFolder = paxosLogFolder;
  }

  public int getFailureDetectionPingMillis() {
    return failureDetectionPingMillis;
  }

  public void setFailureDetectionPingMillis(int failureDetectionPingMillis) {
    this.failureDetectionPingMillis = failureDetectionPingMillis;
  }

  public int getFailureDetectionTimeoutMillis() {
    return failureDetectionTimeoutMillis;
  }

  public void setFailureDetectionTimeoutMillis(int failureDetectionTimeoutMillis) {
    this.failureDetectionTimeoutMillis = failureDetectionTimeoutMillis;
  }

  public boolean isDebugMode() {
    return debugMode;
  }

  public void setDebugMode(boolean debugMode) {
    this.debugMode = debugMode;
  }
}
