package edu.umass.cs.gns.newApp.clientCommandProcessor;

//import edu.umass.cs.gns.nsdesign.replicationframework.ReplicationFrameworkType;

import edu.umass.cs.gns.main.GNS;


/**
 * Encapsulate all these "global" parameters that are needed by the request handler.
 */
public class RequestHandlerParameters {
  /**
   * Determines if we execute logging statements.
   */
  private boolean debugMode = false;
  /**
   * Used for running experiments for Auspice paper.
   */
  private boolean experimentMode = false;
  /**
   * If this is true, we emulate wide-area latencies for packets sent between nodes. The latencies values from this
   * node to a different node come from ping latencies that are given in the config file.
   */
  private boolean emulatePingLatencies = false;
  /**
   * The variation in emulated ping latencies.
   */
  private double variation = 0.1;
  /**
   * Whether use a fixed timeout or an adaptive timeout. By default, fixed timeout is used.
   */
  private boolean adaptiveTimeout = false;
  /**
   * Determines how often we sample and output to the StatLogger.
   */
  private double outputSampleRate = 1.0;
  /**
   * Fixed timeout after which a query retransmitted.
   */
  private int queryTimeout = GNS.DEFAULT_QUERY_TIMEOUT;
  /**
   * Maximum time a name server waits for a response from name server query is logged as failed after this.
   */
  private int maxQueryWaitTime = GNS.DEFAULT_MAX_QUERY_WAIT_TIME;
  /**
   * The size of the cache used to store values and active nameservers.
   */
  private int cacheSize = 1000;
  /**
   * Should we use do load dependent redirection.
   */
  private boolean loadDependentRedirection = false;
  /**
   * The replicationFrameworkType we are using.
   */
  //private ReplicationFrameworkType replicationFramework = ReplicationFrameworkType.LOCATION;

  public RequestHandlerParameters() {
  }
 
  public RequestHandlerParameters(boolean debugMode, boolean experimentMode, boolean emulatePingLatencies, double variation, 
          boolean adaptiveTimeout, double outputSampleRate, int queryTimeout, int maxQueryWaitTime, int cacheSize, 
          boolean loadDependentRedirection
          //, ReplicationFrameworkType replicationFramework
  ) {
    this.debugMode = debugMode;
    this.experimentMode = experimentMode;
    this.emulatePingLatencies = emulatePingLatencies;
    this.variation = variation;
    this.adaptiveTimeout = adaptiveTimeout;
    this.outputSampleRate = outputSampleRate;
    this.queryTimeout = queryTimeout;
    this.maxQueryWaitTime = maxQueryWaitTime;
    this.cacheSize = cacheSize;
    this.loadDependentRedirection = loadDependentRedirection;
    //this.replicationFramework = replicationFramework;
  }

  public boolean isDebugMode() {
    return debugMode;
  }

  public boolean isExperimentMode() {
    return experimentMode;
  }

  public boolean isEmulatePingLatencies() {
    return emulatePingLatencies;
  }

  public double getVariation() {
    return variation;
  }

  public boolean isAdaptiveTimeout() {
    return adaptiveTimeout;
  }

  public double getOutputSampleRate() {
    return outputSampleRate;
  }

  public int getQueryTimeout() {
    return queryTimeout;
  }

  public int getMaxQueryWaitTime() {
    return maxQueryWaitTime;
  }

  public int getCacheSize() {
    return cacheSize;
  }

  public boolean isLoadDependentRedirection() {
    return loadDependentRedirection;
  }

//  public ReplicationFrameworkType getReplicationFramework() {
//    return replicationFramework;
//  }

  public void setDebugMode(boolean debugMode) {
    this.debugMode = debugMode;
  }

  @Override
  public String toString() {
    return "RequestHandlerParameters{" + "debugMode=" + debugMode 
            + ", experimentMode=" + experimentMode + ", emulatePingLatencies=" 
            + emulatePingLatencies + ", variation=" + variation + ", adaptiveTimeout=" 
            + adaptiveTimeout + ", outputSampleRate=" + outputSampleRate 
            + ", queryTimeout=" + queryTimeout + ", maxQueryWaitTime=" 
            + maxQueryWaitTime + ", cacheSize=" + cacheSize 
            + ", loadDependentRedirection=" + loadDependentRedirection 
            //+ ", replicationFramework=" + replicationFramework 
            + '}';
  }

}
