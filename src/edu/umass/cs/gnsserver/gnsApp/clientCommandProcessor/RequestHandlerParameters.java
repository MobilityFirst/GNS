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
package edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor;

//import edu.umass.cs.gnsserver.nsdesign.replicationframework.ReplicationFrameworkType;

import edu.umass.cs.gnsserver.main.GNS;


/**
 * Encapsulate all these "global" parameters that are needed by the request handler.
 */
public class RequestHandlerParameters {
  /**
   * Determines if we execute logging statements.
   */
  private boolean debugMode = false;
  /**
   * If this is true, we emulate wide-area latencies for packets sent between nodes. The latencies values from this
   * node to a different node come from ping latencies that are given in the config file.
   */
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
   * Creates an instance of the RequestHandlerParameters with default values.
   */
  public RequestHandlerParameters() {
  }
 
  /**
   * Creates an instance of the RequestHandlerParameters.
   * 
   * @param debugMode
   * @param queryTimeout
   * @param maxQueryWaitTime
   * @param cacheSize
   */
  public RequestHandlerParameters(boolean debugMode, int queryTimeout, int maxQueryWaitTime, int cacheSize
  ) {
    this.debugMode = debugMode;
    this.queryTimeout = queryTimeout;
    this.maxQueryWaitTime = maxQueryWaitTime;
    this.cacheSize = cacheSize;
  }

  /**
   * Returns true if we're in debug mode.
   * 
   * @return true or false
   */
  public boolean isDebugMode() {
    return debugMode;
  }

  /**
   * Returns the query timeout.
   * Fixed timeout after which a query retransmitted.
   * 
   * @return the query timeout
   */
  public int getQueryTimeout() {
    return queryTimeout;
  }

  /**
   * Returns the max query wait time.
   * This is the time a name server waits for a response. 
   * A query is logged as failed after this.
   * 
   * @return the max query wait time
   */
  public int getMaxQueryWaitTime() {
    return maxQueryWaitTime;
  }

  /**
   * Returns the cache size.
   * 
   * @return the cache size
   */
  public int getCacheSize() {
    return cacheSize;
  }

  /**
   * Set debug mode.
   * 
   * @param debugMode
   */
  
  public void setDebugMode(boolean debugMode) {
    this.debugMode = debugMode;
  }

  @Override
  public String toString() {
    return "RequestHandlerParameters{" + "debugMode=" + debugMode 
            + ", queryTimeout=" + queryTimeout + ", maxQueryWaitTime=" 
            + maxQueryWaitTime + ", cacheSize=" + cacheSize 
           
            + '}';
  }

}
