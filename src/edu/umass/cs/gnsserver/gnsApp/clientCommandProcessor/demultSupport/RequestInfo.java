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
package edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.demultSupport;

import edu.umass.cs.gnsserver.gnsApp.packet.Packet;
import org.json.JSONObject;

/**
 * Class represents the abstract class in which CCP stores info for each ongoing request,
 * from the time it is received by a CCP until a success/failure response is returned.
 * Only a single instance of this class is defined during the lifetime of a request.
 *
 * Some fields in this class are necessary to implement functionality of a local name server, while
 * some are there only to collect and log statistics for a request.
 *
 * Created by abhigyan on 5/29/14.
 */
public abstract class RequestInfo {

  /** The name. */
  protected String name;

  /** Unique request ID assigned to this request by the local name server. */
  protected int ccpReqID;

  /** Time that CCP started processing this request. */
  protected long startTime;
  
  /** Time that CCP completed processing this request. */
  protected long finishTime =  -1;

  /** True if CCP is requesting current set of active replicas for this request. False, otherwise */
  private boolean lookupActives = false;

  /** Number of times this request has initiated lookup actives operation.
   * This could be always zero for some types of requests (namely ADD and REMOVE) that are
   * sent only to replica controllers (and never to active replicas) */
  protected int numLookupActives = 0;

  /***********
   * Fields only for collecting statistics for a request and write log entries
   ***********/

  /** The request type */
  protected Packet.PacketType requestType;

  /** Whether requests is finally successful or not. */
  protected boolean success = false;

  // Abstract methods

  /** 
   * Returns the error message to be sent to client if name server returns no response to a request.
   * @return  */
  public abstract JSONObject getErrorMessage();


  // methods that are implemented

  /**
   * Returns the name. 
   * 
   * @return the name
   */
  
  public synchronized String getName() {
    return name;
  }

  /**
   * Returns the CCP request id.
   * 
   * @return the CCP request id
   */
  public synchronized int getCCPReqID() {
    return ccpReqID;
  }

  /**
   * Returns the start time.
   * 
   * @return returns the start time (ms)
   */
  public synchronized long getStartTime() {
    return startTime;
  }

  /** 
   * Time duration for which the request was under processing at the client command processor.
   * @return 
   */
  public synchronized long getResponseLatency() {
    return finishTime - startTime;
  }

  /**
   * Returns the finish time.
   * 
   * @return returns the finish time (ms)
   */
  public synchronized long getFinishTime() {
    return finishTime;
  }

  /**
   * Sets the finish time (in milliseconds).
   */
  public synchronized void setFinishTime() {
    this.finishTime = System.currentTimeMillis();
  }

  /**
   * Returns the success of the request.
   * 
   * @return true or false
   */
  public synchronized boolean isSuccess() {
    return success;
  }

  /**
   * Sets the success of the request.
   * 
   * @param success
   */
  public synchronized void setSuccess(boolean success) {
    this.success = success;
  }

  /**
   * Returns true if actives are currently being requested. Otherwise false.
   * @return true or false
   */
  public synchronized boolean isLookupActives() {
    return lookupActives;
  }

  /** 
   * Set if actives are currently being requested.
   * Returns false if they are already being requested
   * true if not.
   * 
   * @return true if it is newly set or false if they are already
   */
  public synchronized boolean setLookupActives() {
    if (lookupActives) {
      return false;
    } else {
      lookupActives = true;
      numLookupActives += 1;
      return true;
    }
  }

  /** 
   * After active replicas are received for this request, reset the lookup actives variables.
   */
  public synchronized void unsetLookupActives() {
    assert lookupActives;
    lookupActives = false;
  }

  /**
   * Returns the cumulative number of actives lookups.
   * 
   * @return the number of actives lookups
   */
  public synchronized int getNumLookupActives() {
    return numLookupActives;
  }

}
