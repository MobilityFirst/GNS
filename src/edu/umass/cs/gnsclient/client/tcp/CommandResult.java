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
 *  Initial developer(s): Westy, Emmanuel Cecchet
 *
 */
package edu.umass.cs.gnsclient.client.tcp;

import edu.umass.cs.gnsclient.client.tcp.packet.CommandValueReturnPacket;
import edu.umass.cs.gnsclient.client.tcp.packet.NSResponseCode;
import java.io.Serializable;

/**
 * Keeps track of the returned value from a command.
 * Also has some instrumentation for round trip times and what server responded.
 *
 * @author westy
 */
public class CommandResult implements Serializable /* does it */ {

  /**
   * Set if the response is not an error.
   */
  private final String result;
  /**
   * Instrumentation - records the time the response is received back at the client
   */
  private final long receivedTime;
  /**
   * Indicates if the response is an error. Partially implemented.
   */
  private final NSResponseCode errorCode;
  /**
   * Instrumentation - The RTT as measured from the LNS out and back.
   */
  private final long CCPRoundTripTime; // how long this query took from LSN out and back (set by LNS)
  /**
   * Instrumentation - Total command processing time at the LNS.
   */
  private final long CCPProcessingTime; // how long this query took inside the LNS
  /**
   * Instrumentation - what nameserver responded to this query.
   */
  private final String responder;
  /**
   * Instrumentation - the request counter from the LNS
   */
  private final long requestCnt;
  /**
   * Instrumentation - the current requests per second from the LNS (can be used to tell how busy LNS is)
   */
  private final int requestRate;
//  /**
//   * Database lookup time instrumentation
//   */
//  private final int lookupTime;

//  public CommandResult(String result, long receivedTime) {
//    this.result = result;
//    this.receivedTime = receivedTime;
//  }

  public CommandResult(CommandValueReturnPacket packet, long receivedTime) {
    this.result = packet.getReturnValue();
    this.receivedTime = receivedTime;
    this.errorCode = packet.getErrorCode();
    this.CCPRoundTripTime = packet.getLNSRoundTripTime();
    this.CCPProcessingTime = packet.getLNSProcessingTime();
    this.responder = packet.getResponder();
    this.requestCnt = packet.getRequestCnt();
    this.requestRate = packet.getRequestRate();
    //this.lookupTime = packet.getLookupTime();
  }

  /**
   * Returns the result of the command as a string.
   * 
   * @return 
   */
  public String getResult() {
    return result;
  }

  /**
   * Instrumentation - holds the time when the return message is received.
   * 
   * @return the time in milliseconds
   */
  public long getReceivedTime() {
    return receivedTime;
  }

  /**
   * Returns the error code if any returned by the execution of the command (could be null).'
   * 
   * @return the code
   */
  public NSResponseCode getErrorCode() {
    return errorCode;
  }

  /**
   * Instrumentation - The RTT as measured from the CCP out and back.
   * 
   * @return the time in milliseconds
   */
  public long getCCPRoundTripTime() {
    return CCPRoundTripTime;
  }

  /**
   * Instrumentation - Returns the total command processing time at the LNS.
   * @return 
   */
  public long getCCPProcessingTime() {
    return CCPProcessingTime;
  }

  /**
   * Instrumentation - what nameserver responded to this query (could be null for some non-query command)
   * 
   * @return the id
   */
  public String getResponder() {
    return responder;
  }

  /**
   * Instrumentation - the request counter from the LNS (can be used to tell how busy LNS is)
   * 
   * @return the counter
   */
  public long getRequestCnt() {
    return requestCnt;
  }

  public int getRequestRate() {
    return requestRate;
  }
  
//  /**
//   * Instrumentation - returns the database component of the latency.
//   * 
//   * @return 
//   */
//  
//  public int getLookupTime() {
//    return lookupTime;
//  }
 

}
