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
package edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commandSupport;

import edu.umass.cs.gnsserver.gnsApp.NSResponseCode;

/**
 * Encapsulates the response values and instrumentation that we pass back to the client.
 * @param <NodeIDType>
 */
public class CommandResponse<NodeIDType> {

  /**
   * Value returned.
   */
  private String returnValue;
  /**
   * Indicates if the response is an error. Can be null.
   */
  private NSResponseCode errorCode;
  
  // instrumentation
  /**
   * The RTT as measured from the CCP out and back.
   */
  private long CCPRoundTripTime; // how long this query took
  /**
   * Instrumentation - what nameserver responded to this query
   */
  private NodeIDType responder;
//  /**
//   * Database lookup time instrumentation
//   */
//  private final int lookupTime;

  /**
   * Create a command response object from a return value with an error code.
   * 
   * @param returnValue
   * @param errorCode
   * @param CCPRoundTripTime
   * @param responder
   */
  public CommandResponse(String returnValue, NSResponseCode errorCode, 
          long CCPRoundTripTime, NodeIDType responder) {
    this.returnValue = returnValue;
    this.errorCode = errorCode;
    this.CCPRoundTripTime = CCPRoundTripTime;
    this.responder = responder;
    //this.lookupTime = lookupTime;
  }

  /**
   * Create a command response object from a return value with no error.
   * 
   * @param returnValue
   */
  public CommandResponse(String returnValue) {
    this(returnValue, NSResponseCode.NO_ERROR, -1, null);
  }

  /**
   * Gets the return value.
   * 
   * @return a string
   */
  public String getReturnValue() {
    return returnValue;
  }

  /**
   * Gets the error code.
   * 
   * @return a {@link NSResponseCode}
   */
  public NSResponseCode getErrorCode() {
    return errorCode;
  }
  
  /**
   * Does this Command contain an error result.
   * 
   * @return 
   */
  public boolean isError() {
    return this.errorCode != null;
  }

  /**
   * Gets the LNS round trip time (computed from LNS to NS and back).
   * 
   * @return a long
   */
  public long getCCPRoundTripTime() {
    return CCPRoundTripTime;
  }

  /**
   * Gets the id of the name server that responded (could be -1 if not known).
   * 
   * @return an id
   */
  public NodeIDType getResponder() {
    return responder;
  }

//  /**
//   * Retrieves the database lookup time instrumentation from the command response.
//   * 
//   * @return 
//   */
//  public int getLookupTime() {
//    return lookupTime;
//  }

}
