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
package edu.umass.cs.gnsclient.client.tcp.packet;

import org.json.JSONException;
import org.json.JSONObject;

/**
 * Packet format back to the client by local name server in response to a CommandPacket.
 * Contains the original id plus the return value (as a STRING)
 * plus a possible error code (could be null)
 * plus instrumentation.
 *
 * THIS CLASS IS ALSO IN THE SERVER so they need to be kept consistent
 * at least with fields in the JSON that are needed by the client.
 */
public class CommandValueReturnPacket extends BasicPacket {

  
  private final static String CLIENTREQUESTID = "clientreqID";
  // Not used by the client
  //private final static String CCPREQUESTID = "LNSreqID";
  //private final static String SERVICENAME = "srvceName";
  private final static String RETURNVALUE = "returnValue";
  private final static String ERRORCODE = "errorCode";
  // Instrumentation
  private final static String CCPROUNDTRIPTIME = "ccpRtt";
  private final static String CCPPROCESSINGTIME = "ccpTime";
  private final static String RESPONDER = "responder";
  private final static String REQUESTCNT = "requestCnt";
  private final static String REQUESTRATE = "requestRate";
  //private final static String LOOKUPTIME = "lookuptime";

  /**
   * Identifier of the request.
   */
  private final int requestId;
  /**
   * The returned value - will most likely become a JSONObject soon
   */
  private final String returnValue;
  /**
   * Indicates if the response is an error.
   */
  private final NSResponseCode errorCode;
  /**
   * Instrumentation - The RTT as measured from the LNS out and back.
   */
  private final long LNSRoundTripTime; // how long this query took from the LNS out and back
  /**
   * Instrumentation - Total command processing time at the LNS.
   */
  private final long LNSProcessingTime; // how long this query took inside the LNS
  /**
   * Instrumentation - what nameserver responded to this query.
   */
  private final String responder;
  /**
   * Instrumentation - the request counter from the LNS.
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

  /**
   * Creates a CommandValueReturnPacket from a JSONObject.
   *
   * @param json
   * @throws JSONException
   */
  public CommandValueReturnPacket(JSONObject json) throws JSONException {
    this.type = Packet.getPacketType(json);
    this.requestId = json.getInt(CLIENTREQUESTID);
    this.returnValue = json.getString(RETURNVALUE);
    if (json.has(ERRORCODE)) {
      this.errorCode = NSResponseCode.getResponseCode(json.getInt(ERRORCODE));
    } else {
      this.errorCode = NSResponseCode.NO_ERROR;
    }
    // instrumentation
    this.requestRate = json.getInt(REQUESTRATE);
    this.requestCnt = json.getLong(REQUESTCNT);
    //
    this.LNSRoundTripTime = json.optLong(CCPROUNDTRIPTIME, -1);
    this.LNSProcessingTime = json.optLong(CCPPROCESSINGTIME, -1);
    this.responder = json.optString(RESPONDER, null);
    //this.lookupTime = json.optInt(LOOKUPTIME, -1);
  }

  /**
   * Converts the command object into a JSONObject.
   *
   * @return
   * @throws org.json.JSONException
   */
  @Override
  public JSONObject toJSONObject() throws JSONException {
    JSONObject json = new JSONObject();
    Packet.putPacketType(json, getType());
    json.put(CLIENTREQUESTID, this.requestId);
    json.put(RETURNVALUE, returnValue);
    if (errorCode != null) {
      json.put(ERRORCODE, errorCode.getCodeValue());
    }
    json.put(REQUESTRATE, requestRate); // instrumentation
    json.put(REQUESTCNT, requestCnt); // instrumentation
    // instrumentation
    if (LNSRoundTripTime != -1) {
      json.put(CCPROUNDTRIPTIME, LNSRoundTripTime);
    }
    if (LNSProcessingTime != -1) {
      json.put(CCPPROCESSINGTIME, LNSProcessingTime);
    }
    // instrumentation
    if (responder != null) {
      json.put(RESPONDER, responder);
    }
//    if (lookupTime != -1) {
//      json.put(LOOKUPTIME, lookupTime);
//    }
    return json;
  }

  public int getRequestId() {
    return requestId;
  }

  public String getReturnValue() {
    return returnValue;
  }

  public NSResponseCode getErrorCode() {
    return errorCode;
  }

  public long getLNSRoundTripTime() {
    return LNSRoundTripTime;
  }

  public long getLNSProcessingTime() {
    return LNSProcessingTime;
  }

  public String getResponder() {
    return responder;
  }

  public long getRequestCnt() {
    return requestCnt;
  }

  public int getRequestRate() {
    return requestRate;
  }

//  public int getLookupTime() {
//    return lookupTime;
//  }

}
