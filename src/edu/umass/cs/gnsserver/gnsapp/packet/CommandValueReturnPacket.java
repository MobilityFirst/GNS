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
package edu.umass.cs.gnsserver.gnsapp.packet;

import edu.umass.cs.gigapaxos.interfaces.ClientRequest;
import edu.umass.cs.gnscommon.GNSResponseCode;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.CommandResponse;
import edu.umass.cs.gnsserver.gnsapp.packet.Packet.PacketType;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * Packet format back to the client by local name server in response to a CommandPacket.
 * Contains the original id plus the return value (as a STRING)
 * plus a possible error code (could be null)
 * plus instrumentation.
 *
 * THIS EXACT CLASS IS ALSO IN THE CLIENT so they need to be kept consistent
 * insofar as the fields in the JSON Object is concerned.
 *
 */
public class CommandValueReturnPacket extends BasicPacketWithClientAddress implements ClientRequest {

  private final static String CLIENTREQUESTID = "clientreqID";
  private final static String LNSREQUESTID = "LNSreqID";
  private final static String SERVICENAME = "srvceName";
  private final static String RETURNVALUE = "returnValue";
  private final static String ERRORCODE = "errorCode";
  // Instrumentation
  private final static String CPPROUNDTRIPTIME = "ccpRtt";
  private final static String CPPPROCESSINGTIME = "ccpTime";
  private final static String RESPONDER = "responder";
  private final static String REQUESTCNT = "requestCnt";
  private final static String REQUESTRATE = "requestRate";

  /**
   * Identifier of the request.
   */
  private /*final*/ long clientRequestId;
  /**
   * The service name from the request. Usually the guid or HRN.
   */
  private final String serviceName;
  /**
   * LNS identifier used by the LNS.
   */
  private /*final*/ long LNSRequestId;
  /**
   * The returned value.
   */
  private final String returnValue;
  /**
   * Indicates if the response is an error.
   */
  private final GNSResponseCode errorCode;

  /**
   * Instrumentation - The RTT as measured from the LNS out and back.
   */
//  private final long CPPRoundTripTime; // how long this query took from the CCP out and back
  /**
   * Instrumentation - Total command processing time at the LNS.
   */
//  private final long CPPProcessingTime; // how long this query took inside the CCP
  /**
   * Instrumentation - what nameserver responded to this query.
   */
//  private final String responder;
  /**
   * Instrumentation - the request counter from the LNS.
   */
//  private final long requestCnt;
  /**
   * Instrumentation - the current requests per second from the LNS (can be used to tell how busy LNS is).
   */
//  private final int requestRate;

  /**
   * Creates a CommandValueReturnPacket from a CommandResponse.
   *
   * @param requestId
   * @param CCPRequestId
   * @param serviceName
   * @param response
   * @param requestCnt - current number of requests handled by the CCP (can be used to tell how busy CCP is)
   * @param requestRate
   * @param cppProccessingTime
   */
  public CommandValueReturnPacket(long requestId, long CCPRequestId, String serviceName,
          CommandResponse<String> response, long requestCnt,
          int requestRate, long cppProccessingTime) {
    this.setType(PacketType.COMMAND_RETURN_VALUE);
    this.clientRequestId = requestId;
    this.LNSRequestId = CCPRequestId;
    this.serviceName = serviceName;
    this.returnValue = response.getReturnValue();
    this.errorCode = response.getErrorCode();
//    this.CPPRoundTripTime = response.getCCPRoundTripTime();
//    this.CPPProcessingTime = cppProccessingTime;
//    this.responder = response.getResponder();
//    this.requestCnt = requestCnt;
//    this.requestRate = requestRate;
  }

  public CommandValueReturnPacket(long requestId, GNSResponseCode code, String returnValue) {
    this.setType(PacketType.COMMAND_RETURN_VALUE);
    this.clientRequestId = requestId;
    this.serviceName = null;
    this.returnValue = returnValue;
    this.errorCode = code;
  }

  /**
   * Creates a CommandValueReturnPacket from a JSONObject.
   *
   * @param json
   * @throws JSONException
   */
  public CommandValueReturnPacket(JSONObject json) throws JSONException {
    this.type = Packet.getPacketType(json);
    this.clientRequestId = json.getLong(CLIENTREQUESTID);
    if (json.has(LNSREQUESTID)) {
      this.LNSRequestId = json.getLong(LNSREQUESTID);
    } else {
      this.LNSRequestId = -1;
    }
    this.serviceName = json.getString(SERVICENAME);
    this.returnValue = json.getString(RETURNVALUE);
    if (json.has(ERRORCODE)) {
      this.errorCode = GNSResponseCode.getResponseCode(json.getInt(ERRORCODE));
    } else {
      this.errorCode = GNSResponseCode.NO_ERROR;
    }
  }

  /**
   * Converts the command object into a JSONObject.
   *
   * @return a JSONObject
   * @throws org.json.JSONException
   */
  @Override
  public JSONObject toJSONObject() throws JSONException {
    JSONObject json = new JSONObject();
    Packet.putPacketType(json, getType());
    json.put(CLIENTREQUESTID, this.clientRequestId);
    if (this.LNSRequestId != -1) {
      json.put(LNSREQUESTID, this.LNSRequestId);
    }
    json.put(SERVICENAME, this.serviceName);
    json.put(RETURNVALUE, returnValue);
    if (errorCode != null) {
      json.put(ERRORCODE, errorCode.getCodeValue());
    }
//    json.put(REQUESTRATE, requestRate); // instrumentation
//    json.put(REQUESTCNT, requestCnt); // instrumentation
    // instrumentation
//    if (CPPRoundTripTime != -1) {
//      json.put(CPPROUNDTRIPTIME, CPPRoundTripTime);
//    }
//    if (CPPProcessingTime != -1) {
//      json.put(CPPPROCESSINGTIME, CPPProcessingTime);
//    }
    // instrumentation
//    if (responder != null) {
//      json.put(RESPONDER, responder);
//    }
    return json;
  }

  /**
   * Get the client request id.
   *
   * @return the client request id
   */
  public long getClientRequestId() {
    return clientRequestId;
  }

  @Override
  public String getServiceName() {
    return serviceName;
  }

  /**
   * Get the LNS request id.
   *
   * @return the LNS request id
   */
  public long getLNSRequestId() {
    return LNSRequestId;
  }

  /**
   * Get the return value.
   *
   * @return the return value
   */
  public String getReturnValue() {
    return returnValue;
  }

  /**
   * Get the error code.
   *
   * @return the error code
   */
  public GNSResponseCode getErrorCode() {
    return errorCode;
  }

  /**
   * Get the LNS round trip time (instrumentation).
   *
   * @return the LNS round trip time
   */
//  public long getCPPRoundTripTime() {
//    return CPPRoundTripTime;
//  }
  /**
   * Get the LNS processing time (instrumentation).
   *
   * @return the LNS processing time
   */
//  public long getCPPProcessingTime() {
//    return CPPProcessingTime;
//  }
  /**
   * Get the responder host id (instrumentation).
   *
   * @return the responder
   */
//  public String getResponder() {
//    return responder;
//  }
  /**
   * Get the request count (instrumentation).'
   *
   * @return the request count
   */
//  public long getRequestCnt() {
//    return requestCnt;
//  }
  /**
   * Get the request rate (instrumentation).
   *
   * @return the request rate
   */
//  public int getRequestRate() {
//    return requestRate;
//  }
  @Override
  public ClientRequest getResponse() {
    return this.response;
  }

  @Override
  public long getRequestID() {
    return clientRequestId;
  }

  @Override
  public Object getSummary() {
    return new Object() {
      @Override
      public String toString() {
        return CommandValueReturnPacket.this.getRequestType()
                + ":"
                + CommandValueReturnPacket.this.getServiceName()
                + ":"
                + CommandValueReturnPacket.this.getRequestID()
                + ":"
                + CommandValueReturnPacket.this.getReturnValue();
      }
    };
  }

  public ClientRequest setClientRequestAndLNSIds(long requestID) {
    this.clientRequestId = requestID;
    this.LNSRequestId = requestID;
    return this;
  }
}
