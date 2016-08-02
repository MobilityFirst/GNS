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
 *  Initial developer(s): Westy
 *
 */
package edu.umass.cs.gnscommon;

import edu.umass.cs.gigapaxos.interfaces.ClientRequest;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.CommandResponse;
import edu.umass.cs.gnsserver.gnsapp.packet.BasicPacketWithClientAddress;
import edu.umass.cs.gnsserver.gnsapp.packet.Packet;
import edu.umass.cs.gnsserver.gnsapp.packet.Packet.PacketType;
import edu.umass.cs.gnsserver.main.GNSConfig;
import edu.umass.cs.utils.Util;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.logging.Level;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * Packet format back to the client by local name server in response to a CommandPacket.
 * Contains the original id plus the return value (as a STRING)
 * plus a possible error code (could be null)
 * plus instrumentation.
 *
 */
public class CommandValueReturnPacket extends BasicPacketWithClientAddress implements ClientRequest {

  private final static String CLIENTREQUESTID = "clientreqID";
  private final static String LNSREQUESTID = "LNSreqID";
  private final static String SERVICENAME = "srvceName";
  private final static String RETURNVALUE = "returnValue";
  private final static String ERRORCODE = "errorCode";

  /**
   * Identifier of the request.
   */
  private long clientRequestId;
  /**
   * The service name from the request. Usually the guid or HRN.
   */
  private final String serviceName;
  /**
   * LNS identifier used by the LNS.
   */
  private long LNSRequestId;
  /**
   * The returned value.
   */
  private final String returnValue;
  /**
   * Indicates if the response is an error.
   */
  private final GNSResponseCode errorCode;

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
          CommandResponse response, long requestCnt,
          int requestRate, long cppProccessingTime) {
    this.setType(PacketType.COMMAND_RETURN_VALUE);
    this.clientRequestId = requestId;
    this.LNSRequestId = CCPRequestId;
    this.serviceName = serviceName;
    this.returnValue = response.getReturnValue();
    this.errorCode = response.getExceptionOrErrorCode();
  }

  public CommandValueReturnPacket(String serviceName, long requestId, GNSResponseCode code, String returnValue) {
    this.setType(PacketType.COMMAND_RETURN_VALUE);
    this.clientRequestId = requestId;
    this.serviceName = serviceName;
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
    //GNSConfig.getLogger().log(Level.SEVERE, "JSON = " + json.toString());
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
 * This method is used by fromBytes to recreate a CommandValueReturnPacket from a byte array.
 * @param requestId
 * @param CCPRequestId
 * @param errorNumber
 * @param serviceName
 * @param responseValue
 */
  public CommandValueReturnPacket(long requestId, long CCPRequestId, int errorNumber, String serviceName,
        String responseValue) {
  this.setType(PacketType.COMMAND_RETURN_VALUE);
  this.clientRequestId = requestId;
  this.LNSRequestId = CCPRequestId;
  this.serviceName = serviceName;
  this.returnValue = responseValue;
  this.errorCode = GNSResponseCode.getResponseCode(errorNumber);
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
    } else {
      json.put(ERRORCODE, GNSResponseCode.NO_ERROR.getCodeValue());
    }
    return json;
  }
  
  /**
   * Converts the CommandValueReturnPacket to a byte array.
   * @return The byte array
   * @throws UnsupportedEncodingException 
   */
  public byte[] toBytes() {
	  try {

	  // We need to include the following fields in our byte array:
	  
		// private long clientRequestId; - 8 bytes
	  	//private long LNSRequestId; - 8 bytes
	  	// private final GNSResponseCode errorCode; - Represented by an integer - 4 bytes
	  	//serviceName String's length - an int - 4 bytes
		// private final String serviceName; - variable length
	  	// returnValue String's length - ant int - 4 bytes
		// private final String returnValue; - variable length
	  int errorCodeInt = errorCode.getCodeValue();
	  byte[] serviceNameBytes = serviceName.getBytes("ISO-8859-1");
	  byte[] returnValueBytes = returnValue.getBytes("ISO-8859-1");


	  ByteBuffer buf = ByteBuffer.allocate(8 + 8 + 4 + 4 + serviceNameBytes.length + 4 + returnValueBytes.length);
	  buf.putLong(clientRequestId);
	  buf.putLong(LNSRequestId);
	  buf.putInt(errorCodeInt);
	  buf.putInt(serviceNameBytes.length);
	  buf.put(serviceNameBytes);
	  buf.putInt(returnValueBytes.length);
	  buf.put(returnValueBytes);
	  return buf.array();
	  } catch(UnsupportedEncodingException e) {
		  throw new RuntimeException(e);
	  }
	  
  }

    /**
   * Constructs a CommandValueReturnPacket from a byte array.
   * @param bytes The byte array created by the toBytes method of a CommandValueReturnPacket
   * @return The CommandValueReturnPacket represented by the bytes
   * @throws UnsupportedEncodingException
   */
  public static final CommandValueReturnPacket fromBytes(byte[] bytes) throws UnsupportedEncodingException{
	  ByteBuffer buf = ByteBuffer.wrap(bytes);
	  long clientReqId = buf.getLong();
	  long LNSReqId = buf.getLong();
	  int errorCodeInt = buf.getInt();
	  int serviceNameLength = buf.getInt();
	  byte[] serviceNameBytes = new byte[serviceNameLength];
	  buf.get(serviceNameBytes);
	  String serviceNameString = new String(serviceNameBytes, "ISO-8859-1");
	  int returnValueLength = buf.getInt();
	  byte[] returnValueBytes = new byte[returnValueLength];
	  buf.get(returnValueBytes);
	  String returnValueString = new String(returnValueBytes, "ISO-8859-1");
	  return new CommandValueReturnPacket(clientReqId, LNSReqId, errorCodeInt, serviceNameString, returnValueString);
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
        return getRequestType()
                + ":"
                + getServiceName()
                + ":"
                + getRequestID()
                + ":"
                + getErrorCode()
                + ":"
                + Util.truncate(getReturnValue(), 64, 64);
      }
    };
  }

  public ClientRequest setClientRequestAndLNSIds(long requestID) {
    this.clientRequestId = requestID;
    this.LNSRequestId = requestID;
    return this;
  }
}
