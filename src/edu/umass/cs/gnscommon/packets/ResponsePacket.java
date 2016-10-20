/* Copyright (c) 2015 University of Massachusetts
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 * 
 * Initial developer(s): Westy, arun */
package edu.umass.cs.gnscommon.packets;

import edu.umass.cs.gigapaxos.interfaces.ClientRequest;
import edu.umass.cs.gnscommon.GNSProtocol;
import edu.umass.cs.gnscommon.ResponseCode;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.CommandResponse;
import edu.umass.cs.gnsserver.gnsapp.packet.BasicPacketWithClientAddress;
import edu.umass.cs.gnsserver.gnsapp.packet.Packet;
import edu.umass.cs.gnsserver.gnsapp.packet.Packet.PacketType;
import edu.umass.cs.nio.MessageNIOTransport;
import edu.umass.cs.utils.Util;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

import org.json.JSONException;
import org.json.JSONObject;

/**
 * Packet format back to the client by local name server in response to a
 * CommandPacket. Contains the original id plus the return value (as a STRING)
 * plus a possible error code (could be null) plus instrumentation.
 *
 */
public class ResponsePacket extends BasicPacketWithClientAddress
        implements ClientRequest {

  private final static String QID = GNSProtocol.REQUEST_ID.toString();
  private final static String NAME = GNSProtocol.SERVICE_NAME.toString();
  private final static String RETVAL = GNSProtocol.RETURN_VALUE.toString();
  private final static String ERRCODE = GNSProtocol.ERROR_CODE.toString();

  private final static boolean SUPPORT_OLD_PROTOCOL = true;
  private final static String OLD_COMMAND_RETURN_PACKET_REQUESTID = "clientreqID";
  private final static String OLD_COMMAND_RETURN_PACKET_RETURNVALUE = "returnValue";
  private final static String OLD_COMMAND_RETURN_PACKET_ERRORCODE = "errorCode";

  /**
   * Identifier of the request.
   */
  private long clientRequestId;
  /**
   * The service name from the request. Usually the guid or HRN.
   */
  private final String serviceName;
  /**
   * The returned value.
   */
  private final String returnValue;
  /**
   * Indicates if the response is an error.
   */
  private final ResponseCode errorCode;

  /**
   * Creates a CommandValueReturnPacket from a CommandResponse.
   *
   * @param requestId
   * @param serviceName
   * @param response
   * @param requestCnt
   * - current number of requests handled by the CCP (can be used
   * to tell how busy CCP is)
   * @param requestRate
   * @param cppProccessingTime
   */
  public ResponsePacket(long requestId, String serviceName,
          CommandResponse response, long requestCnt, int requestRate,
          long cppProccessingTime) {
    this.setType(PacketType.COMMAND_RETURN_VALUE);
    this.clientRequestId = requestId;
    this.serviceName = serviceName;
    this.returnValue = response.getReturnValue();
    this.errorCode = response.getExceptionOrErrorCode();
  }

  /**
   * @param serviceName
   * @param requestId
   * @param code
   * @param returnValue
   */
  public ResponsePacket(String serviceName, long requestId,
          ResponseCode code, String returnValue) {
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
  public ResponsePacket(JSONObject json) throws JSONException {
    this.type = Packet.getPacketType(json);

    if (!SUPPORT_OLD_PROTOCOL) {
      this.clientRequestId = json.getLong(QID);
      this.returnValue = json.getString(RETVAL);
      this.serviceName = json.getString(NAME);
      if (json.has(ERRCODE)) {
        this.errorCode = ResponseCode.getResponseCode(json.getInt(ERRCODE));
      } else {
        this.errorCode = ResponseCode.NO_ERROR;
      }
      // probably not necessary as the old ios client never actually sends these 
      // but just to be thorough
    } else {
      if (json.has(QID)) {
        this.clientRequestId = json.getLong(QID);
      } else if (json.has(OLD_COMMAND_RETURN_PACKET_REQUESTID)) {
        this.clientRequestId = json.getLong(OLD_COMMAND_RETURN_PACKET_REQUESTID);
      } else {
        throw new JSONException("Packet missing field " + QID);
      }
      if (json.has(RETVAL)) {
        this.returnValue = json.getString(RETVAL);
      } else if (json.has(OLD_COMMAND_RETURN_PACKET_RETURNVALUE)) {
        this.returnValue = json.getString(OLD_COMMAND_RETURN_PACKET_RETURNVALUE);
      } else {
        throw new JSONException("Packet missing field " + RETVAL);
      }
      if (json.has(ERRCODE)) {
        this.errorCode = ResponseCode.getResponseCode(json.getInt(ERRCODE));
      } else if (json.has(OLD_COMMAND_RETURN_PACKET_ERRORCODE)) {
        this.errorCode = ResponseCode.getResponseCode(json.getInt(OLD_COMMAND_RETURN_PACKET_ERRORCODE));
      } else {
        this.errorCode = ResponseCode.NO_ERROR;
      }
      // not sure what to do here; this is nothing in the old protocol for this from the ios client
      this.serviceName = json.optString(NAME, "unknown");
    }
  }

  /**
   * This method is used by fromBytes to recreate a CommandValueReturnPacket
   * from a byte array.
   *
   * @param requestId
   * @param errorNumber
   * @param serviceName
   * @param responseValue
   */
  public ResponsePacket(long requestId, int errorNumber,
          String serviceName, String responseValue) {
    this.setType(PacketType.COMMAND_RETURN_VALUE);
    this.clientRequestId = requestId;
    this.serviceName = serviceName;
    this.returnValue = responseValue;
    this.errorCode = ResponseCode.getResponseCode(errorNumber);
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
    json.put(QID, this.clientRequestId);
    json.put(NAME, this.serviceName);
    json.put(RETVAL, returnValue);
    if (errorCode != null) {
      json.put(ERRCODE, errorCode.getCodeValue());
    } else {
      json.put(ERRCODE, ResponseCode.NO_ERROR.getCodeValue());
    }
    return json;
  }

  /**
   * Converts the CommandValueReturnPacket to a byte array.
   *
   * @return The byte array
   */
  public byte[] toBytes() {
    try {

      // We need to include the following fields in our byte array:
      // private long clientRequestId; - 8 bytes
      // private long LNSRequestId; - 8 bytes
      // private final ResponseCode errorCode; - Represented by an
      // integer - 4 bytes
      // serviceName String's length - an int - 4 bytes
      // private final String serviceName; - variable length
      // returnValue String's length - ant int - 4 bytes
      // private final String returnValue; - variable length
      int errorCodeInt = errorCode.getCodeValue();
      byte[] serviceNameBytes = serviceName
              .getBytes(MessageNIOTransport.NIO_CHARSET_ENCODING);
      byte[] returnValueBytes = returnValue
              .getBytes(MessageNIOTransport.NIO_CHARSET_ENCODING);

      ByteBuffer buf = ByteBuffer.allocate(
              // requestID
              Long.BYTES
              // error code
              + Integer.BYTES
              // name length
              + Integer.BYTES
              // name bytes
              + serviceNameBytes.length
              // returnValue length
              + Integer.BYTES
              // returnValue bytes
              + returnValueBytes.length);

      // requestID
      buf.putLong(clientRequestId)
              // error code
              .putInt(errorCodeInt)
              // name length

              // name bytes
              .putInt(serviceNameBytes.length)
              .put(serviceNameBytes)
              // returnValue length
              .putInt(returnValueBytes.length)
              // returnValue bytes
              .put(returnValueBytes);

      return buf.array();
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }

  }

  /**
   * Constructs a CommandValueReturnPacket from a byte array.
   *
   * @param bytes
   * The byte array created by the toBytes method of a
   * CommandValueReturnPacket
   * @return The CommandValueReturnPacket represented by the bytes
   * @throws UnsupportedEncodingException
   */
  public static final ResponsePacket fromBytes(byte[] bytes)
          throws UnsupportedEncodingException {
    ByteBuffer buf = ByteBuffer.wrap(bytes);
    long clientReqId = buf.getLong();
    int errorCodeInt = buf.getInt();
    int serviceNameLength = buf.getInt();
    byte[] serviceNameBytes = new byte[serviceNameLength];
    buf.get(serviceNameBytes);
    String serviceNameString = new String(serviceNameBytes,
            MessageNIOTransport.NIO_CHARSET_ENCODING);
    int returnValueLength = buf.getInt();
    byte[] returnValueBytes = new byte[returnValueLength];
    buf.get(returnValueBytes);
    String returnValueString = new String(returnValueBytes,
            MessageNIOTransport.NIO_CHARSET_ENCODING);
    return new ResponsePacket(clientReqId, errorCodeInt,
            serviceNameString, returnValueString);

  }

  /**
   * Get the client request id.
   *
   * @return the client request id
   */
  public long getClientRequestId() {
    return clientRequestId;
  }

  /**
   *
   * @return the service name
   */
  @Override
  public String getServiceName() {
    return serviceName;
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
  public ResponseCode getErrorCode() {
    return errorCode;
  }

  /**
   *
   * @return the client request
   */
  @Override
  public ClientRequest getResponse() {
    return this.response;
  }

  /**
   *
   * @return the request id
   */
  @Override
  public long getRequestID() {
    return clientRequestId;
  }

  /**
   *
   * @return the summary
   */
  @Override
  public Object getSummary() {
    return new Object() {
      @Override
      public String toString() {
        return getRequestType() + ":" + getServiceName() + ":"
                + getRequestID() + ":" + getErrorCode() + ":"
                + Util.truncate(getReturnValue(), 64, 64);
      }
    };
  }

  /**
   * Only for instrumentation.
   *
   * @param requestID
   * @return {@code this}.
   */
  @Deprecated
  public ClientRequest setClientRequestAndLNSIds(long requestID) {
    this.clientRequestId = requestID;
    return this;
  }
}
