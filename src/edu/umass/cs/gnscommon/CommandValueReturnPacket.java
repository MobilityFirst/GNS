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
package edu.umass.cs.gnscommon;

import edu.umass.cs.gigapaxos.interfaces.ClientRequest;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.CommandResponse;
import edu.umass.cs.gnsserver.gnsapp.packet.BasicPacketWithClientAddress;
import edu.umass.cs.gnsserver.gnsapp.packet.Packet;
import edu.umass.cs.gnsserver.gnsapp.packet.Packet.PacketType;
import edu.umass.cs.gnsserver.main.GNSConfig;
import edu.umass.cs.nio.MessageNIOTransport;
import edu.umass.cs.utils.Util;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.logging.Level;

import org.json.JSONException;
import org.json.JSONObject;

/**
 * Packet format back to the client by local name server in response to a
 * CommandPacket. Contains the original id plus the return value (as a STRING)
 * plus a possible error code (could be null) plus instrumentation.
 *
 */
public class CommandValueReturnPacket extends BasicPacketWithClientAddress
		implements ClientRequest {

	private final static String CLIENTREQUESTID = GNSProtocol.REQUEST_ID.toString();
	private final static String SERVICENAME = GNSProtocol.SERVICE_NAME.toString();
	private final static String RETURNVALUE = GNSProtocol.RETURN_VALUE.toString();
	private final static String ERRORCODE = GNSProtocol.ERROR_CODE.toString();

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
	private final GNSResponseCode errorCode;

	/**
	 * Creates a CommandValueReturnPacket from a CommandResponse.
	 *
	 * @param requestId
	 * @param serviceName
	 * @param response
	 * @param requestCnt
	 *            - current number of requests handled by the CCP (can be used
	 *            to tell how busy CCP is)
	 * @param requestRate
	 * @param cppProccessingTime
	 */
	public CommandValueReturnPacket(long requestId, String serviceName,
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
	public CommandValueReturnPacket(String serviceName, long requestId,
			GNSResponseCode code, String returnValue) {
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
		this.type = Packet.getPacketType(json);
		this.clientRequestId = json.getLong(CLIENTREQUESTID);
		this.serviceName = json.getString(SERVICENAME);
		this.returnValue = json.getString(RETURNVALUE);
		if (json.has(ERRORCODE)) {
			this.errorCode = GNSResponseCode.getResponseCode(json
					.getInt(ERRORCODE));
		} else {
			this.errorCode = GNSResponseCode.NO_ERROR;
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
	public CommandValueReturnPacket(long requestId, int errorNumber,
			String serviceName, String responseValue) {
		this.setType(PacketType.COMMAND_RETURN_VALUE);
		this.clientRequestId = requestId;
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
	 * 
	 * @return The byte array
	 */
	public byte[] toBytes() {
		try {

			// We need to include the following fields in our byte array:

			// private long clientRequestId; - 8 bytes
			// private long LNSRequestId; - 8 bytes
			// private final GNSResponseCode errorCode; - Represented by an
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
	 *            The byte array created by the toBytes method of a
	 *            CommandValueReturnPacket
	 * @return The CommandValueReturnPacket represented by the bytes
	 * @throws UnsupportedEncodingException
	 */
	public static final CommandValueReturnPacket fromBytes(byte[] bytes)
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
		return new CommandValueReturnPacket(clientReqId, errorCodeInt,
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
