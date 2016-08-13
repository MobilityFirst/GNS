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
 * Initial developer(s): Westy */
package edu.umass.cs.gnsserver.gnsapp.packet;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;

import edu.umass.cs.gigapaxos.PaxosConfig.PC;
import edu.umass.cs.gigapaxos.interfaces.ClientRequest;
import edu.umass.cs.gnscommon.CommandType;
import edu.umass.cs.gnscommon.CommandValueReturnPacket;
import edu.umass.cs.gnscommon.GNSCommandProtocol;
import edu.umass.cs.gnscommon.GNSResponseCode;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnsserver.gnsapp.packet.Packet.PacketType;
import static edu.umass.cs.gnsserver.gnsapp.packet.Packet.putPacketType;
import edu.umass.cs.gnsserver.main.GNSConfig;
import edu.umass.cs.nio.JSONPacket;
import edu.umass.cs.nio.MessageNIOTransport;
import edu.umass.cs.nio.interfaces.Byteable;
import edu.umass.cs.reconfiguration.ReconfigurationConfig.RC;
import edu.umass.cs.reconfiguration.interfaces.ReplicableRequest;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReplicableClientRequest;
import edu.umass.cs.utils.Config;

import java.util.List;
import java.util.Iterator;
import java.util.Map;
import java.util.logging.Level;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.value.ImmutableValue;
import org.msgpack.value.Value;
import org.msgpack.value.ValueType;

import static edu.umass.cs.gnsserver.gnsapp.packet.Packet.getPacketType;
import static edu.umass.cs.gnsserver.gnsapp.packet.Packet.getPacketType;
import static edu.umass.cs.gnsserver.gnsapp.packet.Packet.getPacketType;
import static edu.umass.cs.gnsserver.gnsapp.packet.Packet.getPacketType;
import static edu.umass.cs.gnsserver.gnsapp.packet.Packet.getPacketType;
import static edu.umass.cs.gnsserver.gnsapp.packet.Packet.getPacketType;
import static edu.umass.cs.gnsserver.gnsapp.packet.Packet.getPacketType;
import static edu.umass.cs.gnsserver.gnsapp.packet.Packet.getPacketType;

/**
 * @author arun, westy
 *
 *         Packet format sent from a client and handled by a local name server.
 *
 */
public class CommandPacket extends BasicPacketWithClientAddress implements
		ClientRequest, ReplicableRequest {

	private final static String CLIENTREQUESTID = "qid";
	private final static String COMMAND = "command";

	/**
	 * bogus service name
	 */
	public final static String BOGUS_SERVICE_NAME = "unknown";

	/**
	 * Identifier of the request on the client.
	 */
	private final long clientRequestId;

	/**
	 * The JSON form of the command. Always includes a COMMANDNAME field. Almost
	 * always has a GUID field or NAME (for HRN records) field.
	 */
	private final JSONObject command;

	private Object result = null;

	/**
	 * Create a CommandPacket instance.
	 *
	 * @param requestId
	 * @param command
	 */
	public CommandPacket(long requestId, JSONObject command) {
		this.setType(PacketType.COMMAND);
		this.clientRequestId = requestId;
		this.command = command;

	}

	/**
	 * Creates a CommandPacket instance from a JSONObject.
	 *
	 * @param json
	 * @throws JSONException
	 */
	public CommandPacket(JSONObject json) throws JSONException {
		this.type = getPacketType(json);
		this.clientRequestId = json.getLong(CLIENTREQUESTID);
		this.command = json.getJSONObject(COMMAND);

	}

	/**
	 * Reconstructs a CommandPacket from a given byte array.
	 * 
	 * @param bytes
	 *            The bytes given by the toBytes method.
	 * @throws JSONException
	 * @throws UnsupportedEncodingException
	 */
	public CommandPacket(byte[] bytes) throws JSONException,
			UnsupportedEncodingException {
		ByteBuffer buf = ByteBuffer.wrap(bytes);
		int packetType = buf.getInt();
		int commandType = buf.getInt();
		this.clientRequestId = buf.getLong();
		this.setType(Packet.getPacketType(packetType));

		this.command = new JSONObject();
		this.command.put(GNSCommandProtocol.COMMAND_INT, commandType);

		// Put in the variable length fields.
		while (buf.hasRemaining()) {
			int keyLength = buf.getInt();
			byte[] keyBytes = new byte[keyLength];
			buf.get(keyBytes);
			String key = new String(keyBytes,
					MessageNIOTransport.NIO_CHARSET_ENCODING);
			int valueLength = buf.getInt();
			byte[] valueBytes = new byte[valueLength];
			buf.get(valueBytes);
			String value = new String(valueBytes,
					MessageNIOTransport.NIO_CHARSET_ENCODING);
			this.command.put(key, value);
		}
	}

	/**
	 * Converts the CommandPacket to bytes. Assumes that all fields other than
	 * GNSCommandProtocol.COMMAND_INT have strings for values. Automatically
	 * expands the output buffer as needed.
	 * 
	 * @return
	 * @throws JSONException
	 * @throws UnsupportedEncodingException
	 */
	private final byte[] toBytesExpanding(byte[] startingArray)
			throws JSONException, UnsupportedEncodingException {
		/* We assume it will be less than 2048 length to start, and will grow if
		 * needed. */
		ByteBuffer buf = ByteBuffer.allocate(2048);
		buf.put(startingArray, 0, Integer.BYTES // commandType
				+ Integer.BYTES // packetType
				+ Long.BYTES // requestID
		);

		/* We assume all remaining keys and values are strings. If not, we would
		 * never come here. */
		@SuppressWarnings("unchecked")
		Iterator<String> keys = command.keys();
		while (keys.hasNext()) {
			String key = keys.next();
			byte[] keyBytes = key
					.getBytes(MessageNIOTransport.NIO_CHARSET_ENCODING);
			String value = command.getString(key);
			byte[] valueBytes = value
					.getBytes(MessageNIOTransport.NIO_CHARSET_ENCODING);
			// Grow the buffer if needed.
			if (buf.remaining() < (Integer.BYTES + keyBytes.length
					+ Integer.BYTES + valueBytes.length)) {
				ByteBuffer newBuf = ByteBuffer.allocate(buf.capacity() * 2
						+ Integer.BYTES + keyBytes.length + Integer.BYTES
						+ valueBytes.length);
				newBuf.put(buf);
				buf = newBuf;
			}
			buf.putInt(keyBytes.length);
			buf.put(keyBytes);
			buf.putInt(valueBytes.length);
			buf.put(valueBytes);
		}
		// Trim any unused buffer space.
		byte[] outByteArray = new byte[buf.position()];
		ByteBuffer outputBuffer = ByteBuffer.wrap(outByteArray);
		outputBuffer.put(buf.array(), 0, outByteArray.length);
		return outputBuffer.array();
	}

	/**
	 * Converts the CommandPacket to bytes. Assumes that all fields other than
	 * GNSCommandProtocol.COMMAND_INT have strings for values.
	 * 
	 * @return Refer {@link Byteable#toBytes()}
	 */
	public final byte[] toBytes() {
		/* We assume it will be less than 1024 length to start, and will grow if
		 * needed. */
		ByteBuffer buf = ByteBuffer.allocate(1024);

		synchronized (command) {
			PacketType packetTypeInstance;
			packetTypeInstance = this.getType();
			int packetType = packetTypeInstance.getInt();
			int commandType = (int) command
					.remove(GNSCommandProtocol.COMMAND_INT);
			buf.putInt(packetType);
			buf.putInt(commandType);
			buf.putLong(this.clientRequestId);
			byte[] output;

			// We assume all remaining keys and values are strings.
			@SuppressWarnings("unchecked")
			Iterator<String> keys = command.keys();
			try {
				while (keys.hasNext()) {
					String key = keys.next();
					byte[] keyBytes = key
							.getBytes(MessageNIOTransport.NIO_CHARSET_ENCODING);
					Object objVal = command.get(key);
					/* We rely on the assumption that if it's not a String, it
					 * will throw a ClassCastException */
					String value = (String) objVal;
					byte[] valueBytes = value
							.getBytes(MessageNIOTransport.NIO_CHARSET_ENCODING);
					buf.putInt(keyBytes.length);
					buf.put(keyBytes);
					buf.putInt(valueBytes.length);
					buf.put(valueBytes);
				}
				// Trim any unused buffer space.
				byte[] outByteArray = new byte[buf.position()];
				ByteBuffer outputBuffer = ByteBuffer.wrap(outByteArray);
				outputBuffer.put(buf.array(), 0, outByteArray.length);
				output = outputBuffer.array();

			} catch (BufferOverflowException boe) {
				// Use the slower expanding buffer method.
				try {
					output = this.toBytesExpanding(buf.array());
				} catch (UnsupportedEncodingException | JSONException
						| ClassCastException e) {
					return this.handleSerializationException(e);
				}
			} catch (UnsupportedEncodingException | JSONException
					| ClassCastException e) {
				return this.handleSerializationException(e);
			} finally {
				// This stops the toBytes method form being destructive.
				try {
					command.put(GNSCommandProtocol.COMMAND_INT, commandType);
				} catch (JSONException e) {
					return this.handleSerializationException(e);
				}
			}
			return output;
		}

	}

	private byte[] handleSerializationException(Exception e) {
		// testing => scream
		if (Config.getGlobalBoolean(PC.ENABLE_INSTRUMENTATION))
			throw new RuntimeException(e);
		// production => try slow path
		else
			return toJSONBytes();
	}

	private byte[] toJSONBytes() {
		try {
			return this.toString().getBytes(
					MessageNIOTransport.NIO_CHARSET_ENCODING);
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Converts the command object into a JSONObject.
	 *
	 * @return the JSONObject
	 * @throws org.json.JSONException
	 */
	@Override
	public JSONObject toJSONObject() throws JSONException {
		JSONObject json = new JSONObject();
		putPacketType(json, getType());
		json.put(CLIENTREQUESTID, this.clientRequestId);
		json.put(COMMAND, this.command);
		return json;
	}

	/**
	 * Return the client request id.
	 *
	 * @return the client request id
	 */
	public long getClientRequestId() {
		return clientRequestId;
	}

	/**
	 * Return the client request id as a long.
	 *
	 * @return the client request id
	 */
	@Override
	public long getRequestID() {
		return clientRequestId;
	}

	/**
	 * For ClientRequest.
	 *
	 * @return the response
	 */
	@Override
	public ClientRequest getResponse() {
		return this.response;
	}

	/**
	 * Return the command.
	 *
	 * @return the command
	 */
	protected JSONObject getCommand() {
		return command;
	}

	/**
	 * The service name is the name of the GUID/HRN that is being written to or
	 * read.
	 */
	@Override
	public String getServiceName() {
		try {
			if (command != null) {
				if (command.has(GNSCommandProtocol.GUID)) {
					return command.getString(GNSCommandProtocol.GUID);
				}
				if (command.has(GNSCommandProtocol.NAME)) {
					return command.getString(GNSCommandProtocol.NAME);
				}
			}
		} catch (JSONException e) {
			// Just ignore it
		}
		return BOGUS_SERVICE_NAME;
	}

	/**
	 * @return True if this command needs to be coordinated at servers or
	 *         executed locally.
	 */
	public boolean getCommandCoordinateReads() {
		try {
			// arun: optBoolean is inefficient (~6us)
			return command != null
					&& command.has(GNSCommandProtocol.COORDINATE_READS)
					&& command.getBoolean(GNSCommandProtocol.COORDINATE_READS);
		} catch (JSONException e) {
			;
		}
		return false;
	}

	/**
	 * @return CommandType as Integer.
	 */
	public int getCommandInteger() {
		try {
			if (command != null) {
				if (command.has(GNSCommandProtocol.COMMAND_INT)) {
					return command.getInt(GNSCommandProtocol.COMMAND_INT);
				}
			}
		} catch (JSONException e) {
		}
		return -1;
	}

	/**
	 * @return CommandType
	 */
	public CommandType getCommandType() {
		try {
			if (command != null) {
				if (command.has(GNSCommandProtocol.COMMAND_INT)) {
					return CommandType.getCommandType(command
							.getInt(GNSCommandProtocol.COMMAND_INT));
				}
				if (command.has(GNSCommandProtocol.COMMANDNAME)) {
					return CommandType.valueOf(command
							.getString(GNSCommandProtocol.COMMANDNAME));
				}
			}
		} catch (IllegalArgumentException | JSONException e) {
		}
		return CommandType.Unknown;
	}

	@Override
	public boolean needsCoordination() {
		CommandType commandType = getCommandType();
		return (commandType.isRead() && getCommandCoordinateReads())
				|| commandType.isUpdate();
	}

	/**
	 * @param force
	 * @return Set coordination mode to true if this is a read command.
	 */
	public ClientRequest setForceCoordinatedReads(boolean force) {
		if (force && getCommandType().isRead())
			// make forcibly coordinated
			return ReplicableClientRequest.wrap(this, true);
		// else
		return this;
	}

	/* ********************** Start of result-related methods **************** */

	/**
	 * Waits till this command has finished execution.
	 */
	public void finish() {
		synchronized (this) {
			while (!this.executed)
				try {
					this.wait();
				} catch (InterruptedException e) {
					e.printStackTrace();
					// continue waiting
				}
		}
	}

	// internal utility method
	private String getRespStr() {
		this.finish();
		if (this.result != null)
			return ((CommandValueReturnPacket) this.result).getReturnValue();
		else
			return null;
	}

	private boolean executed = false;

	/**
	 * Used to set the response obtained by executing this request.
	 * 
	 * 
	 * @param responseStr
	 * @return this
	 */
	CommandPacket setResult(CommandValueReturnPacket responsePacket) {
		// Note: this method has nothing to do with setResponse(ClientRequest)
		synchronized (this) {
			if (this.result == null) {
				this.executed = true;
				this.result = responsePacket;
				this.notifyAll();
			} else
				throw new RuntimeException(
						"Can not set response more than once");
		}
		return this;
	}

	/**
	 * arun: The getResult methods below must satisfy the following invariants:
	 * (1) A successful (without exceptions) invocation of the method can return
	 * a non-null value at most once. This invariant implies that this.result
	 * should be set to null upon a successful invocation.
	 * 
	 * (2) The method is atomic (all-or-none), i.e., exceptions because of the
	 * caller expecting the wrong result type, e.g., invoking getResultList when
	 * the response is a Map, should not change any state and still allow the
	 * caller to still call other getResult methods until one is successful.
	 * This invariant implies that this.result should be reset to null only for
	 * successful calls.
	 */

	/**
	 * @return The result of executing this command.
	 * @throws ClientException
	 */
	public Object getResult() throws ClientException {
		// else
		String responseStr = this.getRespStr();
		try {
			Object retval = responseStr != null
					&& JSONPacket.couldBeJSONObject(responseStr) ? new JSONObject(
					responseStr)
					: responseStr != null
							&& JSONPacket.couldBeJSONArray(responseStr) ? new JSONArray(
							responseStr)
							: getResultValueFromString(responseStr);
			return retval;
		} catch (JSONException e) {
			throw new ClientException(GNSResponseCode.JSON_PARSE_ERROR,
					e.getMessage());
		}
	}

	/**
	 * @return The String result of executing this command.
	 * @throws ClientException
	 */
	public String getResultString() throws ClientException {
		return this.getRespStr();
	}

	/**
	 * @return The JSONObject result of executing this command.
	 * @throws ClientException
	 */
	public JSONObject getResultJSONObject() throws ClientException {
		String responseStr = this.getRespStr();
		try {
			JSONObject json = new JSONObject(responseStr);
			return json;
		} catch (JSONException e) {
			throw new ClientException(GNSResponseCode.JSON_PARSE_ERROR,
					e.getMessage() + " while parsing response string "
							+ responseStr);
		}
	}

	/**
	 * @return The Map<String,?> result of executing this command.
	 * @throws ClientException
	 */
	public Map<String, ?> getResultMap() throws ClientException {
		String responseStr = this.getRespStr();
		try {
			Map<String, ?> map = JSONObjectToMap(new JSONObject(responseStr));
			return map;
		} catch (JSONException e) {
			throw new ClientException(GNSResponseCode.JSON_PARSE_ERROR,
					e.getMessage());
		}
	}

	/**
	 * @return The JSONObject result of executing this command.
	 * @throws ClientException
	 */
	public List<?> getResultList() throws ClientException {
		String responseStr = this.getRespStr();
		try {
			List<?> list = JSONArrayToList(new JSONArray(responseStr));
			return list;
		} catch (JSONException e) {
			throw new ClientException(GNSResponseCode.JSON_PARSE_ERROR,
					e.getMessage());
		}
	}

	/* arun: This method will be a utility method to convert a JSONArray to an
	 * ArrayList while recursively converting the elements to JSONObject or
	 * JSONArray as needed. */
	private static List<?> JSONArrayToList(JSONArray jsonArray) {
		throw new RuntimeException("Unimplemented");
	}

	/* arun: This method will be a utility method to convert a JSONObject to a
	 * Map<String,?> while recursively converting the values to JSONObject or
	 * JSONArray as needed. */
	private static Map<String, ?> JSONObjectToMap(JSONObject jsonArray) {
		throw new RuntimeException("Unimplemented");
	}

	/**
	 * @return The JSONArray result of executing this command.
	 * @throws ClientException
	 */
	public JSONArray getResultJSONArray() throws ClientException {
		String responseStr = this.getRespStr();
		try {
			return new JSONArray(responseStr);
		} catch (JSONException e) {
			throw new ClientException(GNSResponseCode.JSON_PARSE_ERROR,
					e.getMessage());
		}
	}

	/**
	 * @return boolean result value of executing this command.
	 * @throws ClientException
	 */
	public boolean getResultBoolean() throws ClientException {
		Object obj = this.getResult();
		if (obj != null && obj instanceof Boolean)
			return (boolean) obj;
		throw new ClientException(GNSResponseCode.JSON_PARSE_ERROR,
				"Unable to parse response as boolean");
	}

	/**
	 * @return int result value of executing this command.
	 * @throws ClientException
	 */
	public int getResultInt() throws ClientException {
		Object obj = this.getResult();
		if (obj != null && obj instanceof Integer)
			return (int) obj;
		throw new ClientException(GNSResponseCode.JSON_PARSE_ERROR,
				"Unable to parse response as boolean");
	}

	/**
	 * @return long result value of executing this command.
	 * @throws ClientException
	 */
	public long getResultLong() throws ClientException {
		Object obj = this.getResult();
		if (obj != null && obj instanceof Long)
			return (long) obj;
		throw new ClientException(GNSResponseCode.JSON_PARSE_ERROR,
				"Unable to parse response as boolean");
	}

	/**
	 * @return double result value of executing this command.
	 * @throws ClientException
	 */
	public double getResultDouble() throws ClientException {
		Object obj = this.getResult();
		if (obj != null && obj instanceof Double)
			return (double) obj;
		throw new ClientException(GNSResponseCode.JSON_PARSE_ERROR,
				"Unable to parse response as boolean");
	}

	private static Object getResultValueFromString(String str)
			throws ClientException {
		return JSONObject.stringToValue(str);
	}

	/**
	 * @return True if this command has the result of its execution.
	 */
	public boolean hasResult() {
		return this.result != null;
	}

	/* ********************** End of result-related methods **************** */

	@Override
	public Object getSummary() {
		return new Object() {
			@Override
			public String toString() {
				return getRequestType()
						+ ":"
						+ getCommandType().toString()
						+ ":"
						+ getCommandInteger()
						+ ":"
						+ getServiceName()
						+ ":"
						+ getRequestID()
						+ (getClientAddress() != null ? "["
								+ getClientAddress() + "]" : "");
			}
		};
	}

}
