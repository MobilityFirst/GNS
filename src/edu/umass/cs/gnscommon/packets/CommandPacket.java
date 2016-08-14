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
package edu.umass.cs.gnscommon.packets;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;

import edu.umass.cs.gigapaxos.PaxosConfig.PC;
import edu.umass.cs.gigapaxos.interfaces.ClientRequest;
import edu.umass.cs.gnsclient.client.GNSClientConfig;
import edu.umass.cs.gnscommon.CommandType;
import edu.umass.cs.gnscommon.GNSCommandProtocol;
import edu.umass.cs.gnscommon.GNSProtocol;
import edu.umass.cs.gnscommon.GNSResponseCode;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnscommon.utils.JSONByteConverter;
import edu.umass.cs.gnsserver.gnsapp.packet.BasicPacketWithClientAddress;
import edu.umass.cs.gnsserver.gnsapp.packet.Packet;
import edu.umass.cs.gnsserver.gnsapp.packet.Packet.PacketType;
import static edu.umass.cs.gnsserver.gnsapp.packet.Packet.putPacketType;
import edu.umass.cs.gnsserver.main.GNSConfig;
import edu.umass.cs.nio.JSONPacket;
import edu.umass.cs.nio.MessageNIOTransport;
import edu.umass.cs.nio.interfaces.Byteable;
import edu.umass.cs.reconfiguration.ReconfigurationConfig.RC;
import edu.umass.cs.reconfiguration.interfaces.ReplicableRequest;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReplicableClientRequest;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import edu.umass.cs.utils.Config;
import edu.umass.cs.utils.DefaultTest;
import edu.umass.cs.utils.Util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Iterator;
import java.util.Map;
import java.util.logging.Level;

import junit.framework.Assert;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.junit.Test;
import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.value.ImmutableValue;
import org.msgpack.value.Value;
import org.msgpack.value.ValueType;

/**
 * @author arun, westy
 *
 *         Packet format sent from a client and handled by a local name server.
 *
 */
public class CommandPacket extends BasicPacketWithClientAddress implements
		ClientRequest, ReplicableRequest, Byteable {

	private final static String CLIENTREQUESTID = GNSProtocol.REQUEST_ID
			.toString();
	private final static String COMMAND = GNSProtocol.QUERY.toString();
	/**
	 * Refer {@link GNSProtocol#UNKNOWN_NAME}.
	 */
	public final static String BOGUS_SERVICE_NAME = GNSProtocol.UNKNOWN_NAME
			.toString();

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
		this.type = Packet.getPacketType(json);
		this.clientRequestId = json.getLong(CLIENTREQUESTID);
		this.command = json.getJSONObject(COMMAND);

	}

	/**
	 * Reconstructs a CommandPacket from a given byte array.
	 * 
	 * @param bytes
	 *            The bytes given by the toBytes method.
	 * @throws RequestParseException
	 */
	public CommandPacket(byte[] bytes) throws RequestParseException {
		ByteBuffer buf = ByteBuffer.wrap(bytes);

		/**
		 * We will come here only if this class implements Byteable and the
		 * sender also implements Byteable. If the sender used toJSONObject(),
		 * we won't come here because
		 * {@link GNSApp#getRequest(byte[],NIOHeader)} will directly invoke
		 * {@link Packet#createInstance(JSONObject, Stringifiable<String>)}
		 */
		// packet type
		this.setType(Packet.getPacketType(buf.getInt()));
		// requestID
		this.clientRequestId = buf.getLong();

		// JSON command
		this.command = getJSONObject(buf, ByteMode.byteModeMap.get(
		// ByteMode
				(int) buf.get()));
	}

	private static JSONObject fromBytesStringerHack(ByteBuffer buf)
			throws UnsupportedEncodingException, JSONException {
		JSONObject json = new JSONObject();
		json
		// query type
		.put(GNSCommandProtocol.COMMAND_INT, buf.getInt())
		// force coordination flag
				.putOpt(GNSCommandProtocol.FORCE_COORDINATE_READS,
						buf.get() == 1 ? true : null);

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
			json.put(key, value);
		}
		return json;
	}

	private static JSONObject getJSONObject(ByteBuffer bbuf, ByteMode mode)
			throws RequestParseException {
		try {
			switch (mode) {
			case ORG_JSON:
				throw new RuntimeException("Should never come here");
			case HOMEBREW:
				return JSONByteConverter.fromBytesHardcoded(bbuf);
			case JACKSON:
				return JSONByteConverter.fromBytesJackson(bbuf);
			case MSGPACK:
				return JSONByteConverter.fromBytesMsgpack(bbuf);
			case STRING_WING:
				return fromBytesStringerHack(bbuf);
			default:
				throw new RuntimeException("Unrecognized byteification mode");
			}
		} catch (IOException | JSONException e) {
			throw new RequestParseException(e);
		}
	}

	private static enum ByteMode {
		ORG_JSON(0), HOMEBREW(1), JACKSON(2), MSGPACK(3), STRING_WING(4);

		private final int val;

		ByteMode(int val) {
			this.val = val;
		}

		private static final Map<Integer, ByteMode> byteModeMap = new HashMap<Integer, ByteMode>();
		static {
			for (ByteMode mode : ByteMode.values())
				byteModeMap.put(mode.val, mode);
		}
	}

	// used only at sender; receiver reads from packet
	private static final ByteMode byteMode = ByteMode.byteModeMap.get(Config
			.getGlobalInt(GNSClientConfig.GNSCC.BYTE_MODE));

	/**
	 * Converts the CommandPacket to bytes. Assumes that all fields other than
	 * GNSCommandProtocol.COMMAND_INT have strings for values.
	 * 
	 * @return Refer {@link Byteable#toBytes()}
	 */
	public final byte[] toBytes() {
		try {
			switch (byteMode) {
			/* There is little point in using JSON just for this.command instead
			 * of the default toJSONObject() method, so we just do that. */
			case ORG_JSON:
				return this.toJSONObject().toString()
						.getBytes(MessageNIOTransport.NIO_CHARSET_ENCODING);
			case HOMEBREW:
				return this.appendByteifiedInnerJSONCommand(
						this.toByteBufferWithOuterFields(),
						JSONByteConverter.toBytesHardcoded(this.command));
			case JACKSON:
				return this.appendByteifiedInnerJSONCommand(
						this.toByteBufferWithOuterFields(),
						JSONByteConverter.toBytesJackson(this.command));
			case MSGPACK:
				return this.appendByteifiedInnerJSONCommand(
						this.toByteBufferWithOuterFields(),
						JSONByteConverter.toBytesMsgpack(this.command));
			case STRING_WING:
				// different from above three
				return this.toBytesWingItAsString(
						toByteBufferWithOuterFields(), this.command);
			default:
				throw new RuntimeException("Unrecognized byteification mode");
			}
		} catch (JSONException | IOException e) {
			e.printStackTrace();
			throw new RuntimeException("Unable to byteify " + this);
		}
	}

	private ByteBuffer toByteBufferWithOuterFields() {
		synchronized (command) {
			return ByteBuffer.allocate(1024).putInt(
			// packet type
					this.getType().getInt())
			// requestID
					.putLong(this.clientRequestId)
					// ByteMode
					.put((byte) byteMode.val);
			// JSON command next
		}
	}

	private byte[] appendByteifiedInnerJSONCommand(ByteBuffer bbuf, byte[] inner) {
		return bbuf.remaining() >= inner.length ? Arrays.copyOfRange(
				bbuf.put(inner).array(), 0, bbuf.position()) : ByteBuffer
				.allocate(bbuf.position() + inner.length)
				.put(bbuf.array(), 0, bbuf.position()).put(inner).array();
	}

	/* This is a hack attempt that assumes that almost all values as strings.
	 * Hack because field "values" in general are not meant to be strings and we
	 * will just be putting the overhead elsewhere if we try to ensure that they
	 * are strings. But this still works and is useful for instrumentation
	 * purposes. */
	private byte[] toBytesWingItAsString(ByteBuffer buf, JSONObject json) {
		// can we still get integer-less packets from iOS devices?
		Integer commandType = (Integer) command
				.remove(GNSCommandProtocol.COMMAND_INT);
		assert (commandType != null);
		boolean forceCoordinated = this.removeForceCoordinated();

		// query type
		buf.putInt(commandType != null ? commandType : -1)
		// force coordination
				.put(forceCoordinated ? (byte) 1 : (byte) 0);

		// We assume all remaining keys and values are strings.
		@SuppressWarnings("unchecked")
		Iterator<String> keys = command.keys();
		Object objVal = null;
		String key = null;
		try {
			while (keys.hasNext()) {
				key = keys.next();
				byte[] keyBytes = key
						.getBytes(MessageNIOTransport.NIO_CHARSET_ENCODING);
				objVal = command.get(key);
				/* We rely on the assumption that if it's not a String, it will
				 * throw a ClassCastException */
				byte[] valueBytes = ((String) objVal)
						.getBytes(MessageNIOTransport.NIO_CHARSET_ENCODING);

				// Grow the buffer if needed.
				if (buf.remaining() < (Integer.BYTES + keyBytes.length
						+ Integer.BYTES + valueBytes.length))
					buf = ByteBuffer.allocate(
							buf.capacity() * 2 + Integer.BYTES
									+ keyBytes.length + Integer.BYTES
									+ valueBytes.length)
					// arun: Fixed bug here that was missing the flip
							.put((ByteBuffer) buf.flip());

				buf.putInt(keyBytes.length).put(keyBytes)
						.putInt(valueBytes.length).put(valueBytes);
			}
			// Trim any unused buffer space.
			return ByteBuffer.wrap(new byte[buf.position()])
					.put(buf.array(), 0, buf.position()).array();

		} catch (UnsupportedEncodingException | JSONException
				| ClassCastException e) {
			/* arun: Uncomment to check with "ant test" that many values are not
			 * strings. This usually does not break code because the server
			 * decodes the strings correctly, but it is unwise to rely on that
			 * behavior and difficult to correctly maintain in code. */
			// System.err.println(e + " for " + key + ":" + objVal);
			return this.handleSerializationException(e, commandType,
					forceCoordinated);
		} finally {
			// This stops the toBytes method form being destructive.
			try {
				this.putBackRemoved(commandType, forceCoordinated);
			} catch (JSONException e) {
				return this.handleSerializationException(e, commandType,
						forceCoordinated);
			}
		}

	}

	private void putBackRemoved(Integer commandType, boolean forceCoordinated)
			throws JSONException {
		if (commandType != null)
			this.command.put(GNSCommandProtocol.COMMAND_INT, commandType);
		if (forceCoordinated)
			this.command.put(GNSCommandProtocol.FORCE_COORDINATE_READS,
					forceCoordinated);
	}

	private byte[] handleSerializationException(Exception e,
			Integer commandType, boolean forceCoordinated) {
		// testing => scream
		if (Config.getGlobalBoolean(PC.ENABLE_INSTRUMENTATION))
			throw new RuntimeException(e);
		// production => try slow path
		try {
			this.command.put(GNSCommandProtocol.COMMAND_INT, commandType);
		} catch (JSONException e1) {
			throw new RuntimeException(e1);

		}
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
					&& command.has(GNSCommandProtocol.FORCE_COORDINATE_READS)
					&& command
							.getBoolean(GNSCommandProtocol.FORCE_COORDINATE_READS);
		} catch (JSONException e) {
			;
		}
		return false;
	}

	private boolean removeForceCoordinated() {
		return this.command.has(GNSCommandProtocol.FORCE_COORDINATE_READS)
				&& (Boolean) this.command
						.remove(GNSCommandProtocol.FORCE_COORDINATE_READS);
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
			return ((ResponsePacket) this.result).getReturnValue();
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
	CommandPacket setResult(ResponsePacket responsePacket) {
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
	private static List<?> JSONArrayToList(JSONArray jsonArray)
			throws JSONException {
		if (jsonArray == null)
			return null;
		ArrayList<Object> list = new ArrayList<Object>();
		Object val = null;

		if (jsonArray.length() > 0)
			for (int i = 0; i < jsonArray.length(); i++)
				if ((val = jsonArray.get(i)) instanceof JSONObject)
					list.add(JSONObjectToMap((JSONObject) val));

				else if (val instanceof JSONArray)
					list.add(JSONArrayToList((JSONArray) val));

				else
					// primitive type object
					list.add(val);
		return list;
	}

	/* arun: This method will be a utility method to convert a JSONObject to a
	 * Map<String,?> while recursively converting the values to JSONObject or
	 * JSONArray as needed. */
	private static Map<String, ?> JSONObjectToMap(JSONObject json)
			throws JSONException {
		if (json == null)
			return null;
		String[] keys = JSONObject.getNames(json);
		if (keys != null) {
			Map<String, Object> map = new HashMap<String, Object>();
			for (String key : keys)
				if (json.get(key) instanceof JSONObject)
					map.put(key, JSONObjectToMap(json.getJSONObject(key)));
				else if (json.get(key) instanceof JSONArray)
					map.put(key, JSONArrayToList(json.getJSONArray(key)));

				else
					// primitive type object
					map.put(key, json.get(key));
			return map;
		}
		return null;
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

	/**
	 * Unit testing.
	 */
	public static class CommandPacketTest extends DefaultTest {
		/**
		 * @throws JSONException
		 */
		@SuppressWarnings("unchecked")
		@Test
		public void test_01_JSONObjectToMap() throws JSONException {
			Util.assertAssertionsEnabled();
			Map<String, ?> map = (JSONObjectToMap(new JSONObject()
					.put("hello", "world")
					.put("collField", Arrays.asList("hello", "world", 123))
					.put("jsonField", new JSONObject().put("foo", true))));
			System.out.println(map);
			org.junit.Assert.assertTrue(map.get("jsonField") instanceof Map
					&& (Boolean) ((Map<String, ?>) map.get("jsonField"))
							.get("foo"));

		}

		/**
		 * @throws JSONException
		 */
		@SuppressWarnings("unchecked")
		@Test
		public void test_01_JSONArrayToMap() throws JSONException {
			Util.assertAssertionsEnabled();
			List<?> list = (JSONArrayToList(new JSONArray()

			.put("hello") // 0

					.put(Arrays.asList("hello", "world", 123)) // 1

					.put(new JSONObject().put("foo", true)))); // 2

			System.out.println(list);
			org.junit.Assert.assertTrue(list.get(2) instanceof Map
					&& (Boolean) ((Map<String, ?>) list.get(2)).get("foo"));

		}
	}
}
