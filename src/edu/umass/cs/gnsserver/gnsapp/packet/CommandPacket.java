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
package edu.umass.cs.gnsserver.gnsapp.packet;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;

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
 * Packet format sent from a client and handled by a local name server.
 *
 */
public class CommandPacket extends BasicPacketWithClientAddress implements ClientRequest, ReplicableRequest{

  private final static String CLIENTREQUESTID = "clientreqID";
  private final static String LNSREQUESTID = "LNSreqID";
  private final static String SENDERADDRESS = MessageNIOTransport.SNDR_IP_FIELD;
  private final static String SENDERPORT = MessageNIOTransport.SNDR_PORT_FIELD;
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
   * LNS identifier - filled in at the LNS.
   * 
   * arun: This will go away as we don't a separate LNSRequestId in this class. We
   * can either rely on ENABLE_ID_TRANSFORM in the async client or the LNS could 
   * simply maintain a re-mapping table with a new CommandPacket in case of 
   * conflicting IDs.
   */
  @Deprecated
  private long LNSRequestId;
  /**
   * The IP address of the sender as a string.
   * 
   * arun: This does not have to be maintained in this class.
   */
  @Deprecated
  private final String senderAddress;
  /**
   * The TCP port of the sender as an int.
   * 
   * arun: This does not have to be maintained in this class.
   */
  @Deprecated
  private final int senderPort;

  /**
   * The JSON form of the command. Always includes a COMMANDNAME field.
   * Almost always has a GUID field or NAME (for HRN records) field.
   */
  private final JSONObject command;
  /**
   * The stop requests needsCoordination() method must return true by default.
   */
  private boolean needsCoordination = true;
  private boolean needsCoordinationExplicitlySet = false;

  private int retransmissions = 0;
  
  private Object result=null;
  
  /**
   * Create a CommandPacket instance.
   *
   * @param requestId
   * @param senderAddress
   * @param command
   * @param senderPort
   */
  private CommandPacket(long requestId, String senderAddress, int senderPort, JSONObject command) {
    this.setType(PacketType.COMMAND);
    this.clientRequestId = requestId;
    /* arun: can only come here via public constructor with no sender address.
     * In preparation of removing sender address altogether from the stringified
     * form.
     */
    assert(senderAddress==null && senderPort==-1);
    this.senderAddress = senderAddress;
    this.senderPort = senderPort;
    this.command = command;

    this.LNSRequestId = -1; // this will be filled in at the LNS
  }

  /**
   * Creates a command packet with a null host and -1 port which will be
   * filled in when the packet is sent out.
   *
   * @param requestId
   * @param command
   */
  public CommandPacket(long requestId, JSONObject command) {
    this(requestId, null, -1, command);
  }
  
  protected CommandPacket() {
	  throw new RuntimeException("This method should not have been called");
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
    if (json.has(LNSREQUESTID)) {
      this.LNSRequestId = json.getLong(LNSREQUESTID);
    } else {
      this.LNSRequestId = json.getLong(CLIENTREQUESTID);//-1;
    }
    this.senderAddress = json.optString(SENDERADDRESS, null);
    this.senderPort = json.has(SENDERPORT) ? json.getInt(SENDERPORT) : -1;
    this.command = json.getJSONObject(COMMAND);

  }
  
  /**
   * Reconstructs a CommandPacket from a given byte array.
   * @param bytes The bytes given by the toBytes method.
   * @return The reconstructed CommandPacket
   * @throws JSONException
   * @throws UnsupportedEncodingException
   */
  public CommandPacket(byte[] bytes) throws JSONException, UnsupportedEncodingException{
  	ByteBuffer buf = ByteBuffer.wrap(bytes);
  	int packetType = buf.getInt();
  	int commandType = buf.getInt();
  	this.clientRequestId = buf.getLong();
  	long cmdClientReqId = buf.getLong();
  	this.LNSRequestId = buf.getLong();
  	this.senderPort = buf.getInt();
  	this.senderAddress = null;	
  	this.setType(Packet.getPacketType(packetType));
  	
  	this.command = new JSONObject();
  	this.command.put(GNSCommandProtocol.COMMAND_INT, commandType); 
  	this.command.put(CLIENTREQUESTID, cmdClientReqId);
  	//cmd.put(LNSREQUESTID, lnsReqId);
  	//this.command.put(SENDERPORT, senderPort);
  	
  	
  	//Put in the variable length fields.
  	while(buf.hasRemaining()){
  		int keyLength = buf.getInt();
  		byte[] keyBytes = new byte[keyLength];
  		buf.get(keyBytes);
  		String key = new String(keyBytes, "ISO-8859-1");
  		int valueLength = buf.getInt();
  		byte[] valueBytes = new byte[valueLength];
  		buf.get(valueBytes);
  		String value = new String(valueBytes, "ISO-8859-1");
  		this.command.put(key, value);
  	}
  }

  /**
   * Reconstructs a CommandPacket from a given byte array.
   * @param bytes The bytes given by the toBytes method.
   * @return The reconstructed CommandPacket
   * @throws JSONException
   * @throws UnsupportedEncodingException
   */
  public static final CommandPacket fromBytes(byte[] bytes) throws JSONException, UnsupportedEncodingException{
  	JSONObject commandPacketJSON = new JSONObject();
  	ByteBuffer buf = ByteBuffer.wrap(bytes);
  	int commandType = buf.getInt();
  	int packetType = buf.getInt();
  	long clientReqId = buf.getLong();
  	long cmdClientReqId = buf.getLong();
  	long lnsReqId = buf.getLong();
  	int senderPort = buf.getInt();
  	
  	//Put in the fixed fields.
  	commandPacketJSON.put(CLIENTREQUESTID, clientReqId);
  	Packet.putPacketType(commandPacketJSON, Packet.getPacketType(packetType));
  	
  	JSONObject cmd = new JSONObject();
  	cmd.put(GNSCommandProtocol.COMMAND_INT, commandType); 
  	cmd.put(CLIENTREQUESTID, cmdClientReqId);
  	cmd.put(LNSREQUESTID, lnsReqId);
  	cmd.put(SENDERPORT, senderPort);
  	
  	
  	//Put in the variable length fields.
  	while(buf.hasRemaining()){
  		int keyLength = buf.getInt();
  		byte[] keyBytes = new byte[keyLength];
  		buf.get(keyBytes);
  		String key = new String(keyBytes, "ISO-8859-1");
  		int valueLength = buf.getInt();
  		byte[] valueBytes = new byte[valueLength];
  		buf.get(valueBytes);
  		String value = new String(valueBytes, "ISO-8859-1");
  		cmd.put(key, value);
  	}
  	commandPacketJSON.put("command", cmd);
  	return new CommandPacket(commandPacketJSON);
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
    if (this.LNSRequestId != -1) {
      json.put(LNSREQUESTID, this.LNSRequestId);
    }
    json.put(COMMAND, this.command);
    /* arun: serializing sender address should never be needed. These 
     * are needed if at all at local name servers to remember the 
     * original sender. Even that could be done by remembering the
     * sender address outside of this class.
     */
    if (senderAddress != null) {
      //json.put(SENDERADDRESS, this.senderAddress);
    }
    if (senderPort != -1) {
      //json.put(SENDERPORT, this.senderPort);
    }
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
   * Return the LNS request id.
   *
   * @return the LNS request id
   */
  public long getLNSRequestId() {
    return LNSRequestId;
  }

  /**
   * Set the LNS request id.
   *
   * @param LNSRequestId
   */
  public void setLNSRequestId(int LNSRequestId) {
    this.LNSRequestId = LNSRequestId;
  }

  /**
   * @return {@code this}
   */
  public CommandPacket incrRetransmissions() {
    this.retransmissions++;
    return this;
  }

  /**
   * @return Number of retransmissions.
   */
  public int getRetransmissions() {
    return this.retransmissions;
  }

  /**
   * Return the sender address.
   *
   * @return a string
   */
  public String getSenderAddress() {
    return senderAddress;
  }

  /**
   * Return the sender port.
   *
   * @return the sender port
   */
  public int getSenderPort() {
    return senderPort;
  }

  /**
   * Return the command.
   *
   * @return the command
   */
  public JSONObject getCommand() {
    return command;
  }

  /**
   * The service name is the name of the GUID/HRN that is being written to
   * or read.
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
 * @return True if this command needs to be coordinated at servers or executed locally.
 */
public boolean getCommandCoordinateReads() {
    try {
      // arun: optBoolean is inefficient (~6us)
      return command != null
              && command.has(GNSCommandProtocol.COORDINATE_READS)
              && command.getBoolean(GNSCommandProtocol.COORDINATE_READS);
    } catch (JSONException e) {;
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
          return CommandType.getCommandType(command.getInt(GNSCommandProtocol.COMMAND_INT));
        }
        if (command.has(GNSCommandProtocol.COMMANDNAME)) {
          return CommandType.valueOf(command.getString(GNSCommandProtocol.COMMANDNAME));
        }
      }
    } catch (IllegalArgumentException | JSONException e) {
    }
    return CommandType.Unknown;
  }

  @Override
  public boolean needsCoordination() {
    if (needsCoordinationExplicitlySet) {
      if (needsCoordination) {
        GNSConfig.getLogger().log(Level.FINER, "{0} needs coordination (set)", this);
      }
      return needsCoordination;
    } else {
      // Cache it.
      needsCoordinationExplicitlySet = true;
      CommandType commandType = getCommandType();
      needsCoordination = (commandType.isRead() && getCommandCoordinateReads())
              || commandType.isUpdate();
      if (needsCoordination) {
        GNSConfig.getLogger().log(Level.FINER, "{0} needs coordination", this);
      }
      return needsCoordination;
    }
  }
  
	/**
	 * @param force 
	 * @return Set coordination mode to true if this is a read command.
	 */
	public ClientRequest setForceCoordinatedReads(boolean force) {
		if (force && getCommandType().isRead())
			// make forcibly coordinated
			return ReplicableClientRequest.wrap(this,true);
		// else
		return this;
	}

  @Override
  public void setNeedsCoordination(boolean needsCoordination) {
    needsCoordinationExplicitlySet = true;
    this.needsCoordination = needsCoordination;
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
			}
			else throw new RuntimeException("Can not set response more than once");
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
					e.getMessage() + " while parsing response string " + responseStr);
		}
	}
	
	/**
	 * @return The Map<String,?> result of executing this command.
	 * @throws ClientException
	 */
	public Map<String,?> getResultMap() throws ClientException {
		String responseStr = this.getRespStr();
		try {
			Map<String,?> map = JSONObjectToMap(new JSONObject(responseStr));
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
	
	/* arun: This method will be a utility method to convert a JSONArray to
	 * an ArrayList while recursively converting the elements to JSONObject 
	 * or JSONArray as needed.
	 */
	private static List<?> JSONArrayToList(JSONArray jsonArray) {
		throw new RuntimeException("Unimplemented");
	}
	
	/* arun: This method will be a utility method to convert a JSONObject to
	 * a Map<String,?> while recursively converting the values to JSONObject 
	 * or JSONArray as needed.
	 */
	private static Map<String,?> JSONObjectToMap(JSONObject jsonArray) {
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
		if(obj != null && obj instanceof Boolean) return (boolean)obj;
		throw new ClientException(GNSResponseCode.JSON_PARSE_ERROR, "Unable to parse response as boolean");
	}
	
	/**
	 * @return int result value of executing this command.
	 * @throws ClientException
	 */
	public int getResultInt() throws ClientException {
		Object obj = this.getResult();
		if(obj != null && obj instanceof Integer) return (int)obj;
		throw new ClientException(GNSResponseCode.JSON_PARSE_ERROR, "Unable to parse response as boolean");
	}
	
	/**
	 * @return long result value of executing this command.
	 * @throws ClientException
	 */
	public long getResultLong() throws ClientException {
		Object obj = this.getResult();
		if(obj != null && obj instanceof Long) return (long)obj;
		throw new ClientException(GNSResponseCode.JSON_PARSE_ERROR, "Unable to parse response as boolean");
	}

	/**
	 * @return double result value of executing this command.
	 * @throws ClientException
	 */
	public double getResultDouble() throws ClientException {
		Object obj = this.getResult();
		if(obj != null && obj instanceof Double) return (double)obj;
		throw new ClientException(GNSResponseCode.JSON_PARSE_ERROR, "Unable to parse response as boolean");
	}

	private static Object getResultValueFromString(String str) throws ClientException {
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
        return getRequestType() + ":"
                + getCommandType().toString() + ":"
                + getCommandInteger() + ":"
                + getServiceName() + ":"
                + getRequestID() + (getClientAddress() != null ? "["
                + getClientAddress() + "]" : "");
      }
    };
  }
/**
 * Converts the CommandPacket to bytes. Assumes that all fields other than GNSCommandProtocol.COMMAND_INT have strings for values. Automatically expands the output buffer as needed.
 * @return
 * @throws JSONException
 * @throws UnsupportedEncodingException
 */
private final byte[] toBytesExpanding(byte[] startingArray) throws JSONException, UnsupportedEncodingException{
	ByteBuffer buf = ByteBuffer.allocate(2048); //We assume it will be less than 2048 length to start, and will grow if needed.
	buf.put(startingArray, 0, 4+4+8+8+8+4); //Accounts for the values we already put into the array for the commandType, packetType, clientReqId, lnsReqId, senderPort
	@SuppressWarnings("unchecked") //We assume all keys and values are strings.
	Iterator<String> keys = command.keys();
	while (keys.hasNext()){
		String key = keys.next();
		byte[] keyBytes = key.getBytes("ISO-8859-1");
		String value = command.getString(key);
		byte[] valueBytes = value.getBytes("ISO-8859-1");
		//Grow the buffer if needed.
		if (buf.remaining() < (4 + keyBytes.length + 4 + valueBytes.length)){
			ByteBuffer newBuf = ByteBuffer.allocate(buf.capacity()*2 + 4 + keyBytes.length + 4 + valueBytes.length);
			newBuf.put(buf);
			buf = newBuf;
		}
		buf.putInt(keyBytes.length);
		buf.put(keyBytes);
		buf.putInt(valueBytes.length);
		buf.put(valueBytes);
	}
	//Trim any unused buffer space.
	byte[] outByteArray = new byte[buf.position()];
	ByteBuffer outputBuffer = ByteBuffer.wrap(outByteArray);
	outputBuffer.put(buf.array(), 0, outByteArray.length);
	return outputBuffer.array();
}

/**
 * Converts the CommandPacket to bytes. Assumes that all fields other than GNSCommandProtocol.COMMAND_INT have strings for values.
 * @return
 * @throws JSONException
 * @throws UnsupportedEncodingException
 */
public final byte[] toBytes() {
	ByteBuffer buf = ByteBuffer.allocate(1024); //We assume it will be less than 1024 length to start, and will grow if needed.

	synchronized(command){
		PacketType packetTypeInstance;
		packetTypeInstance = this.getType();//getPacketType(command);
		int packetType = packetTypeInstance.getInt();
		long clientRedId = this.clientRequestId;
		long cmdClientReqId = command.has(CLIENTREQUESTID) ? (Long) command.remove(CLIENTREQUESTID) : -1;
		long lnsReqId = command.has(LNSREQUESTID) ? (Long) command.remove(LNSREQUESTID) : cmdClientReqId;
		int senderPort = command.has(SENDERPORT) ? (int) command.remove(SENDERPORT) : -1;

		int commandType = (int) command.remove(GNSCommandProtocol.COMMAND_INT);
		buf.putInt(packetType);
		buf.putInt(commandType);
		buf.putLong(clientRedId);
		buf.putLong(cmdClientReqId);
		buf.putLong(lnsReqId);
		buf.putInt(senderPort);
		byte[] output;
		@SuppressWarnings("unchecked") //We assume all keys and values are strings.
		Iterator<String> keys = command.keys();
		try{
			while (keys.hasNext()){
				String key = keys.next();
				byte[] keyBytes = key.getBytes("ISO-8859-1");
				String value = command.getString(key);
				byte[] valueBytes = value.getBytes("ISO-8859-1");
				buf.putInt(keyBytes.length);
				buf.put(keyBytes);
				buf.putInt(valueBytes.length);
				buf.put(valueBytes);
			}
			//Trim any unused buffer space.
			byte[] outByteArray = new byte[buf.position()];
			ByteBuffer outputBuffer = ByteBuffer.wrap(outByteArray);
			outputBuffer.put(buf.array(), 0, outByteArray.length);
			output = outputBuffer.array();


		} 
		catch(BufferOverflowException boe){
			//Use the slower expanding buffer method.
			try {
				output = this.toBytesExpanding(buf.array());
			} catch (UnsupportedEncodingException | JSONException e) {
				throw new RuntimeException(e);
			}
		} catch (UnsupportedEncodingException | JSONException e) {
			throw new RuntimeException(e);
		}
		finally{
			//This stops the toBytes method form being destructive.
			try {
				command.put(GNSCommandProtocol.COMMAND_INT, commandType);
				Packet.putPacketType(command, packetTypeInstance);
				command.put(CLIENTREQUESTID, cmdClientReqId);
				command.put(LNSREQUESTID, lnsReqId);
				command.put(SENDERPORT, senderPort);
				
			} catch (JSONException e) {
				throw new RuntimeException(e);
			} 
		}
		return output;
	}

}






}
