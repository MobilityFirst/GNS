
package edu.umass.cs.gnscommon.packets;

import edu.umass.cs.gigapaxos.PaxosConfig.PC;
import edu.umass.cs.gigapaxos.interfaces.ClientRequest;
import edu.umass.cs.gnsclient.client.CommandUtils;
import edu.umass.cs.gnsclient.client.GNSClientConfig;
import edu.umass.cs.gnscommon.CommandType;
import edu.umass.cs.gnscommon.GNSProtocol;
import edu.umass.cs.gnscommon.ResponseCode;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnscommon.utils.CanonicalJSON;
import edu.umass.cs.gnscommon.utils.JSONByteConverter;
import edu.umass.cs.gnsserver.gnsapp.packet.BasicPacketWithClientAddress;
import edu.umass.cs.gnsserver.gnsapp.packet.Packet;
import edu.umass.cs.nio.JSONPacket;
import edu.umass.cs.nio.MessageNIOTransport;
import edu.umass.cs.nio.interfaces.Byteable;
import edu.umass.cs.reconfiguration.interfaces.ReplicableRequest;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import edu.umass.cs.utils.Config;
import edu.umass.cs.utils.Util;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;


public class CommandPacket extends BasicPacketWithClientAddress implements
        ClientRequest, ReplicableRequest, Byteable {

  private final static String QID = GNSProtocol.REQUEST_ID.toString();
  private final static String COMMAND = GNSProtocol.COMMAND_QUERY.toString();
  // paper over arbitrary string changes in the protocol
  private final static boolean SUPPORT_OLD_PROTOCOL = true;
  private final static String OLD_COMMAND_PACKET_REQUESTID = "clientreqID";
  private final static String OLD_COMMAND_PACKET_COMMAND = "command";


  public final static String BOGUS_SERVICE_NAME = GNSProtocol.UNKNOWN_NAME
          .toString();


  private final long clientRequestId;


  private final JSONObject command;


  private boolean forceCoordination = false;

  // never serialized
  private Object result = null;


  public CommandPacket(long requestId, JSONObject command) {
    this(requestId, command, true);
  }


  public CommandPacket(long requestId, JSONObject command, boolean validate) {
    this.setType(Packet.PacketType.COMMAND);
    this.clientRequestId = requestId;
    this.command = command;
    if (validate) {
      validateCommandType();
    }
  }


  public CommandPacket(JSONObject json) throws JSONException {
    this.type = Packet.getPacketType(json);

    if (!SUPPORT_OLD_PROTOCOL) {
      this.clientRequestId = json.getLong(QID);
      this.command = json.getJSONObject(COMMAND);
    } else {
      if (json.has(QID)) {
        this.clientRequestId = json.getLong(QID);
      } else if (json.has(OLD_COMMAND_PACKET_REQUESTID)) {
        this.clientRequestId = json.getLong(OLD_COMMAND_PACKET_REQUESTID);
      } else {
        throw new JSONException("Packet missing field " + QID);
      }
      if (json.has(COMMAND)) {
        this.command = json.getJSONObject(COMMAND);
      } else if (json.has(OLD_COMMAND_PACKET_COMMAND)) {
        this.command = json.getJSONObject(OLD_COMMAND_PACKET_COMMAND);
      } else {
        throw new JSONException("Packet missing field " + COMMAND);
      }
    }

    this.forceCoordination = json.has(GNSProtocol.FORCE_COORDINATE_READS.toString())
            ? json.getBoolean(GNSProtocol.FORCE_COORDINATE_READS.toString()) : false;

    validateCommandType();
  }


  public CommandPacket(byte[] bytes) throws RequestParseException {
    ByteBuffer buf = ByteBuffer.wrap(bytes);


    // packet type
    this.setType(Packet.getPacketType(buf.getInt()));
    // requestID
    this.clientRequestId = buf.getLong();
    // forceCoordination
    this.forceCoordination = (buf.get() == 1);
    // ByteMode
    ByteMode mode = ByteMode.byteModeMap.get(
            (int) buf.get());
    // JSON command
    this.command = getJSONObject(buf, mode);

    validateCommandType();
  }


  // Note that this implementation of this method will do nothing in production code because
  // assertions will be disabled.
  protected void validateCommandType() {
    assert (!this.getCommandType().isMutualAuth());
  }

  private static JSONObject fromBytesStringerHack(ByteBuffer buf)
          throws UnsupportedEncodingException, JSONException {
    JSONObject json = new JSONObject();
    // query type
    json.put(GNSProtocol.COMMAND_INT.toString(), buf.getInt());

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
      for (ByteMode mode : ByteMode.values()) {
        byteModeMap.put(mode.val, mode);
      }
    }
  }

  // used only at sender; receiver reads from packet
  private static final ByteMode byteMode = ByteMode.byteModeMap.get(Config
          .getGlobalInt(GNSClientConfig.GNSCC.BYTE_MODE));


  @Override
  public final byte[] toBytes() {
    try {
      switch (byteMode) {

        case ORG_JSON:
          return this.toJSONObject().toString()
                  .getBytes(MessageNIOTransport.NIO_CHARSET_ENCODING);
        case HOMEBREW:
          return this.appendByteifiedInnerJSONCommand(
                  this.toByteBufferWithOuterFields(),
                  JSONByteConverter.toBytesHardcoded(this.command));
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
      return ByteBuffer.allocate(512).putInt(
              // packet type
              this.getType().getInt())
              // requestID
              .putLong(this.clientRequestId)
              // forceCoordination
              .put(this.forceCoordination ? (byte) 1 : (byte) 0)
              // ByteMode
              .put((byte) byteMode.val);
      // JSON command coming next
    }
  }

  private byte[] appendByteifiedInnerJSONCommand(ByteBuffer bbuf, byte[] inner) {
    return bbuf.remaining() >= inner.length ? Arrays.copyOfRange(
            bbuf.put(inner).array(), 0, bbuf.position()) : ByteBuffer
            .allocate(bbuf.position() + inner.length)
            .put(bbuf.array(), 0, bbuf.position()).put(inner).array();
  }


  private byte[] toBytesWingItAsString(ByteBuffer buf, JSONObject json) {
    // can we still get integer-less packets from iOS devices?
    Integer commandType = (Integer) command
            .remove(GNSProtocol.COMMAND_INT.toString());
    assert (commandType != null);

    // query type
    buf.putInt(commandType != null ? commandType : -1);

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

        byte[] valueBytes = ((String) objVal)
                .getBytes(MessageNIOTransport.NIO_CHARSET_ENCODING);

        // Grow the buffer if needed.
        if (buf.remaining() < (Integer.BYTES + keyBytes.length
                + Integer.BYTES + valueBytes.length)) {
          buf = ByteBuffer.allocate(
                  buf.capacity() * 2 + Integer.BYTES
                  + keyBytes.length + Integer.BYTES
                  + valueBytes.length)
                  // arun: Fixed bug here that was missing the flip
                  .put((ByteBuffer) buf.flip());
        }

        buf.putInt(keyBytes.length).put(keyBytes)
                .putInt(valueBytes.length).put(valueBytes);
      }
      // Trim any unused buffer space.
      return ByteBuffer.wrap(new byte[buf.position()])
              .put(buf.array(), 0, buf.position()).array();

    } catch (UnsupportedEncodingException | JSONException | ClassCastException e) {

      // System.err.println(e + " for " + key + ":" + objVal);
      return this.handleSerializationException(e, commandType);
    } finally {
      // This stops the toBytes method form being destructive.
      try {
        this.putBackRemoved(commandType);
      } catch (JSONException e) {
        return this.handleSerializationException(e, commandType);
      }
    }

  }

  private void putBackRemoved(Integer commandType) throws JSONException {
    if (commandType != null) {
      this.command.put(GNSProtocol.COMMAND_INT.toString(), commandType);
    }
    ;
  }

  private byte[] handleSerializationException(Exception e,
          Integer commandType) {
    // testing => scream
    if (Config.getGlobalBoolean(PC.ENABLE_INSTRUMENTATION)) {
      throw new RuntimeException(e);
    }
    // production => try slow path
    try {
      this.command.put(GNSProtocol.COMMAND_INT.toString(), commandType);
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


  @Override
  public JSONObject toJSONObject() throws JSONException {
    JSONObject json = new JSONObject();
    Packet.putPacketType(json, getType());
    json.put(QID, this.clientRequestId);
    json.put(COMMAND, this.command);
    if (this.forceCoordination) {
      json.put(GNSProtocol.FORCE_COORDINATE_READS.toString(), this.forceCoordination);
    }
    return json;
  }


  @Override
  public long getRequestID() {
    return clientRequestId;
  }


  @Override
  public ClientRequest getResponse() {
    return this.response;
  }


  public JSONObject getCommand() {
    return command;
  }


  @Override
  public String getServiceName() {
    try {
      if (command != null) {
        if (command.has(GNSProtocol.GUID.toString())) {
          return command.getString(GNSProtocol.GUID.toString());
        }
        if (command.has(GNSProtocol.NAME.toString())) {
          return command.getString(GNSProtocol.NAME.toString());
        }
      }
    } catch (JSONException e) {
      // Just ignore it
    }
    return BOGUS_SERVICE_NAME;
  }


  public int getCommandInteger() {
    try {
      if (command != null) {
        if (command.has(GNSProtocol.COMMAND_INT.toString())) {
          return command.getInt(GNSProtocol.COMMAND_INT.toString());
        }
      }
    } catch (JSONException e) {
    }
    return -1;
  }


  @Override
  public boolean needsCoordination() {
    return this.forceCoordination || getCommandType().isUpdate();
  }


  public CommandType getCommandType() {
    return getJSONCommandType(command);
  }


  public static CommandType getJSONCommandType(JSONObject command) {
    try {
      if (command != null) {
        if (command.has(GNSProtocol.COMMAND_INT.toString())) {
          return CommandType.getCommandType(command
                  .getInt(GNSProtocol.COMMAND_INT.toString()));
        }
        if (command.has(GNSProtocol.COMMANDNAME.toString())) {
          return CommandType.valueOf(command
                  .getString(GNSProtocol.COMMANDNAME.toString()));
        }
      }
    } catch (IllegalArgumentException | JSONException e) {
    }
    return CommandType.Unknown;
  }


  public ClientRequest setForceCoordinatedReads(boolean force) {
    if (force && (getCommandType().isRead())) {
      this.forceCoordination = true;
    }
    return this;
  }



  public void finish() {
    synchronized (this) {
      while (!this.executed) {
        try {
          this.wait();
        } catch (InterruptedException e) {
          e.printStackTrace();
          // continue waiting
        }
      }
    }
  }

  // internal utility method
  private String getRespStr() throws ClientException {
    this.finish();
    if (this.result != null) {
      ResponsePacket responsePacket = CommandUtils.checkResponse((ResponsePacket) this.result, this);
      // checkResponse explicitly returns null!
      if (responsePacket == null) {
        return null;
      } else {
        return responsePacket.getReturnValue();
      }
    } else {
      return null;
    }
  }

  private boolean executed = false;


  CommandPacket setResult(ResponsePacket responsePacket) {
    // Note: this method has nothing to do with setResponse(ClientRequest)
    synchronized (this) {
      if (this.result == null) {
        this.executed = true;
        this.result = responsePacket;
        this.notifyAll();
      } else {
        throw new RuntimeException(
                "Can not set response more than once");
      }
    }
    return this;
  }



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
      throw new ClientException(ResponseCode.JSON_PARSE_ERROR,
              e.getMessage());
    }
  }


  public String getResultString() throws ClientException {
    return this.getRespStr();
  }


  public JSONObject getResultJSONObject() throws ClientException {
    String responseStr = this.getRespStr();
    try {
      JSONObject json = responseStr != null ? new JSONObject(responseStr) : new JSONObject();
      return json;
    } catch (JSONException e) {
      throw new ClientException(ResponseCode.JSON_PARSE_ERROR,
              e.getMessage() + " while parsing response string "
              + responseStr);
    }
  }


  public Map<String, ?> getResultMap() throws ClientException {
    String responseStr = this.getRespStr();
    try {
      Map<String, ?> map = Util.JSONObjectToMap(responseStr != null ? new JSONObject(responseStr) : new JSONObject());
      return map;
    } catch (JSONException e) {
      throw new ClientException(ResponseCode.JSON_PARSE_ERROR,
              e.getMessage());
    }
  }


  public List<?> getResultList() throws ClientException {
    String responseStr = this.getRespStr();
    try {
      List<?> list = Util.JSONArrayToList(responseStr != null ? new JSONArray(responseStr) : new JSONArray());
      return list;
    } catch (JSONException e) {
      throw new ClientException(ResponseCode.JSON_PARSE_ERROR,
              e.getMessage());
    }
  }


  public JSONArray getResultJSONArray() throws ClientException {
    String responseStr = this.getRespStr();
    try {
      return responseStr != null ? new JSONArray(responseStr) : new JSONArray();
    } catch (JSONException e) {
      throw new ClientException(ResponseCode.JSON_PARSE_ERROR,
              e.getMessage());
    }
  }


  public boolean getResultBoolean() throws ClientException {
    Object obj = this.getResult();
    if (obj != null && obj instanceof Boolean) {
      return (boolean) obj;
    }
    throw new ClientException(ResponseCode.JSON_PARSE_ERROR,
            "Unable to parse response as boolean");
  }


  public int getResultInt() throws ClientException {
    Object obj = this.getResult();
    if (obj != null && obj instanceof Integer) {
      return (int) obj;
    }
    throw new ClientException(ResponseCode.JSON_PARSE_ERROR,
            "Unable to parse response as boolean");
  }


  public long getResultLong() throws ClientException {
    Object obj = this.getResult();
    if (obj != null && obj instanceof Long) {
      return (long) obj;
    }
    throw new ClientException(ResponseCode.JSON_PARSE_ERROR,
            "Unable to parse response as boolean");
  }


  public double getResultDouble() throws ClientException {
    Object obj = this.getResult();
    if (obj != null && obj instanceof Double) {
      return (double) obj;
    }
    throw new ClientException(ResponseCode.JSON_PARSE_ERROR,
            "Unable to parse response as boolean");
  }

  private static Object getResultValueFromString(String str)
          throws ClientException {
    return CanonicalJSON.stringToValue(str);
  }


  public boolean hasResult() {
    return this.result != null;
  }



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
