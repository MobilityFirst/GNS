package edu.umass.cs.gnrs.packet;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * ***********************************************************
 * This class implements the packet transmitted between local nameserver and a primary nameserver to get information about the
 * contents of the nameserver;
 *
 * @author Westy
 ***********************************************************
 */
public class DumpRequestPacket extends BasicPacket {

  public final static String ID = "id";
  public final static String PRIMARY_NAMESERVER = "primary";
  public final static String LOCAL_NAMESERVER = "local";
  public final static String JSON = "json";
  private final static String ARGUMENT = "arg";
  private int id;
  /**
   * Primary name server receiving the request *
   */
  private int primaryNameServer;
  /**
   * Local name server sending the request *
   */
  private int localNameServer;
  /**
   * JOSNObject where the results are kept *
   */
  private JSONArray jsonArray;
  private String argument;

  /**
   * Constructs a new DumpRequestPacket packet
   * @param localNameServer
   * @param primaryNameServer
   * @param jsonArray
   * @param argument 
   */
  public DumpRequestPacket(int localNameServer, int primaryNameServer, JSONArray jsonArray, String argument) {
    this.type = Packet.PacketType.DUMP_REQUEST;
    this.id = 0;
    this.primaryNameServer = primaryNameServer;
    this.localNameServer = localNameServer;
    this.jsonArray = jsonArray;
    this.argument = argument;
  }

  /**
   * Constructs a new DumpRequestPacket packet
   * @param localNameServer 
   */
  public DumpRequestPacket(int localNameServer) {
    this(localNameServer, -1, new JSONArray(), null);
  }
  
  /**
   * Constructs a new DumpRequestPacket packet
   * @param localNameServer
   * @param tagName 
   */
  public DumpRequestPacket(int localNameServer, String tagName) {
    this(localNameServer, -1, new JSONArray(), tagName);
  }

  /**
   * ***********************************************************
   * Constructs new DumpRequestPacket from a JSONObject
   *
   * @param json JSONObject representing this packet
   * @throws JSONException
   ***********************************************************
   */
  public DumpRequestPacket(JSONObject json) throws JSONException {
    if (Packet.getPacketType(json) != Packet.PacketType.DUMP_REQUEST) {
      Exception e = new Exception("DumpRequestPacket: wrong packet type " + Packet.getPacketType(json));
      e.printStackTrace();
      return;
    }

    this.type = Packet.getPacketType(json);
    this.id = json.getInt(ID);
    this.primaryNameServer = json.getInt(PRIMARY_NAMESERVER);
    this.localNameServer = json.getInt(LOCAL_NAMESERVER);
    this.jsonArray = json.getJSONArray(JSON);
    this.argument = json.optString(ARGUMENT, null);
  }
  
  /**
   * ***********************************************************
   * Converts a DumpRequestPacket to a JSONObject.
   *
   * @return JSONObject representing this packet.
   * @throws JSONException
   ***********************************************************
   */
  @Override
  public JSONObject toJSONObject() throws JSONException {
    JSONObject json = new JSONObject();
    Packet.putPacketType(json, getType());
    json.put(ID, id);
    json.put(PRIMARY_NAMESERVER, primaryNameServer);
    json.put(LOCAL_NAMESERVER, localNameServer);
    json.put(JSON, jsonArray);
    if (this.argument != null) {
      json.put(ARGUMENT, argument);
    }
    return json;
  }
  
  
  public int getId() {
    return id;
  }

  public void setId(int id) {
    this.id = id;
  }

  public JSONArray getJsonArray() {
    return jsonArray;
  }

  public void setJsonArray(JSONArray jsonArray) {
    this.jsonArray = jsonArray;
  }

  public int getLocalNameServer() {
    return localNameServer;
  }

  public void setLocalNameServer(int localNameServer) {
    this.localNameServer = localNameServer;
  }

  public int getPrimaryNameServer() {
    return primaryNameServer;
  }

  public void setPrimaryNameServer(int primaryNameServer) {
    this.primaryNameServer = primaryNameServer;
  }

  public String getArgument() {
    return argument;
  }

}
