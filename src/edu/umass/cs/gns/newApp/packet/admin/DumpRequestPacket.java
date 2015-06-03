package edu.umass.cs.gns.newApp.packet.admin;

import edu.umass.cs.gns.newApp.packet.BasicPacketWithCCPAddress;
import edu.umass.cs.gns.newApp.packet.Packet;
import edu.umass.cs.gns.nio.Stringifiable;

import java.net.InetSocketAddress;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 * This class implements the packet transmitted between local nameserver and a primary nameserver to toString information about the
 contents of the nameserver;
 *
 * @author Westy
 * @param <NodeIDType>
 */
public class DumpRequestPacket<NodeIDType> extends BasicPacketWithCCPAddress {

  public final static String ID = "id";
  public final static String PRIMARY_NAMESERVER = "primary";
  public final static String JSON = "json";
  private final static String ARGUMENT = "arg";
  private int id;
  /**
   * Primary name server receiving the request *
   */
  private NodeIDType primaryNameServer;

  /**
   * JOSNObject where the results are kept *
   */
  private JSONArray jsonArray;
  private String argument;

  /**
   * Constructs a new DumpRequestPacket packet
   * @param id
   * @param primaryNameServer
   * @param lnsAddress
   * @param jsonArray
   * @param argument 
   */
  public DumpRequestPacket(int id, InetSocketAddress lnsAddress, NodeIDType primaryNameServer, JSONArray jsonArray, String argument) {
    super(lnsAddress);
    this.type = Packet.PacketType.DUMP_REQUEST;
    this.id = id;
    this.primaryNameServer = primaryNameServer;
    //this.localNameServer = localNameServer;
    //this.lnsAddress = lnsAddress;
    this.jsonArray = jsonArray;
    this.argument = argument;
  }

  /**
   * Constructs a new DumpRequestPacket packet
   * @param id
   * @param lnsAddress 
   */
  public DumpRequestPacket(int id, InetSocketAddress lnsAddress) {
    this(id, lnsAddress, null, new JSONArray(), null);
  }

  /**
   * Constructs a new DumpRequestPacket packet
   * @param id
   * @param lnsAddress
   * @param tagName 
   */
  public DumpRequestPacket(int id, InetSocketAddress lnsAddress, String tagName) {
    this(id, lnsAddress, null, new JSONArray(), tagName);
  }

  /**
   *
   * Constructs new DumpRequestPacket from a JSONObject
   *
   * @param json JSONObject representing this packet
   * @throws org.json.JSONException
   */
  public DumpRequestPacket(JSONObject json, Stringifiable<NodeIDType> unstringer) throws JSONException {
    super(json.optString(CCP_ADDRESS, null), json.optInt(CCP_PORT, INVALID_PORT));
    if (Packet.getPacketType(json) != Packet.PacketType.DUMP_REQUEST) {
      Exception e = new Exception("DumpRequestPacket: wrong packet type " + Packet.getPacketType(json));
      e.printStackTrace();
      return;
    }
    this.type = Packet.getPacketType(json);
    this.id = json.getInt(ID);
    this.primaryNameServer = json.has(PRIMARY_NAMESERVER) ? unstringer.valueOf(json.getString(PRIMARY_NAMESERVER)) : null;
    this.jsonArray = json.getJSONArray(JSON);
    this.argument = json.optString(ARGUMENT, null);
  }

  /**
   *
   * Converts a DumpRequestPacket to a JSONObject.
   *
   * @return JSONObject representing this packet.
   * @throws org.json.JSONException
   */
  @Override
  public JSONObject toJSONObject() throws JSONException {
    JSONObject json = new JSONObject();
    Packet.putPacketType(json, getType());
    super.addToJSONObject(json);
    json.put(ID, id);
    json.put(PRIMARY_NAMESERVER, primaryNameServer);
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

  public NodeIDType getPrimaryNameServer() {
    return primaryNameServer;
  }

  public void setPrimaryNameServer(NodeIDType primaryNameServer) {
    this.primaryNameServer = primaryNameServer;
  }

  public String getArgument() {
    return argument;
  }
}
