
package edu.umass.cs.gnsserver.gnsapp.packet.admin;

import edu.umass.cs.gnsserver.gnsapp.packet.BasicPacketWithReturnAddress;
import edu.umass.cs.gnsserver.gnsapp.packet.Packet;
import edu.umass.cs.nio.interfaces.Stringifiable;

import java.net.InetSocketAddress;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;


public class DumpRequestPacket<NodeIDType> extends BasicPacketWithReturnAddress {


  public final static String ID = "id";


  public final static String PRIMARY_NAMESERVER = "primary";


  public final static String JSON = "json";
  private final static String ARGUMENT = "arg";
  private int id;

  private NodeIDType primaryNameServer;


  private JSONArray jsonArray;
  private String argument;


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


  public DumpRequestPacket(int id, InetSocketAddress lnsAddress) {
    this(id, lnsAddress, null, new JSONArray(), null);
  }


  public DumpRequestPacket(int id, InetSocketAddress lnsAddress, String tagName) {
    this(id, lnsAddress, null, new JSONArray(), tagName);
  }


  public DumpRequestPacket(JSONObject json, Stringifiable<NodeIDType> unstringer) throws JSONException {
    super(json);
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
