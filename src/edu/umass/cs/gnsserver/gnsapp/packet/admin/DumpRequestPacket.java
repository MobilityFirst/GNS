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
package edu.umass.cs.gnsserver.gnsapp.packet.admin;

import edu.umass.cs.gnsserver.gnsapp.packet.BasicPacketWithReturnAddress;
import edu.umass.cs.gnsserver.gnsapp.packet.Packet;
import edu.umass.cs.nio.interfaces.Stringifiable;

import java.net.InetSocketAddress;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 * This class implements the packet transmitted between local nameserver and a primary
 * nameserver to toString information about the contents of the nameserver.
 *
 * @author Westy
 * @param <NodeIDType>
 */
public class DumpRequestPacket<NodeIDType> extends BasicPacketWithReturnAddress {

  /** id */
  public final static String ID = "id";

  /** primary */
  public final static String PRIMARY_NAMESERVER = "primary";

  /** json */
  public final static String JSON = "json";
  private final static String ARGUMENT = "arg";
  private int id;
  /**
   * Primary name server receiving the request *
   */
  private NodeIDType primaryNameServer;

  /**
   * Place where the results are kept *
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
   * Constructs new DumpRequestPacket from a JSONObject
   *
   * @param json JSONObject representing this packet
   * @param unstringer
   * @throws org.json.JSONException
   */
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

  /**
   * Return the id.
   * 
   * @return the id
   */
  public int getId() {
    return id;
  }

  /**
   * Set the id.
   * 
   * @param id
   */
  public void setId(int id) {
    this.id = id;
  }

  /**
   * Return the json array.
   * 
   * @return the json array
   */
  public JSONArray getJsonArray() {
    return jsonArray;
  }

  /**
   * Set the json array.
   * 
   * @param jsonArray
   */
  public void setJsonArray(JSONArray jsonArray) {
    this.jsonArray = jsonArray;
  }

  /**
   * Return the primary Name Server.
   * 
   * @return the primary Name Server
   */
  public NodeIDType getPrimaryNameServer() {
    return primaryNameServer;
  }

  /**
   * Set the primary Name Server.
   * 
   * @param primaryNameServer
   */
  public void setPrimaryNameServer(NodeIDType primaryNameServer) {
    this.primaryNameServer = primaryNameServer;
  }

  /**
   * Get the argument.
   * 
   * @return the argument
   */
  public String getArgument() {
    return argument;
  }
}
