/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.nsdesign.packet;

import edu.umass.cs.gns.nsdesign.nodeconfig.GNSNodeConfig;
import edu.umass.cs.gns.util.Util;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.HashSet;
import java.util.Set;

/**
 * This class implements the packet transmitted between local nameserver and a primary nameserver to toString information
 about the current active nameserver set.
 *
 * @author Hardeep Uppal
 * @param <NodeIDType>
 */
public class ActiveNameServerInfoPacket<NodeIDType> extends BasicPacket {

  public final static String PRIMARY_NAMESERVER = "primary";
  public final static String LOCAL_NAMESERVER = "local";
  public final static String NAME = "name";
  public final static String RECORDKEY = "recordkey";
  public final static String ACTIVE_NAMESERVERS = "active";
  /**
   * Name (service/host/domain or device name) *
   */
  private String name;
  /**
   * The key of the value key pair. For GNRS this will be EdgeRecord, CoreRecord or GroupRecord. *
   */
  private String recordKey;
  /**
   * Primary name server receiving the request *
   */
  private NodeIDType primaryNameServer;
  /**
   * Local name server sending the request *
   */
  private int localNameServer;
  /**
   * Set containing ids of active name servers *
   */
  private Set<NodeIDType> activeNameServers;

  public ActiveNameServerInfoPacket() {
  }

  /**
   * ***********************************************************
   * Constructs new ActiveNameServerPacket with the give parameter.
   *
   * @param localNameServer
   * @param primaryNameServer
   * @param recordType
   * @param activeNameservers
   * @param name Host/domain/device name
   ***********************************************************
   */
  public ActiveNameServerInfoPacket(int localNameServer, NodeIDType primaryNameServer, String name, String recordType, Set<NodeIDType> activeNameservers) {
    this.type = Packet.PacketType.ACTIVE_NAMESERVER_INFO;
    this.primaryNameServer = primaryNameServer;
    this.localNameServer = localNameServer;
    this.recordKey = recordType;
    this.name = name;
    this.activeNameServers = activeNameservers;
  }

  /**
   * ***********************************************************
   * Constructs new ActiveNameServerPacket with the give parameter.
   *
   * @param localNameserver Local name server sending the request
   * @param name Host/domain/device name
   ***********************************************************
   */
  public ActiveNameServerInfoPacket(int localNameServer, String name, String recordKey) {
    this.type = Packet.PacketType.ACTIVE_NAMESERVER_INFO;
    this.primaryNameServer = (NodeIDType) GNSNodeConfig.INVALID_NAME_SERVER_ID;
    this.localNameServer = localNameServer;
    this.recordKey = recordKey;
    this.name = name;
  }

  /**
   * ***********************************************************
   * Constructs new ActiveNameServerPacket from a JSONObject
   *
   * @param json JSONObject representing this packet
   * @throws org.json.JSONException
   ***********************************************************
   */
  public ActiveNameServerInfoPacket(JSONObject json) throws JSONException {
    if (Packet.getPacketType(json) != Packet.PacketType.ACTIVE_NAMESERVER_INFO) {
      Exception e = new Exception("NewReplicaPacket: wrong packet type " + Packet.getPacketType(json));
      e.printStackTrace();
      return;
    }

    this.type = Packet.PacketType.ACTIVE_NAMESERVER_INFO;
    this.primaryNameServer = (NodeIDType) json.get(PRIMARY_NAMESERVER);
    this.localNameServer = json.getInt(LOCAL_NAMESERVER);
    this.recordKey = json.getString(RECORDKEY);
    this.name = json.getString(NAME);
    this.activeNameServers = json.has(ACTIVE_NAMESERVERS) ? Util.stringToSetOfNodeId(json.getString(NAME)) : null;
  }

  /**
   * ***********************************************************
   * Converts a ActiveNSUpdatePacket to a JSONObject.
   *
   * @return JSONObject representing this packet.
   * @throws org.json.JSONException
   ***********************************************************
   */
  @Override
  public JSONObject toJSONObject() throws JSONException {
    JSONObject json = new JSONObject();
    Packet.putPacketType(json, getType());
    json.put(PRIMARY_NAMESERVER, primaryNameServer.toString());
    json.put(LOCAL_NAMESERVER, getLocalNameServer());
    json.put(RECORDKEY, getRecordKey());
    json.put(NAME, getName());
    if (activeNameServers != null) {
      json.put(ACTIVE_NAMESERVERS, Util.setOfNodeIdToString(activeNameServers));
    }

    return json;
  }

  /**
   * **********************************************************
   * Converts a JSONArray to an Set of Integers
   *
   * @param json JSONArray
   * @return Set<Integer> with the content of JSONArray.
   * @throws org.json.JSONException
   **********************************************************
   */
  private Set<Integer> toSetInteger(JSONArray json) throws JSONException {
    Set<Integer> set = new HashSet<Integer>();

    if (json == null) {
      return set;
    }

    for (int i = 0; i < json.length(); i++) {
      set.add(new Integer(json.getString(i)));
    }

    return set;
  }

  /**
   * Test *
   */
  public static void main(String[] args) throws Exception {
    long t1 = System.currentTimeMillis();
    ActiveNameServerInfoPacket pkt = new ActiveNameServerInfoPacket(13, "h.com", "EdgeRecord");	//0ms
    long t2 = System.currentTimeMillis();
    JSONObject json = pkt.toJSONObject();		//3ms
    long t3 = System.currentTimeMillis();
    String jsonString = json.toString();		//2ms
    long t31 = System.currentTimeMillis();
    byte[] bytearray = jsonString.getBytes();	//0ms
    long t4 = System.currentTimeMillis();
    jsonString = new String(bytearray);			//0ms
    long t41 = System.currentTimeMillis();
    json = new JSONObject(jsonString);			//2ms
    long t42 = System.currentTimeMillis();
    pkt = new ActiveNameServerInfoPacket(json);	//0ms
    long t43 = System.currentTimeMillis();

    System.out.println("pkt:" + (t2 - t1));
    System.out.println("json:" + (t3 - t2));
    System.out.println("jsonString:" + (t31 - t3));
    System.out.println("bytearray:" + (t4 - t31));
    System.out.println("jsonString:" + (t41 - t4));
    System.out.println("json:" + (t42 - t41));
    System.out.println("pkt:" + (t43 - t42));

    Set active = new HashSet();
    active.add("1");
    active.add("2");
    active.add("3");
    pkt.activeNameServers = active;
    System.out.println(pkt.toJSONObject());
    pkt = new ActiveNameServerInfoPacket(pkt.toJSONObject());
    System.out.println(pkt.toString());
  }

  /**
   * @return the name
   */
  public String getName() {
    return name;
  }

  /**
   * @return the recordKey
   */
  public String getRecordKey() {
    return recordKey;
  }

  /**
   * @return the primaryNameServer
   */
  public NodeIDType getPrimaryNameServer() {
    return primaryNameServer;
  }

  /**
   * @return the localNameServer
   */
  public int getLocalNameServer() {
    return localNameServer;
  }

  /**
   * @return the activeNameServers
   */
  public Set<NodeIDType> getActiveNameServers() {
    return activeNameServers;
  }

  public void setPrimaryNameServer(NodeIDType primaryNameServer) {
    this.primaryNameServer = primaryNameServer;
  }

  public void setActiveNameServers(Set<NodeIDType> activeNameServers) {
    this.activeNameServers = activeNameServers;
  }
}
