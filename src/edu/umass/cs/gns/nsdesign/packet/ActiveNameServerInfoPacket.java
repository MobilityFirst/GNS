/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.nsdesign.packet;

import edu.umass.cs.gns.util.Stringifiable;
import org.json.JSONException;
import org.json.JSONObject;
import java.util.Set;

/**
 * This class implements the packet transmitted between local nameserver and a primary nameserver to toString information
 * about the current active nameserver set.
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
   * @param localNameServer
   * @param name Host/domain/device name
   * @param recordKey
   ***********************************************************
   */
  public ActiveNameServerInfoPacket(int localNameServer, String name, String recordKey) {
    this.type = Packet.PacketType.ACTIVE_NAMESERVER_INFO;
    this.primaryNameServer = null;
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
  public ActiveNameServerInfoPacket(JSONObject json, Stringifiable<NodeIDType> unstringer) throws JSONException {
    if (Packet.getPacketType(json) != Packet.PacketType.ACTIVE_NAMESERVER_INFO) {
      Exception e = new Exception("NewReplicaPacket: wrong packet type " + Packet.getPacketType(json));
      e.printStackTrace();
      return;
    }

    this.type = Packet.PacketType.ACTIVE_NAMESERVER_INFO;
    this.primaryNameServer = json.has(PRIMARY_NAMESERVER) ? unstringer.valueOf(json.getString(PRIMARY_NAMESERVER)) : null;
    this.localNameServer = json.getInt(LOCAL_NAMESERVER);
    this.recordKey = json.getString(RECORDKEY);
    this.name = json.getString(NAME);
    this.activeNameServers = json.has(ACTIVE_NAMESERVERS) ? 
            unstringer.getValuesFromJSONArray(json.getJSONArray(ACTIVE_NAMESERVERS)) : null;
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
    if (primaryNameServer != null) {
      json.put(PRIMARY_NAMESERVER, primaryNameServer);
    }
    json.put(LOCAL_NAMESERVER, getLocalNameServer());
    json.put(RECORDKEY, getRecordKey());
    json.put(NAME, getName());
    if (activeNameServers != null) {
      json.put(ACTIVE_NAMESERVERS, activeNameServers);
    }

    return json;
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
