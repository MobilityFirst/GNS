/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.nsdesign.packet;

import edu.umass.cs.gns.nsdesign.packet.Packet.PacketType;
import edu.umass.cs.gns.util.JSONUtils;
import java.net.InetSocketAddress;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.Set;

/**
 * This packet is sent by local name server to a name server to request the current active replicas
 * for a name. The name server replies also uses this packet to send the reply to a local name server.
 *
 * If the name server is a replica controller for this name and the name exists, then the field
 * <code>activeNameServers</code> in the packet is filled with the current set of active name servers.
 * Otherwise, <code>activeNameServers</code> is set to null.
 *
 * @author Abhigyan
 */
public class RequestActivesPacket extends BasicPacket {

  public static final String NAME = "name";
  public static final String ACTIVES = "actives";
  //public static final String LNSID = "lnsid";
  private final static String LNS_ADDRESS = "lnsAddress";
  private final static String LNS_PORT = "lnsPort";
  public static final String LNS_REQ_ID = "lnsreqid";
  public static final String NSID = "nsid";

  /**
   * Name for which the active replicas are being requested
   */
  private String name;

//  /**
//   * Local name server sending the request.
//   */
//  private int lnsID;
  
  /**
   * Local name server sending the request.
   * Replaces lnsId.
   */
  private InetSocketAddress lnsAddress = null;

  /**
   * Name server that received request from LNS.
   */
  private int nsID;

  /**
   * Active name servers for the name. This field is populated when name server
   * sends a reply to a local name server.
   */
  private Set<Integer> activeNameServers;

  /**
   * Unique request ID assigned by local name server.
   */
  private int lnsRequestID;

  public RequestActivesPacket(String name, InetSocketAddress lnsAddress, int lnsRequestID, int nsID) {
    this.name = name;
    this.type = PacketType.REQUEST_ACTIVES;
    //this.lnsID = lnsID;
    this.lnsAddress = lnsAddress;
    this.lnsRequestID = lnsRequestID;
    this.nsID = nsID;
  }

  public RequestActivesPacket(JSONObject json) throws JSONException {
    this.name = json.getString(NAME);
    this.activeNameServers = JSONUtils.JSONArrayToSetInteger(json.getJSONArray(ACTIVES));
    this.type = PacketType.REQUEST_ACTIVES;
    //this.lnsID = json.getInt(LNSID);
    this.lnsAddress = new InetSocketAddress(json.getString(LNS_ADDRESS), json.getInt(LNS_PORT));
    this.lnsRequestID = json.getInt(LNS_REQ_ID);
    this.nsID = json.getInt(NSID);
  }

  @Override
  public JSONObject toJSONObject() throws JSONException {
    JSONObject json = new JSONObject();
    json.put(NAME, name);
    json.put(ACTIVES, new JSONArray(activeNameServers));
    Packet.putPacketType(json, getType());
    //json.put(LNSID, lnsID);
    json.put(LNS_ADDRESS, lnsAddress.getHostString());
    json.put(LNS_PORT, lnsAddress.getPort());
    json.put(LNS_REQ_ID, lnsRequestID);
    json.put(NSID, nsID);
    return json;
  }

  public void setActiveNameServers(Set<Integer> activeNameServers) {
    this.activeNameServers = activeNameServers;
  }

  public String getName() {
    return name;
  }

  public Set<Integer> getActiveNameServers() {
    return activeNameServers;
  }

//  public int getLNSID() {
//    return lnsID;
//  }
  public InetSocketAddress getLnsAddress() {
    return lnsAddress;
  }

  public int getLnsRequestID() {
    return lnsRequestID;
  }

  public int getNsID() {
    return nsID;
  }
}
