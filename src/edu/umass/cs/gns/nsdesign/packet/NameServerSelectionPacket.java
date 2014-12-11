/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.nsdesign.packet;

import edu.umass.cs.gns.util.Stringifiable;
import java.net.InetSocketAddress;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * ***********************************************************
 * This class provides the packet transmitted from a local name
 * server to indicate its closest (least ping latency) name
 * server.
 *
 * Abhigyan: This packet is needed to do a locality-aware replication in GNS.
 * This packet contains all information needed by replica controllers
 * to make its replication decisions. It sends the number of lookups and updates received by
 * the local name server for a name, in addition to the closest name server to that local name server.
 *
 * The number of lookups are counted as 'votes' for the closest name server; name servers that receive most
 * votes are chosen to be active replicas for a name.
 *
 * IMPORTANT: This information should be sent by an active replica to replica controllers,
 * and it is not the role of local name servers to send this information. However, currently we don't have a
 * way of inferring locality of demand at active name servers, so local name servers are doing this task.
 *
 *
 * @author Abhigyan
 * @param <NodeIDType>
 ***********************************************************
 */
public class NameServerSelectionPacket<NodeIDType> extends BasicPacketWithNSAndLNS<NodeIDType> {

  private final static String NAME = "name";
  private final static String VOTE = "vote";
  private final static String UPDATE = "update";

  /**
   * Name (service/host/domain or device name) *
   */
  private final String name;
  /**
   * Vote = # of lookup since the last vote *
   */

  private final int vote;
  /**
   * Number of updates since the last message for this name *
   */
  private final int update;

  /**
   * Unique ID for this vote message *
   */
  /**
   * ***********************************************************
   * Constructs a new NSLocationPacket with the given parameters
   *
   * @param name Name (service/host/domain or device name)
   * @param vote # of lookup since the last vote
   * @param update
   * @param nameserverID ID of a name server closest to the
   * transmitting local name server
   * @param lnsAddress
   ***********************************************************
   */
  public NameServerSelectionPacket(String name,
          int vote, int update, NodeIDType nameserverID, InetSocketAddress lnsAddress) {
    super(nameserverID, lnsAddress);
    this.type = Packet.PacketType.NAMESERVER_SELECTION;
    this.name = name;
    this.vote = vote;
    this.update = update;
    //this.nameserverID = nameserverID;
    //this.localnameserverID = localnameserverID;
  }

  /**
   * ***********************************************************
   * Constructs a new NSLocationPacket from a JSONObject.
   *
   * @param json JSONObject that represents NSLocaitionPacket
   * @param unstringer
   * @throws org.json.JSONException
   ***********************************************************
   */
  public NameServerSelectionPacket(JSONObject json, Stringifiable<NodeIDType> unstringer) throws JSONException {
    super(json.has(NAMESERVER_ID) ? unstringer.valueOf(json.getString(NAMESERVER_ID)) : null,
            json.optString(LNS_ADDRESS, null), json.optInt(LNS_PORT, INVALID_PORT));
    this.type = Packet.getPacketType(json);
    this.name = json.getString(NAME);
    this.vote = json.getInt(VOTE);
    this.update = json.getInt(UPDATE);
  }

  /**
   * ***********************************************************
   * Converts a NSLocationPacket to a JSONObject
   *
   * @return JSONObject that represents NSLocaitionPacket
   * @throws org.json.JSONException
   ***********************************************************
   */
  @Override
  public JSONObject toJSONObject() throws JSONException {
    JSONObject json = new JSONObject();
    Packet.putPacketType(json, getType());
    super.addToJSONObject(json);
    json.put(NAME, getName());
    json.put(VOTE, getVote());
    json.put(UPDATE, getUpdate());
    return json;
  }

  /**
   * @return the name
   */
  public String getName() {
    return name;
  }

  /**
   * @return the lookup vote
   */
  public int getVote() {
    return vote;
  }

  /**
   * @return the number of updates
   */
  public int getUpdate() {
    return update;
  }

  /**
   * @return UniqueID
   */
  public int getUniqueID() {
    throw new UnsupportedOperationException();
  }
}
