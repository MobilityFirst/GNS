/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.nsdesign.packet;

import edu.umass.cs.gns.nsdesign.nodeconfig.GNSNodeConfig;
import edu.umass.cs.gns.nsdesign.nodeconfig.NodeId;
import java.net.InetSocketAddress;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * A SelectRequestPacket is like a DNS packet without a GUID, but with a key and value.
 * The semantics is that we want to look up all the records that have a field named key with the given value.
 * We also use this to do automatic group GUID maintenence.
 *
 * @author westy
 */
public class SelectRequestPacket extends BasicPacketWithNSAndLNS {

  public enum SelectOperation {

    EQUALS, // special case query for field with value
    NEAR, // special case query for location field near point
    WITHIN, // special case query for location field within bounding box
    QUERY, // general purpose query
  }

  public enum GroupBehavior {

    NONE, // normal query, just returns results
    GROUP_SETUP, // set up a group guid that satisfies general purpose query
    GROUP_LOOKUP; // lookup value of group guid with an associated query
  }
  //
  private final static String ID = "id";
  private final static String KEY = "key";
  private final static String VALUE = "value";
  private final static String OTHERVALUE = "otherValue";
  private final static String QUERY = "query";
  private final static String LNSQUERYID = "lnsQueryId";
  //private final static String NSID = "nsid";
  private final static String NSQUERYID = "nsQueryId";
  private final static String SELECT_OPERATION = "operation";
  private final static String GROUP_BEHAVIOR = "group";
  private final static String GUID = "guid";
  private final static String REFRESH = "refresh";
  //
  private int id;
  private String key;
  private Object value;
  private Object otherValue;
  private String query;
  private int lnsQueryId = -1; // used by the local name server to maintain state
  //private NodeId<String> nsID; // the name server handling this request (if this is -1 the packet hasn't made it to the NS yet)
  private int nsQueryId = -1; // used by the name server to maintain state
  private SelectOperation selectOperation;
  private GroupBehavior groupBehavior;
  // for group guid
  private String guid; // the group GUID we are maintaning or null for simple select
  private int minRefreshInterval; // minimum time between allowed refreshes of the guid

  /**
   * Constructs a new QueryResponsePacket
   *
   * @param id
   * @param lnsAddress
   * @param selectOperation
   * @param key
   * @param groupBehavior
   * @param value
   * @param otherValue
   */
  public SelectRequestPacket(int id, InetSocketAddress lnsAddress, SelectOperation selectOperation, GroupBehavior groupBehavior, String key, Object value, Object otherValue) {
    super(GNSNodeConfig.INVALID_NAME_SERVER_ID, lnsAddress);
    this.type = Packet.PacketType.SELECT_REQUEST;
    this.id = id;
    this.key = key;
    this.value = value;
    this.otherValue = otherValue;
    //this.nsID = GNSNodeConfig.INVALID_NAME_SERVER_ID;
    this.selectOperation = selectOperation;
    this.groupBehavior = groupBehavior;
    this.query = null;
    this.guid = null;
  }

  /**
   * Helper to construct a SelectRequestPacket for a context aware group guid.
   *
   * @param id
   * @param lns
   * @param selectOperation
   * @param groupOperation
   * @param query
   * @param guid
   * @param minRefreshInterval
   */
  private SelectRequestPacket(int id, InetSocketAddress lnsAddress, SelectOperation selectOperation, GroupBehavior groupOperation, String query, String guid, int minRefreshInterval) {
    super(GNSNodeConfig.INVALID_NAME_SERVER_ID, lnsAddress);
    this.type = Packet.PacketType.SELECT_REQUEST;
    this.id = id;
    this.query = query;
    //this.nsID = GNSNodeConfig.INVALID_NAME_SERVER_ID;
    this.selectOperation = selectOperation;
    this.groupBehavior = groupOperation;
    this.key = null;
    this.value = null;
    this.otherValue = null;
    this.guid = guid;
    this.minRefreshInterval = minRefreshInterval;
  }

  /**
   * Creates a request to search all name servers for GUIDs that match the given query.
   *
   * @param id
   * @param lnsAddress
   * @param query
   * @return
   */
  public static SelectRequestPacket MakeQueryRequest(int id, InetSocketAddress lnsAddress, String query) {
    return new SelectRequestPacket(id, lnsAddress, SelectOperation.QUERY, GroupBehavior.NONE, query, null, -1);
  }

  /**
   * Just like a MakeQueryRequest except we're creating a new group guid to maintain results.
   * Creates a request to search all name servers for GUIDs that match the given query.
   *
   * @param id
   * @param lnsAddress
   * @param query
   * @param guid
   * @param refreshInterval
   * @return
   */
  public static SelectRequestPacket MakeGroupSetupRequest(int id, InetSocketAddress lnsAddress, String query, String guid, int refreshInterval) {
    return new SelectRequestPacket(id, lnsAddress, SelectOperation.QUERY, GroupBehavior.GROUP_SETUP, query, guid, refreshInterval);
  }

  /**
   * Just like a MakeQueryRequest except we're potentially updating the group guid to maintain results.
   * Creates a request to search all name servers for GUIDs that match the given query.
   *
   * @param id
   * @param lnsAddress
   * @param guid
   * @return
   */
  public static SelectRequestPacket MakeGroupLookupRequest(int id, InetSocketAddress lnsAddress, String guid) {
    return new SelectRequestPacket(id, lnsAddress, SelectOperation.QUERY, GroupBehavior.GROUP_LOOKUP, null, guid, -1);
  }

  /**
   * Constructs new SelectRequestPacket from a JSONObject
   *
   * @param json JSONObject representing this packet
   * @throws org.json.JSONException
   */
  public SelectRequestPacket(JSONObject json) throws JSONException {
    super(new NodeId<String>(json.getString(NAMESERVER_ID)),
            json.optString(LNS_ADDRESS, null), json.optInt(LNS_PORT, INVALID_PORT));
    if (Packet.getPacketType(json) != Packet.PacketType.SELECT_REQUEST) {
      throw new JSONException("SelectRequestPacket: wrong packet type " + Packet.getPacketType(json));
    }
    this.type = Packet.getPacketType(json);
    this.id = json.getInt(ID);
    this.key = json.has(KEY) ? json.getString(KEY) : null;
    this.value = json.optString(VALUE, null);
    this.otherValue = json.optString(OTHERVALUE, null);
    this.query = json.optString(QUERY, null);
    this.lnsQueryId = json.getInt(LNSQUERYID);
    //this.nsID = new NodeId<String>(json.getString(NSID));
    this.nsQueryId = json.getInt(NSQUERYID);
    this.selectOperation = SelectOperation.valueOf(json.getString(SELECT_OPERATION));
    this.groupBehavior = GroupBehavior.valueOf(json.getString(GROUP_BEHAVIOR));
    this.guid = json.optString(GUID, null);
    this.minRefreshInterval = json.optInt(REFRESH, -1);
  }

  /**
   * Converts a SelectRequestPacket to a JSONObject.
   *
   * @return JSONObject
   * @throws org.json.JSONException
   */
  @Override
  public JSONObject toJSONObject() throws JSONException {
    JSONObject json = new JSONObject();
    addToJSONObject(json);
    return json;
  }

  @Override
  public void addToJSONObject(JSONObject json) throws JSONException {
    Packet.putPacketType(json, getType());
    super.addToJSONObject(json);
    json.put(ID, id);
    if (key != null) {
      json.put(KEY, key);
    }
    if (value != null) {
      json.put(VALUE, value);
    }
    if (otherValue != null) {
      json.put(OTHERVALUE, otherValue);
    }
    if (query != null) {
      json.put(QUERY, query);
    }
    json.put(LNSQUERYID, lnsQueryId);
    //json.put(NSID, nsID.toString());
    json.put(NSQUERYID, nsQueryId);
    json.put(SELECT_OPERATION, selectOperation.name());
    json.put(GROUP_BEHAVIOR, groupBehavior.name());
    if (guid != null) {
      json.put(GUID, guid);
    }
    if (minRefreshInterval != -1) {
      json.put(REFRESH, minRefreshInterval);
    }
  }

  public void setLnsQueryId(int lnsQueryId) {
    this.lnsQueryId = lnsQueryId;
  }

  public void setNsQueryId(int nsQueryId) {
    this.nsQueryId = nsQueryId;
  }

//  public void setNsID(NodeId<String> nsID) {
//    this.nsID = nsID;
//  }

  public int getId() {
    return id;
  }

  public String getKey() {
    return key;
  }

  public Object getValue() {
    return value;
  }

//  public int getLnsID() {
//    return lnsID;
//  }
  public int getLnsQueryId() {
    return lnsQueryId;
  }

//  public NodeId<String> getNameServerID() {
//    return nsID;
//  }

  public int getNsQueryId() {
    return nsQueryId;
  }

  public SelectOperation getSelectOperation() {
    return selectOperation;
  }

  public GroupBehavior getGroupBehavior() {
    return groupBehavior;
  }

  public Object getOtherValue() {
    return otherValue;
  }

  public String getQuery() {
    return query;
  }

  public void setQuery(String query) {
    this.query = query;
  }

  public String getGuid() {
    return guid;
  }

  public int getMinRefreshInterval() {
    return minRefreshInterval;
  }

}
