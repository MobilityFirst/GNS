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
 *  Initial developer(s): Abhigyan Sharma, Westy
 *
 */
package edu.umass.cs.gnsserver.gnsApp.packet;

import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commandSupport.SHA1HashFunction;
import edu.umass.cs.gnscommon.utils.Base64;
import edu.umass.cs.nio.interfaces.Stringifiable;

import java.net.InetSocketAddress;

import org.json.JSONException;
import org.json.JSONObject;

/**
 * A SelectRequestPacket is like a DNS_SUBTYPE_QUERY packet without a GUID, but with a key and value.
 * The semantics is that we want to look up all the records that have a field named key with the given value.
 * We also use this to do automatic group GUID maintenence.
 *
 * @author westy
 * @param <NodeIDType>
 */
public class SelectRequestPacket<NodeIDType> extends BasicPacketWithNSAndCCP<NodeIDType> implements Request {

  /**
   * The select operation.
   */
  public enum SelectOperation {

    /**
     * Special case query for field with value.
     */
    EQUALS, 

    /**
     * Special case query for location field near point.
     */
    NEAR,

    /**
     * Special case query for location field within bounding box.
     */
    WITHIN,

    /**
     * General purpose query.
     */
    QUERY, 
  }

  /**
   * The group behavior.
   */
  public enum GroupBehavior {

    /**
     * Normal query, just returns results.
     */
    NONE, // 

    /**
     * Set up a group guid that satisfies general purpose query.
     */
    GROUP_SETUP, // 

    /**
     * Lookup value of group guid with an associated query.
     */
    GROUP_LOOKUP; // 
  }
  //
  private final static String ID = "id";
  private final static String KEY = "key";
  private final static String VALUE = "value";
  private final static String OTHERVALUE = "otherValue";
  private final static String QUERY = "query";
  private final static String CCPQUERYID = "ccpQueryId";
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
  private int ccpQueryId = -1; // used by the command processor to maintain state
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
   * @param ccpAddress
   * @param selectOperation
   * @param key
   * @param groupBehavior
   * @param value
   * @param otherValue
   */
  @SuppressWarnings("unchecked")
  public SelectRequestPacket(int id, InetSocketAddress ccpAddress, SelectOperation selectOperation, GroupBehavior groupBehavior, String key, Object value, Object otherValue) {
    super(null, ccpAddress);
    this.type = Packet.PacketType.SELECT_REQUEST;
    this.id = id;
    this.key = key;
    this.value = value;
    this.otherValue = otherValue;
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
  @SuppressWarnings("unchecked")
  private SelectRequestPacket(int id, InetSocketAddress ccpAddress, SelectOperation selectOperation, GroupBehavior groupOperation, String query, String guid, int minRefreshInterval) {
    super(null, ccpAddress);
    this.type = Packet.PacketType.SELECT_REQUEST;
    this.id = id;
    this.query = query;
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
   * @param ccpAddress
   * @param query
   * @return a SelectRequestPacket
   */
  public static SelectRequestPacket MakeQueryRequest(int id, InetSocketAddress ccpAddress, String query) {
    return new SelectRequestPacket(id, ccpAddress, SelectOperation.QUERY, GroupBehavior.NONE, query, null, -1);
  }

  /**
   * Just like a MakeQueryRequest except we're creating a new group guid to maintain results.
   * Creates a request to search all name servers for GUIDs that match the given query.
   *
   * @param id
   * @param ccpAddress
   * @param query
   * @param guid
   * @param refreshInterval
   * @return a SelectRequestPacket
   */
  public static SelectRequestPacket MakeGroupSetupRequest(int id, InetSocketAddress ccpAddress, String query, String guid, int refreshInterval) {
    return new SelectRequestPacket(id, ccpAddress, SelectOperation.QUERY, GroupBehavior.GROUP_SETUP, query, guid, refreshInterval);
  }

  /**
   * Just like a MakeQueryRequest except we're potentially updating the group guid to maintain results.
   * Creates a request to search all name servers for GUIDs that match the given query.
   *
   * @param id
   * @param ccpAddress
   * @param guid
   * @return a SelectRequestPacket
   */
  public static SelectRequestPacket MakeGroupLookupRequest(int id, InetSocketAddress ccpAddress, String guid) {
    return new SelectRequestPacket(id, ccpAddress, SelectOperation.QUERY, GroupBehavior.GROUP_LOOKUP, null, guid, -1);
  }

  /**
   * Constructs new SelectRequestPacket from a JSONObject
   *
   * @param json JSONObject representing this packet
   * @param unstringer
   * @throws org.json.JSONException
   */
  @SuppressWarnings("unchecked")
  public SelectRequestPacket(JSONObject json, Stringifiable<NodeIDType> unstringer) throws JSONException {
    super(json.has(NAMESERVER_ID) ? unstringer.valueOf(json.getString(NAMESERVER_ID)) : null,
            json.optString(CCP_ADDRESS, null), json.optInt(CCP_PORT, INVALID_PORT));
    if (Packet.getPacketType(json) != Packet.PacketType.SELECT_REQUEST) {
      throw new JSONException("SelectRequestPacket: wrong packet type " + Packet.getPacketType(json));
    }
    this.type = Packet.getPacketType(json);
    this.id = json.getInt(ID);
    this.key = json.has(KEY) ? json.getString(KEY) : null;
    this.value = json.optString(VALUE, null);
    this.otherValue = json.optString(OTHERVALUE, null);
    this.query = json.optString(QUERY, null);
    this.ccpQueryId = json.getInt(CCPQUERYID);
    //this.nsID = new NodeIDType(json.getString(NSID));
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
    json.put(CCPQUERYID, ccpQueryId);
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

  /**
   * Set the CCP Query ID.
   * 
   * @param ccpQueryId
   */
  public void setCCPQueryId(int ccpQueryId) {
    this.ccpQueryId = ccpQueryId;
  }

  /**
   * Set the NS Query ID.
   * 
   * @param nsQueryId
   */
  public void setNsQueryId(int nsQueryId) {
    this.nsQueryId = nsQueryId;
  }

  /**
   * Return the ID.
   * 
   * @return the ID
   */
  public int getId() {
    return id;
  }

  /**
   * Return the key.
   * 
   * @return the key
   */
  public String getKey() {
    return key;
  }

  /**
   * Return the value.
   * 
   * @return the value
   */
  public Object getValue() {
    return value;
  }

  /**
   * Return the CCP query id.
   * 
   * @return the CCP query id
   */
  public int getCcpQueryId() {
    return ccpQueryId;
  }

  /**
   * Return the NS query id.
   * 
   * @return the NS query id
   */
  public int getNsQueryId() {
    return nsQueryId;
  }

  /**
   * Return the select operation.
   * 
   * @return the select operation
   */
  public SelectOperation getSelectOperation() {
    return selectOperation;
  }

  /**
   * Return the group behavior.
   * 
   * @return the group behavior
   */
  public GroupBehavior getGroupBehavior() {
    return groupBehavior;
  }

  /**
   * Return the other value.
   * 
   * @return the other value
   */
  public Object getOtherValue() {
    return otherValue;
  }

  /**
   * Return the query.
   * 
   * @return the query
   */
  public String getQuery() {
    return query;
  }

  /**
   * Set the query.
   * 
   * @param query
   */
  public void setQuery(String query) {
    this.query = query;
  }

  @Override
  public String getServiceName() {
    if (query != null) {
      // FIXME: maybe cache this
      return Base64.encodeToString(SHA1HashFunction.getInstance().hash(this.query), false);
    } else {
      // FIXME:
      return "SelectRequest";
    }
  }

  /**
   * Return the guid.
   * 
   * @return the guid
   */
  public String getGuid() {
    return guid;
  }

  /**
   * Get the min refresh interval.
   * 
   * @return the min refresh interval
   */
  public int getMinRefreshInterval() {
    return minRefreshInterval;
  }

}
