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
package edu.umass.cs.gnsserver.gnsapp.packet;

import edu.umass.cs.gigapaxos.interfaces.ClientRequest;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.ShaOneHashFunction;
import edu.umass.cs.gnscommon.utils.Base64;

import org.json.JSONException;
import org.json.JSONObject;

/**
 * A SelectRequestPacket is like a DNS_SUBTYPE_QUERY packet without a GUID, but with a key and value.
 * The semantics is that we want to look up all the records that have a field named key with the given value.
 * We also use this to do automatic group GUID maintenence.
 *
 * @author westy
 */
@SuppressWarnings("deprecation")
public class SelectRequestPacket extends BasicPacketWithNSReturnAddress
        implements ClientRequest {

  private final static String ID = "id";
  private final static String KEY = "key";
  private final static String READER = "reader";
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
  private long requestId;
  private String reader;
  private String key;
  private Object value;
  private Object otherValue;
  private String query;
  private int ccpQueryId = -1; // used by the command processor to maintain state
  private int nsQueryId = -1; // used by the name server to maintain state
  private SelectOperation selectOperation;
  private SelectGroupBehavior groupBehavior;
  // for group guid
  private String guid; // the group GUID we are maintaning or null for simple select
  private int minRefreshInterval; // minimum time between allowed refreshes of the guid

  /**
   * Constructs a new SelectRequestPacket
   *
   * @param id
   * @param selectOperation
   * @param key
   * @param reader
   * @param groupBehavior
   * @param value
   * @param otherValue
   */
  public SelectRequestPacket(long id, SelectOperation selectOperation, 
          SelectGroupBehavior groupBehavior,
          String reader, String key, Object value, Object otherValue) {
    super();
    this.type = Packet.PacketType.SELECT_REQUEST;
    this.requestId = id;
    this.reader = reader;
    this.key = key;
    this.value = value;
    this.otherValue = otherValue;
    this.selectOperation = selectOperation;
    this.groupBehavior = groupBehavior;
    this.query = null;
    this.guid = null;
  }

  /*
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
  private SelectRequestPacket(long id, SelectOperation selectOperation, 
          SelectGroupBehavior groupOperation,
          String reader, String query, String guid, int minRefreshInterval) {
    super();
    this.type = Packet.PacketType.SELECT_REQUEST;
    this.requestId = id;
    this.reader = reader;
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
   * @param reader
   * @param query
   * @return a SelectRequestPacket
   */
  public static SelectRequestPacket MakeQueryRequest(long id, String reader, String query) {
    return new SelectRequestPacket(id, SelectOperation.QUERY, 
             SelectGroupBehavior.NONE, reader, query, null, -1);
  }

  /**
   * Just like a MakeQueryRequest except we're creating a new group guid to maintain results.
   * Creates a request to search all name servers for GUIDs that match the given query.
   *
   * @param id
   * @param reader
   * @param query
   * @param guid
   * @param refreshInterval
   * @return a SelectRequestPacket
   */
  public static SelectRequestPacket MakeGroupSetupRequest(long id, String reader, String query,
          String guid,
          int refreshInterval) {
    return new SelectRequestPacket(id, SelectOperation.QUERY, SelectGroupBehavior.GROUP_SETUP,
            reader, query, guid, refreshInterval);
  }

  /**
   * Just like a MakeQueryRequest except we're potentially updating the group guid to maintain results.
   * Creates a request to search all name servers for GUIDs that match the given query.
   *
   * @param id
   * @param reader
   * @param guid
   * @return a SelectRequestPacket
   */
  public static SelectRequestPacket MakeGroupLookupRequest(long id, String reader, String guid) {
    return new SelectRequestPacket(id, SelectOperation.QUERY, SelectGroupBehavior.GROUP_LOOKUP, 
            reader, null, guid, -1);
  }

  /**
   * Constructs new SelectRequestPacket from a JSONObject
   *
   * @param json JSONObject representing this packet
   * @throws org.json.JSONException
   */
  public SelectRequestPacket(JSONObject json) throws JSONException {
    super(json);
    if (Packet.getPacketType(json) != Packet.PacketType.SELECT_REQUEST) {
      throw new JSONException("SelectRequestPacket: wrong packet type " + Packet.getPacketType(json));
    }
    this.type = Packet.getPacketType(json);
    this.requestId = json.getLong(ID);
    this.reader = json.has(READER) ? json.getString(READER) : null;
    this.key = json.has(KEY) ? json.getString(KEY) : null;
    this.value = json.optString(VALUE, null);
    this.otherValue = json.optString(OTHERVALUE, null);
    this.query = json.optString(QUERY, null);
    this.ccpQueryId = json.getInt(CCPQUERYID);
    //this.nsID = new NodeIDType(json.getString(NSID));
    this.nsQueryId = json.getInt(NSQUERYID);
    this.selectOperation = SelectOperation.valueOf(json.getString(SELECT_OPERATION));
    this.groupBehavior = SelectGroupBehavior.valueOf(json.getString(GROUP_BEHAVIOR));
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
    json.put(ID, requestId);
    if (reader != null) {
      json.put(READER, reader);
    }
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
  public long getId() {
    return requestId;
  }

  /**
   * Sets the request id.
   *
   * @param requestId
   */
  public void setRequestId(long requestId) {
    this.requestId = requestId;
  }

  /**
   * Return the reader.
   * 
   * @return  the reader
   */
  public String getReader() {
    return reader;
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
   * Return the CCP query requestId.
   *
   * @return the CCP query requestId
   */
  public int getCcpQueryId() {
    return ccpQueryId;
  }

  /**
   * Return the NS query requestId.
   *
   * @return the NS query requestId
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
  public SelectGroupBehavior getGroupBehavior() {
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

  /**
   *
   * @return the service name
   */
  @Override
  public String getServiceName() {
    if (query != null) {
      // FIXME: maybe cache this
      return Base64.encodeToString(ShaOneHashFunction.getInstance().hash(this.query), false);
    } else {
      // FIXME:
      return "_SelectRequest_";
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

  /**
   *
   * @return the response
   */
  @Override
  public ClientRequest getResponse() {
    return this.response;
  }

  /**
   *
   * @return the request id
   */
  @Override
  public long getRequestID() {
    return requestId;
  }

  /**
   *
   * @return the summary object
   */
  @Override
  public Object getSummary() {
    return new Object() {
      @Override
      public String toString() {
        return SelectRequestPacket.this.getType() + ":"
                + SelectRequestPacket.this.requestId + ":"
                + SelectRequestPacket.this.getQuery() + "[" + SelectRequestPacket.this.getClientAddress() + "]";
      }
    };
  }
}
