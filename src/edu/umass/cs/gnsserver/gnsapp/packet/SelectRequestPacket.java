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

import java.util.List;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gigapaxos.interfaces.ClientRequest;
import edu.umass.cs.gnscommon.packets.commandreply.LocalSelectHandleInfo;
import edu.umass.cs.gnscommon.packets.commandreply.SelectHandleInfo;
import edu.umass.cs.gnsserver.utils.JSONUtils;

/**
 * A SelectRequestPacket is like a DNS_SUBTYPE_QUERY packet without a GUID, but with a key and value.
 * The semantics is that we want to look up all the records that have a field named key with the given value.
 * We also use this to do automatic group GUID maintenance.
 * 
 * This packet also has a field to include a notification, which needs to be sent to 
 * all GUIDs that satisfy a query in a select request.
 *
 * @author westy, aditya
 */
public class SelectRequestPacket extends BasicPacketWithNSReturnAddress implements ClientRequest 
{
	private final static String KEY 							= "key";
	private final static String READER 							= "reader";
	private final static String VALUE 							= "value";
	private final static String OTHERVALUE 						= "otherValue";
	private final static String QUERY 							= "query";
	private final static String PROJECTION 						= "projection";
	private final static String NSQUERYID 						= "nsQueryId";
	private final static String SELECT_OPERATION 				= "operation";
	private final static String NOTIFICATION_STR 				= "notifcationMesg";
	private final static String SELECT_HANDLE 					= "selectHandle";
	private final static String LOCAL_SELECT_HANDLE 			= "localSelectHandle";
  
  
	private SelectOperation selectOperation;
  
	//
	private String reader;
	private String key;
	private Object value;
	private Object otherValue;
	private String query;
	private List<String> projection;
	private int nsQueryId = -1; // used by the name server to maintain state
  
  
	private String notificationStr = null;
	
	private LocalSelectHandleInfo localSelectHandle = null;
	// used for notification status select operation. 
	private SelectHandleInfo selectHandle = null;  
	
  /**
   * Constructs a new SelectRequestPacket
   *
   * @param id
   * @param selectOperation
   * @param key
   * @param reader
   * @param value
   * @param otherValue
   */
  public SelectRequestPacket(SelectOperation selectOperation,
          String reader, String key, Object value, Object otherValue) {
    super();
    this.type = Packet.PacketType.SELECT_REQUEST;
    this.reader = reader;
    this.key = key;
    this.value = value;
    this.otherValue = otherValue;
    this.selectOperation = selectOperation;
    this.query = null;
    this.projection = null;
  }

  /*
   * Helper to construct a SelectRequestPacket for a context aware group guid.
   *
   * @param id
   * @param lns
   * @param selectOperation
   * @param query
   * @param guid
   */
  private SelectRequestPacket(SelectOperation selectOperation,
          String reader, String query, List<String> projection, 
          String notifcationStr, SelectHandleInfo selectHandle, 
          LocalSelectHandleInfo localHandle)
  {
    super();
    this.type = Packet.PacketType.SELECT_REQUEST;
    this.reader = reader;
    this.query = query;
    this.projection = projection;
    this.selectOperation = selectOperation;
    this.key = null;
    this.value = null;
    this.otherValue = null;
    this.notificationStr = notifcationStr;
    this.selectHandle = selectHandle;
    this.localSelectHandle = localHandle;
  }

  /**
   * Creates a request to search all name servers for GUIDs that match the given query.
   *
   * @param id
   * @param reader
   * @param query
   * @param projection
   * @return a SelectRequestPacket
   */
  public static SelectRequestPacket makeQueryRequest( String reader, 
		  String query, List<String> projection) 
  {
	  return new SelectRequestPacket(SelectOperation.QUERY, 
    		reader, query, projection, null, null, null);
  }
  
  
  /**
   * Creates a request to search name servers to compute the GUIDs that satisfy 
   * the given query and send those the given  notification.
   *
   * @param id
   * @param reader
   * @param query
   * @param projection
   * @return a SelectRequestPacket
   */
  public static SelectRequestPacket makeSelectNotifyRequest(String reader, String query, 
		  			List<String> projection, String notificationStr) 
  {
	  return new SelectRequestPacket(SelectOperation.SELECT_NOTIFY,
			  reader, query, projection, notificationStr, null, null);
  }
  
  
  /**
   * Creates a SelectRequestPacket for a NOTIFICATION_STATUS select operation. 
   *
   * @param id
   * @param reader
   * @param selectHandle
   * 
   * @return a SelectRequestPacket
   */
  public static SelectRequestPacket makeSelectNotificationStatusRequest
  					(String reader, SelectHandleInfo selectHandle, 
  					LocalSelectHandleInfo localSelectHandle)
  {
	  return new SelectRequestPacket(SelectOperation.NOTIFICATION_STATUS,
			  reader, null, null, null, selectHandle, localSelectHandle);
  }
  
  /**
   * Constructs new SelectRequestPacket from a JSONObject
   *
   * @param json JSONObject representing this packet
   * @throws org.json.JSONException
   */
  public SelectRequestPacket(JSONObject json) throws JSONException 
  {
	  super(json);
	  if (Packet.getPacketType(json) != Packet.PacketType.SELECT_REQUEST) {
		  throw new JSONException("SelectRequestPacket: wrong packet type " + Packet.getPacketType(json));
	  }
	  this.type = Packet.getPacketType(json);
	  //this.requestId = json.getLong(ID);
	  this.selectOperation = SelectOperation.valueOf(json.getString(SELECT_OPERATION));
	  
	  this.reader = json.optString(READER, null);
	  this.key = json.optString(KEY, null);
	  this.value = json.optString(VALUE, null);
	  this.otherValue = json.optString(OTHERVALUE, null);
	  this.query = json.optString(QUERY, null);
	  if (json.has(PROJECTION)) 
	  {
		  this.projection = JSONUtils.JSONArrayToArrayListString(json.getJSONArray(PROJECTION));
	  } else 
	  {
		  this.projection = null;
	  }
	  this.nsQueryId = json.getInt(NSQUERYID);
	  
	  this.notificationStr = json.optString(NOTIFICATION_STR, null);
	  this.selectHandle = json.has(SELECT_HANDLE) ? 
			  	SelectHandleInfo.fromJSONArray(json.getJSONArray(SELECT_HANDLE)):null;
			  	
	  this.localSelectHandle = json.has(LOCAL_SELECT_HANDLE) ?
			  	LocalSelectHandleInfo.fromJSONObject(json.getJSONObject(LOCAL_SELECT_HANDLE)):null;
	
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
  public void addToJSONObject(JSONObject json) throws JSONException 
  {
	  Packet.putPacketType(json, getType());
	  super.addToJSONObject(json);
	  json.put(NSQUERYID, nsQueryId);
	  json.put(SELECT_OPERATION, selectOperation.name());
    
	  if (reader != null) 
	  {
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
	  if (projection != null) {
		  json.put(PROJECTION, projection);
	  }
    
	  if(this.notificationStr != null)
	  {
		  json.put(NOTIFICATION_STR, notificationStr);
	  }
	  
	  if(this.selectHandle != null)
	  {
		  json.put(SELECT_HANDLE, selectHandle.toJSONArray());
	  }
	  
	  if(this.localSelectHandle != null)
	  {
		  json.put(LOCAL_SELECT_HANDLE, this.localSelectHandle.toJSONObject());
	  }
  }

  /**
   * Set the NS Query ID.
   *
   * @param nsQueryId
   */
  public void setNsQueryId(int nsQueryId) 
  {
	  this.nsQueryId = nsQueryId;
  }
  

  /**
   * Return the reader.
   *
   * @return the reader
   */
  public String getReader() 
  {
	  return reader;
  }

  /**
   * Return the key.
   *
   * @return the key
   */
  public String getKey() 
  {
	  return key;
  }

  /**
   * Return the value.
   *
   * @return the value
   */
  public Object getValue() 
  {
	  return value;
  }

  /**
   * Return the NS query requestId.
   *
   * @return the NS query requestId
   */
  public int getNsQueryId() 
  {
	  return nsQueryId;
  }

  /**
   * Return the select operation.
   *
   * @return the select operation
   */
  public SelectOperation getSelectOperation() 
  {
	  return selectOperation;
  }

  /**
   * Return the other value.
   *
   * @return the other value
   */
  public Object getOtherValue() 
  {
	  return otherValue;
  }

  /**
   * Return the query.
   *
   * @return the query
   */
  public String getQuery() 
  {
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
   * @return the projection
   */
  public List<String> getProjection() {
    return projection;
  }

  /**
   *
   * @param projection
   */
  public void setProjection(List<String> projection) {
    this.projection = projection;
  }

  /**
   *
   * @return the service name
   */
  @Override
  public String getServiceName() {
      // aditya: all select request packets should have this name. 
 	  // All select requests should have same name because edu.umass.cs.reconfiguration.ActiveReplica
 	  // stores a demand profile for each distinct name, so we tag all select requests with 
 	  // the same  name so that they have only one name and edu.umass.cs.reconfiguration.ActiveReplica
 	  // stores only one demand profile. 
	  
	  return "SelectRequest";
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
//  @Override
//  public long getRequestID() {
//    return requestId;
//  }

  /**
   * Returns the notification string. 
   * @return
   */
  public String getNotificationString()
  {
	  return this.notificationStr;
  }
  
  /**
   * Returns the select handle.
   * @return
   */
  public SelectHandleInfo getSelectHandleInfo()
  {
	  return this.selectHandle;
  }
  
  /**
   * Returns a local select handle. 
   * @return
   */
  public LocalSelectHandleInfo getLocalSelectHandleInfo()
  {
	  return this.localSelectHandle;
  }
  
  
  /**
   *
   * @return the summary object
   */
  @Override
  public Object getSummary() 
  {
	  return new Object() 
	  {
		  @Override
		  public String toString() {
			  return getType() + ":"
                + getQuery() 
                + getProjection()
                + "[" + SelectRequestPacket.this.getClientAddress() + "]";
		  }
	  };
  }
  
  @Override
  public long getRequestID() 
  {
	  return -1;
  }
  
}