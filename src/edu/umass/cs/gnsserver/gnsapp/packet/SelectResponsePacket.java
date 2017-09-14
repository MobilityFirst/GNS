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
import edu.umass.cs.gnscommon.ResponseCode;
import edu.umass.cs.gnscommon.packets.commandreply.NotificationStatsToIssuer;

import java.net.InetSocketAddress;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * This class implements a packet that contains a response
 * to a select statement.
 *
 * @author Westy
 */
public class SelectResponsePacket extends BasicPacketWithReturnAddressAndNsAddress implements ClientRequest 
{
	//
	private final static String ID 						= "id";
	private final static String RECORDS 				= "records";
	private final static String NOTIFICATION_STATS		= "notificationStats";
	private final static String NSQUERYID 				= "nsQueryId";
	private final static String RESPONSECODE 			= "code";
	private final static String ERRORSTRING 			= "error";
	
	private long requestId;
	private int nsQueryId;
	private JSONArray records;
	// Only used in SelectNotify command.
	private NotificationStatsToIssuer notificationStats;
  
	private ResponseCode responseCode;
	private String errorMessage;

  /*
   * Constructs a new SelectResponsePacket
   *
   * @param id
   * @param jsonObject
   */
  private SelectResponsePacket(long id, InetSocketAddress clientAddress, int nsQueryId,
          InetSocketAddress nsAddress, JSONArray records, 
          NotificationStatsToIssuer notificationStats, ResponseCode responseCode,
          String errorMessage) 
  {
	  super(nsAddress, clientAddress);
	  this.type = Packet.PacketType.SELECT_RESPONSE;
	  this.requestId = id;
	  this.nsQueryId = nsQueryId;
	  this.records = records;
	  this.notificationStats = notificationStats;
	  this.responseCode = responseCode;
	  this.errorMessage = errorMessage;
  }

  /**
   * Used by a NameServer to a send response with full records back to the collecting NameServer
   *
   * @param id
   * @param lnsAddress
   * @param lnsQueryId
   * @param nsQueryId
   * @param nsAddress
   * @param records
   * @return a SelectResponsePacket
   */
  public static SelectResponsePacket makeSuccessPacketForFullRecords(
          long id, InetSocketAddress lnsAddress,
          int nsQueryId, InetSocketAddress nsAddress, JSONArray records) 
  {
	  return new SelectResponsePacket(id, lnsAddress, nsQueryId, nsAddress, records,
			  null, ResponseCode.NO_ERROR, null);
  }
  
  /**
   * Used by a NameServer to a send response with notification stats back to 
   * an entry-point name server. 
   *
   * @param id
   * @param lnsAddress
   * @param nsQueryId
   * @param nsAddress
   * @param notificationStats
   * @return a SelectResponsePacket
   */
  public static SelectResponsePacket makeSuccessPacketForNotificationStatsOnly
  			(long id, InetSocketAddress lnsAddress,
  					int nsQueryId, InetSocketAddress nsAddress, 
  					NotificationStatsToIssuer notificationStats) 
  {
	  return new SelectResponsePacket(id, lnsAddress, nsQueryId, nsAddress,
            null, notificationStats, ResponseCode.NO_ERROR, null);
  }
  

  /**
   * Used by a NameServer to a failure response to a NameServer or Local NameServer
   *
   * @param id
   * @param lnsAddress
   * @param nsQueryId
   * @param nsAddress
   * @param errorMessage
   * @return a SelectResponsePacket
   */
  public static SelectResponsePacket makeFailPacket(long id, InetSocketAddress lnsAddress,
           int nsQueryId, InetSocketAddress nsAddress, String errorMessage) {
    return new SelectResponsePacket(id, lnsAddress, nsQueryId, nsAddress,
            null, null, ResponseCode.UNSPECIFIED_ERROR, errorMessage);
  }
  
  /**
   * Constructs new SelectResponsePacket from a JSONObject
   *
   * @param json JSONObject representing this packet
   * @throws org.json.JSONException
   */
  public SelectResponsePacket(JSONObject json) throws JSONException {
    super(json);
    if (Packet.getPacketType(json) != Packet.PacketType.SELECT_RESPONSE) {
      throw new JSONException("StatusPacket: wrong packet type " + Packet.getPacketType(json));
    }
    this.type = Packet.getPacketType(json);
    this.requestId = json.getLong(ID);
    //this.lnsID = json.getInt(LNSID);
    this.nsQueryId = json.getInt(NSQUERYID);
    //this.nameServer = new NodeIDType(json.getString(NAMESERVER));
    this.responseCode = ResponseCode.valueOf(json.getString(RESPONSECODE));
    // either of these could be null
    this.records = json.optJSONArray(RECORDS);
    if(json.has(NOTIFICATION_STATS))
    {
    	this.notificationStats = NotificationStatsToIssuer.fromJSON
    			(json.optJSONObject(NOTIFICATION_STATS));
    }
    this.errorMessage = json.optString(ERRORSTRING, null);

  }

  /**
   * Converts a SelectResponsePacket to a JSONObject.
   *
   * @return JSONObject representing this packet.
   * @throws org.json.JSONException
   */
  @Override
  public JSONObject toJSONObject() throws JSONException {
    JSONObject json = new JSONObject();
    Packet.putPacketType(json, getType());
    super.addToJSONObject(json);
    json.put(ID, requestId);
    //json.put(LNSID, lnsID);
    json.put(NSQUERYID, nsQueryId);
    //json.put(NAMESERVER, nameServer.toString());
    json.put(RESPONSECODE, responseCode.name());
    if (records != null) 
    {
    	json.put(RECORDS, records);
    }
    if(this.notificationStats != null)
    {
    	json.put(NOTIFICATION_STATS, notificationStats.toJSONObject());
    }
    if (errorMessage != null) {
    	json.put(ERRORSTRING, errorMessage);
    }
    return json;
  }

  /**
   * Return the requestId.
   *
   * @return the requestId
   */
//  public long getId() {
//    return requestId;
//  }

  /**
   * Return the records.
   *
   * @return the records
   */
  public JSONArray getRecords() {
    return records;
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
   * Return the response code.
   *
   * @return the response code
   */
  public ResponseCode getResponseCode() {
    return responseCode;
  }

  /**
   * Return the error message.
   *
   * @return the error message
   */
  public String getErrorMessage() {
    return errorMessage;
  }

  /**
   *
   * @return the service name
   */
  @Override
  public String getServiceName() {
	  // aditya: all select response packets should have this name. 
	  // All select responses should have same name because edu.umass.cs.reconfiguration.ActiveReplica
	  // stores a demand profile for each distinct name, so we tag all select responses with 
	  // the same  name so that they have only one name and edu.umass.cs.reconfiguration.ActiveReplica
	  // stores only one demand profile. 
	  
    return "SelectResponse";
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
   * @return the id
   */
  @Override
  public long getRequestID() {
    return requestId;
  }
  
  public NotificationStatsToIssuer getNotificationStats()
  {
	  return this.notificationStats;
  }
}