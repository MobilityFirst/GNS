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
package edu.umass.cs.gnsserver.gnsapp;

import edu.umass.cs.gnscommon.packets.commandreply.NotificationStatsToIssuer;
import edu.umass.cs.gnsserver.gnsapp.packet.SelectOperation;
import edu.umass.cs.gnsserver.gnsapp.packet.SelectRequestPacket;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import org.json.JSONObject;


import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class represents a data structure to store information
 * about Select operations performed on the GNS.
 */
public class NSSelectInfo {

  private final int queryId;
  
  //Select request packet that was created after receiving a select COMMAND from a client.
  private final SelectRequestPacket selectPacket;
  
  //the list of servers that have yet to be processed
  private final Set<InetSocketAddress> allServers;

  private final Set<InetSocketAddress> serversToBeProcessed; // the list of servers that have yet to be processed
  private final ConcurrentHashMap<String, JSONObject> recordResponses;
  //  private final SelectGroupBehavior groupBehavior;
  private final List<NotificationStatsToIssuer> notificationStatusList;
  

  //private final int minRefreshInterval; // in seconds

  /**
   * NSSelectInfo constructor.
   * 
   * @param id
   * @param serverIds
   * @param selectPacket
   */
  public NSSelectInfo(int id, Set<InetSocketAddress> serverIds, SelectRequestPacket selectPacket)
  {
	  this.queryId = id;
  	  this.selectPacket = selectPacket;
  		
  	  this.serversToBeProcessed = new HashSet<InetSocketAddress>();
  	  this.serversToBeProcessed.addAll(serverIds);
      
  	  this.allServers = new HashSet<InetSocketAddress>();
  	  this.allServers.addAll(serverIds);
      
  	  this.recordResponses = new ConcurrentHashMap<String, JSONObject>();
  	  this.notificationStatusList = new LinkedList<NotificationStatsToIssuer>();
  }
  
  	/**
  	 * 
  	 * @return the queryId
  	 */
  public int getId() 
  {
	  return queryId;
  }
  	
  	/**
  	 * Removes the server if from the list of servers that have yet to be processed.
  	 * 
  	 * @param address
  	 */
  	public void removeServerAddress(InetSocketAddress address) {
  		serversToBeProcessed.remove(address);
  	}
  	
  	/**
  	 * 
  	 * @return the set of servers
  	 */
  	public Set<InetSocketAddress> serversYetToRespond() {
  		return serversToBeProcessed;
  	}

  /**
   * Returns true if all the names servers have responded.
   *
   * @return true if all the names servers have responded
   */
  public boolean allServersResponded() {
    return serversToBeProcessed.isEmpty();
  }

  /**
   * Adds the result of a query for a particular guid if the guid has not been seen yet.
   *
   * @param name
   * @param json
   * @return true if the response was not seen yet, false otherwise
   */
  public boolean addRecordResponseIfNotSeenYet(String name, JSONObject json) {
	  if (!recordResponses.containsKey(name)) {
		  recordResponses.put(name, json);
		  return true;
    } else {
      return false;
    }
  }
  
  public void addNotificationStat(NotificationStatsToIssuer notificationStats)
  {
	  synchronized(notificationStatusList)
  	  {
  		  if(notificationStats != null)
  		  {
  			  notificationStatusList.add(notificationStats);
  		  }
  	  }
  }
  
  public List<NotificationStatsToIssuer> getAllNotificationStats()
  {
	  return this.notificationStatusList;
  }

  /**
   * Returns that responses that have been see for this query.
   *
   * @return a set of JSONObjects
   */
  public Set<JSONObject> getResponsesAsSet() {
    return new HashSet<>(recordResponses.values());
  }

  /**
   * Returns that responses that have been see for this query.
   *
   * @return a set of JSONObjects
   */
  public List<JSONObject> getResponsesAsList() {
    return new ArrayList<>(recordResponses.values());
  }

  /**
   * Return the operation.
   *
   * @return a {@link SelectOperation}
   */
  public SelectOperation getSelectOperation() {
    return this.selectPacket.getSelectOperation();
  }

  /**
   * Return the query.
   *
   * @return a string
   */
  public String getQuery() {
    return this.selectPacket.getQuery();
  }

  /**
   * Return the projection which is a list of fields.
   * Null means that select should return a list of guids instead
   * of records (old-style).
   * 
   * @return the projection
   */
  public List<String> getProjection() {
    return this.selectPacket.getProjection();
  }
  
  /**
   * Returns all name servers to which this select request was sent to.
   * @return
   */
  public Set<InetSocketAddress> getAllServers()
  {
	  return this.allServers;
  }
  
  public SelectRequestPacket getSelectRequestPacket()
  {
	  return this.selectPacket;
  }
}