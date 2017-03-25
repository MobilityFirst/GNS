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

import edu.umass.cs.gnsserver.gnsapp.packet.SelectGroupBehavior;
import edu.umass.cs.gnsserver.gnsapp.packet.SelectOperation;

import java.net.InetSocketAddress;
import org.json.JSONObject;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class represents a data structure to store information
 * about Select operations performed on the GNS.
 */
public class NSSelectInfo {
  private final int queryId;
  private final Set<InetSocketAddress> serversToBeProcessed; // the list of servers that have yet to be processed
  private final ConcurrentHashMap<String, JSONObject> responses;
  private final SelectOperation selectOperation;
  private final SelectGroupBehavior groupBehavior;
  private final String guid; // the group GUID we are maintaining or null for simple select
  private final String query; // The string used to set up the query if applicable
  private final int minRefreshInterval; // in seconds
  /**
   * 
   * @param id
   * @param serverIds 
   * @param selectOperation 
   * @param groupBehavior 
   * @param query 
   * @param minRefreshInterval 
   * @param guid 
   */
  public NSSelectInfo(int id, Set<InetSocketAddress> serverIds, SelectOperation selectOperation, SelectGroupBehavior groupBehavior, String query, int minRefreshInterval, String guid) {
    this.queryId = id;
    this.serversToBeProcessed = Collections.newSetFromMap(new ConcurrentHashMap<InetSocketAddress, Boolean>());
    this.serversToBeProcessed.addAll(serverIds);
    this.responses = new ConcurrentHashMap<>(10, 0.75f, 3);
    this.selectOperation = selectOperation;
    this.groupBehavior = groupBehavior;
    this.query = query;
    this.guid = guid;
    this.minRefreshInterval = minRefreshInterval;
  }

  /**
   * 
   * @return the queryId
   */
  public int getId() {
    return queryId;
  }

  /**
   * Removes the server if from the list of servers that have yet to be processed.
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
  public boolean addResponseIfNotSeenYet(String name, JSONObject json) {
    if (!responses.containsKey(name)) {
      responses.put(name, json);
      return true;
    } else {
      return false;
    }
  }

  /**
   * Returns that responses that have been see for this query.
   * 
   * @return a set of JSONObjects
   */
  public Set<JSONObject> getResponsesAsSet() {
    return new HashSet<>(responses.values());
  }

  /**
   * Return the operation.
   * 
   * @return a {@link SelectOperation}
   */
  public SelectOperation getSelectOperation() {
    return selectOperation;
  }

  /**
   * Return the behavior.
   * 
   * @return a GroupBehavior
   */
  public SelectGroupBehavior getGroupBehavior() {
    return groupBehavior;
  }
  
  /**
   * Return the query.
   * 
   * @return a string
   */
  public String getQuery() {
    return query;
  }
  
  /**
   * Return the guid.
   * 
   * @return a string
   */
  public String getGuid() {
    return guid;
  }

  /**
   * Return the minimum refresh interval.
   * 
   * @return an int
   */
  public int getMinRefreshInterval() {
    return minRefreshInterval;
  }
  
}
