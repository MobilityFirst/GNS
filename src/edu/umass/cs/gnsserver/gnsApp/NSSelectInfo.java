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
package edu.umass.cs.gnsserver.gnsApp;

import edu.umass.cs.gnsserver.gnsApp.packet.SelectRequestPacket.SelectOperation;
import edu.umass.cs.gnsserver.gnsApp.packet.SelectRequestPacket.GroupBehavior;
import org.json.JSONObject;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class represents a data structure to store information
 * about Select operations performed on the GNS.
 * @param <NodeIDType>
 */
public class NSSelectInfo<NodeIDType> {
  private final int id;
  private final Set<NodeIDType> serversToBeProcessed; // the list of servers that have yet to be processed
  private final ConcurrentHashMap<String, JSONObject> responses;
  private final SelectOperation selectOperation;
  private final GroupBehavior groupBehavior;
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
  public NSSelectInfo(int id, Set<NodeIDType> serverIds, SelectOperation selectOperation, GroupBehavior groupBehavior, String query, int minRefreshInterval, String guid) {
    this.id = id;
    this.serversToBeProcessed = Collections.newSetFromMap(new ConcurrentHashMap<NodeIDType, Boolean>());
    this.serversToBeProcessed.addAll(serverIds);
    this.responses = new ConcurrentHashMap<String, JSONObject>(10, 0.75f, 3);
    this.selectOperation = selectOperation;
    this.groupBehavior = groupBehavior;
    this.query = query;
    this.guid = guid;
    this.minRefreshInterval = minRefreshInterval;
  }

  /**
   * 
   * @return 
   */
  public int getId() {
    return id;
  }

  /**
   * Removes the server if from the list of servers that have yet to be processed.
   * @param id 
   */
  public void removeServerID(NodeIDType id) {
    serversToBeProcessed.remove(id);
  }
  /**
   * 
   * @return 
   */
  public Set<NodeIDType> serversYetToRespond() {
    return serversToBeProcessed;
  }
  /**
   * Returns true if all the names servers have responded.
   * 
   * @return 
   */
  public boolean allServersResponded() {
    return serversToBeProcessed.isEmpty();
  }

  /**
   * Adds the result of a query for a particular guid if the guid has not been seen yet.
   * 
   * @param name
   * @param json
   * @return 
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
    return new HashSet<JSONObject>(responses.values());
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
   * @return a {@link GroupBehavior}
   */
  public GroupBehavior getGroupBehavior() {
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
