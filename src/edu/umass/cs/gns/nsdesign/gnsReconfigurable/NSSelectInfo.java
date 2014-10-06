package edu.umass.cs.gns.nsdesign.gnsReconfigurable;

import edu.umass.cs.gns.nsdesign.packet.SelectRequestPacket.SelectOperation;
import edu.umass.cs.gns.nsdesign.packet.SelectRequestPacket.GroupBehavior;
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
   * @return 
   */
  public Set<JSONObject> getResponsesAsSet() {
    return new HashSet<JSONObject>(responses.values());
  }

  public Map<String, JSONObject> getResponsesAsMap() {
    return new HashMap<String, JSONObject>(responses);
  }

  public SelectOperation getSelectOperation() {
    return selectOperation;
  }

  public GroupBehavior getGroupBehavior() {
    return groupBehavior;
  }
  
  public String getQuery() {
    return query;
  }
  
  public String getGuid() {
    return guid;
  }

  public int getMinRefreshInterval() {
    return minRefreshInterval;
  }
  
}
