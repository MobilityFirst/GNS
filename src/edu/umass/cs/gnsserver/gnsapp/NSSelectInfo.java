
package edu.umass.cs.gnsserver.gnsapp;

import edu.umass.cs.gnsserver.gnsapp.packet.SelectGroupBehavior;
import edu.umass.cs.gnsserver.gnsapp.packet.SelectOperation;
import org.json.JSONObject;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;


public class NSSelectInfo<NodeIDType> {
  private final int id;
  private final Set<NodeIDType> serversToBeProcessed; // the list of servers that have yet to be processed
  private final ConcurrentHashMap<String, JSONObject> responses;
  private final SelectOperation selectOperation;
  private final SelectGroupBehavior groupBehavior;
  private final String guid; // the group GUID we are maintaining or null for simple select
  private final String query; // The string used to set up the query if applicable
  private final int minRefreshInterval; // in seconds

  public NSSelectInfo(int id, Set<NodeIDType> serverIds, SelectOperation selectOperation, SelectGroupBehavior groupBehavior, String query, int minRefreshInterval, String guid) {
    this.id = id;
    this.serversToBeProcessed = Collections.newSetFromMap(new ConcurrentHashMap<NodeIDType, Boolean>());
    this.serversToBeProcessed.addAll(serverIds);
    this.responses = new ConcurrentHashMap<>(10, 0.75f, 3);
    this.selectOperation = selectOperation;
    this.groupBehavior = groupBehavior;
    this.query = query;
    this.guid = guid;
    this.minRefreshInterval = minRefreshInterval;
  }


  public int getId() {
    return id;
  }


  public void removeServerID(NodeIDType id) {
    serversToBeProcessed.remove(id);
  }

  public Set<NodeIDType> serversYetToRespond() {
    return serversToBeProcessed;
  }

  public boolean allServersResponded() {
    return serversToBeProcessed.isEmpty();
  }


  public boolean addResponseIfNotSeenYet(String name, JSONObject json) {
    if (!responses.containsKey(name)) {
      responses.put(name, json);
      return true;
    } else {
      return false;
    }
  }


  public Set<JSONObject> getResponsesAsSet() {
    return new HashSet<>(responses.values());
  }


  public SelectOperation getSelectOperation() {
    return selectOperation;
  }


  public SelectGroupBehavior getGroupBehavior() {
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
