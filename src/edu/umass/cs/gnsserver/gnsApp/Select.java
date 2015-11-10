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
/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved
 */

import edu.umass.cs.gnsserver.database.AbstractRecordCursor;
import edu.umass.cs.gnsserver.exceptions.FailedDBOperationException;
import edu.umass.cs.gnsserver.main.GNS;
import edu.umass.cs.gnsserver.gnsApp.clientSupport.NSGroupAccess;
import edu.umass.cs.gnsserver.gnsApp.recordmap.NameRecord;
import edu.umass.cs.gnsserver.gnsApp.packet.SelectRequestPacket;
import edu.umass.cs.gnsserver.gnsApp.packet.SelectResponsePacket;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import edu.umass.cs.gnsserver.gnsApp.packet.SelectRequestPacket.SelectOperation;
import edu.umass.cs.gnsserver.gnsApp.packet.SelectRequestPacket.GroupBehavior;
import edu.umass.cs.gnsserver.utils.ResultValue;
import edu.umass.cs.gnsserver.utils.Util;
import java.net.InetSocketAddress;
import java.util.Date;
import java.util.HashSet;

/**
 * This class handles select operations which have a similar semantics to an SQL SELECT.
 * The semantics is that we want to look up all the records with a given value or whose
 * value falls in a given range or that more generally match a query.
 *
 * The SelectRequestPacket is sent to some NS (determining which one is done by the
 * LNS). This NS handles the broadcast to all of the NSs and the collection of results.
 *
 * For all select operations the NS which receive the broadcasted select packet execute the
 * appropriate query to collect all the guids that satisfy it. They then send the full records
 * from all these queries back to the collecting NS. The collecting NS then extracts the GUIDS
 * from all the results removing duplicates and then sends back JUST THE GUIDs, not the full
 * records.
 *
 * Here's the special handling the NS does for guid GROUPs:
 *
 * On the request side when we receive a GROUP_SETUP request we do the regular broadcast thing.
 *
 * On the response side for a GROUP_SETUP we do the regular collate thing and return the results,
 * plus we set the value of the group guid and the values of last_refreshed_time.
 * We need a GROUP info structure to hold these things.
 *
 * On the request side when we receive a GROUP_LOOKUP request we need to
 * 1) Check to see if enough time has passed since the last update
 * (current time greater than last_refreshed_time + min_refresh_interval). If it has we
 * do the usual query broadcast.
 * If not enough time has elapsed we send back the response with the current value of the group guid.
 *
 * On the response when see a GROUP_LOOKUP it means that enough time has passed since the last update
 * (in the other case the response is sent back on request side of things).
 * We handle this exactly the same as we do GROUP_SETUP (set group, return results, time bookkeeping).
 *
 * @author westy
 */
public class Select {

  private static Random randomID = new Random();
  private static ConcurrentMap<Integer, NSSelectInfo<String>> queriesInProgress = new ConcurrentHashMap<Integer, NSSelectInfo<String>>(10, 0.75f, 3);

  /**
   * Handles a select request that was received from a client.
   * 
   * @param incomingJSON
   * @param replica
   * @throws JSONException
   * @throws UnknownHostException
   * @throws FailedDBOperationException
   */
  public static void handleSelectRequest(JSONObject incomingJSON, 
          GnsApplicationInterface<String> replica) throws JSONException, UnknownHostException, FailedDBOperationException {
    SelectRequestPacket<String> packet = new SelectRequestPacket<String>(incomingJSON, replica.getGNSNodeConfig());
    if (packet.getNsQueryId() != -1) { // this is how we tell if it has been processed by the NS
      handleSelectRequestFromNS(incomingJSON, replica);
    } else {
      handleSelectRequestFromCCP(incomingJSON, replica);
    }
  }

  /**
   * Handle a select request from an LNS.
   * This node is the broadcaster and selector.
   *
   * @param incomingJSON
   * @param app
   * @throws JSONException
   * @throws UnknownHostException
   * @throws FailedDBOperationException
   */
  private static void handleSelectRequestFromCCP(JSONObject incomingJSON, 
          GnsApplicationInterface<String> app) throws JSONException, UnknownHostException, FailedDBOperationException {
    SelectRequestPacket<String> packet = new SelectRequestPacket<String>(incomingJSON, app.getGNSNodeConfig());
    // special case handling of the GROUP_LOOK operation
    // If sufficient time hasn't passed we just send the current value back
    if (packet.getGroupBehavior().equals(GroupBehavior.GROUP_LOOKUP)) {
      // grab the timing parameters that we squirreled away from the SETUP
      Date lastUpdate = NSGroupAccess.getLastUpdate(packet.getGuid(), app, packet.getCppAddress());
      int minRefreshInterval = NSGroupAccess.getMinRefresh(packet.getGuid(), app, packet.getCppAddress());
      if (lastUpdate != null) {
        if (AppReconfigurableNodeOptions.debuggingEnabled) {
          GNS.getLogger().info("GROUP_LOOKUP Request: " + new Date().getTime() + " - " + lastUpdate.getTime() + " <= " + minRefreshInterval);
        }
        // if not enough time has passed we just return the current value of the group
        if (new Date().getTime() - lastUpdate.getTime() <= minRefreshInterval) {
          if (AppReconfigurableNodeOptions.debuggingEnabled) {
            GNS.getLogger().info("GROUP_LOOKUP Request: Time has not elapsed. Returning current group value for " + packet.getGuid());
          }
          ResultValue result = NSGroupAccess.lookupMembers(packet.getGuid(), true, app, packet.getCppAddress());
          sendReponsePacketToCCP(packet.getId(), packet.getCcpQueryId(), packet.getCppAddress(), result.toStringSet(), app);
          return;
        }
      } else {
        GNS.getLogger().info("GROUP_LOOKUP Request: No Last Update Info ");
      }
    }
    // the code below executes for regular selects and also for GROUP SETUP and GROUP LOOKUP but for lookup
    // only if enough time has elapsed since last lookup (see above)
    // OR in the anamolous situation where the update info could not be found
    if (AppReconfigurableNodeOptions.debuggingEnabled) {
      GNS.getLogger().info(packet.getSelectOperation().toString() + 
              " Request: Forwarding request for " + packet.getGuid() != null ? packet.getGuid() : "non-guid select");
    }
    // If it's not a group lookup or is but enough time has passed we do the usual thing
    // and send the request out to all the servers. We'll receive a response sent on the flipside.
    Set<String> serverIds = app.getGNSNodeConfig().getActiveReplicas();
    // store the info for later
    int queryId = addQueryInfo(serverIds, packet.getSelectOperation(), packet.getGroupBehavior(),
            packet.getQuery(), packet.getMinRefreshInterval(), packet.getGuid());
    if (packet.getGroupBehavior().equals(GroupBehavior.GROUP_LOOKUP)) {
      // the query string is supplied with a lookup so we stuff in it there. It was saved from the SETUP operation.
      packet.setQuery(NSGroupAccess.getQueryString(packet.getGuid(), app, packet.getCppAddress()));
    }
    packet.setNameServerID(app.getNodeID());
    packet.setNsQueryId(queryId); // Note: this also tells handleSelectRequest that it should go to NS now
    JSONObject outgoingJSON = packet.toJSONObject();
    if (AppReconfigurableNodeOptions.debuggingEnabled) {
      GNS.getLogger().info("NS" + app.getNodeID().toString() + " sending select " 
              + outgoingJSON + " to " + Util.setOfNodeIdToString(serverIds));
    }
    try {
      for (String serverId : serverIds) {
        app.sendToID(serverId, outgoingJSON); // send to myself too
      }
    } catch (IOException e) {
      GNS.getLogger().severe("Exception while sending select request: " + e);
    }
  }

  /**
   * Handle a select request from the collecting NS. This is what other NSs do when they
   * get a SelectRequestPacket from the NS that originally received the packet (the one that is collecting
   * all the records).
   * This NS looks up the records and returns them.
   *
   * @param incomingJSON
   * @param app
   * @throws JSONException
   */
  private static void handleSelectRequestFromNS(JSONObject incomingJSON, 
          GnsApplicationInterface<String> app) throws JSONException {
    if (AppReconfigurableNodeOptions.debuggingEnabled) {
      GNS.getLogger().info("NS " + app.getNodeID().toString() + " recvd QueryRequest: " + incomingJSON);
    }
    SelectRequestPacket<String> request = new SelectRequestPacket<String>(incomingJSON, app.getGNSNodeConfig());
    try {
      // grab the records
      JSONArray jsonRecords = getJSONRecordsForSelect(request, app);
      @SuppressWarnings("unchecked")
      SelectResponsePacket<String> response = SelectResponsePacket.makeSuccessPacketForRecordsOnly(request.getId(), request.getCppAddress(),
              request.getCcpQueryId(), request.getNsQueryId(), app.getNodeID(), jsonRecords);
      if (AppReconfigurableNodeOptions.debuggingEnabled) {
        GNS.getLogger().info("NS " + app.getNodeID().toString() + " sending back " 
                + jsonRecords.length() + " records");
      }
      // and send them back to the originating NS
      app.sendToID(request.getNameServerID(), response.toJSONObject());
    } catch (Exception e) {
      GNS.getLogger().severe("Exception while handling select request: " + e);
      SelectResponsePacket failResponse = SelectResponsePacket.makeFailPacket(request.getId(), request.getCppAddress(),
              request.getCcpQueryId(), request.getNsQueryId(), app.getNodeID(), e.getMessage());
      try {
        app.sendToID(request.getNameServerID(), failResponse.toJSONObject());
      } catch (IOException f) {
        GNS.getLogger().severe("Unable to send Failure SelectResponsePacket: " + f);
        return;
      }
    }
  }

  /**
   * Handles a select response.
   * This code runs in the collecting NS.
   *
   * @param json
   * @param replica
   * @throws JSONException
   */
  public static void handleSelectResponse(JSONObject json, 
          GnsApplicationInterface<String> replica) throws JSONException {
    SelectResponsePacket<String> packet = new SelectResponsePacket<>(json, replica.getGNSNodeConfig());
    if (AppReconfigurableNodeOptions.debuggingEnabled) {
      GNS.getLogger().fine("NS " + replica.getNodeID().toString() + " recvd from NS " + packet.getNameServerID().toString());
    }
    NSSelectInfo<String> info = queriesInProgress.get(packet.getNsQueryId());
    if (info == null) {
      GNS.getLogger().warning("NS " + replica.getNodeID().toString() + " unabled to located query info:" + packet.getNsQueryId());
      return;
    }
    // if there is no error update our results list
    if (SelectResponsePacket.ResponseCode.NOERROR.equals(packet.getResponseCode())) {
      // stuff all the unique records into the info structure
      processJSONRecords(packet.getRecords(), info, replica);
    } else { // error response
      if (AppReconfigurableNodeOptions.debuggingEnabled) {
        GNS.getLogger().fine("NS " + replica.getNodeID().toString() + " processing error response: " + packet.getErrorMessage());
      }
    }
    // Remove the NS ID from the list to keep track of who has responded
    info.removeServerID(packet.getNameServerID());
    if (AppReconfigurableNodeOptions.debuggingEnabled) {
      GNS.getLogger().fine("NS" + replica.getNodeID().toString() + " servers yet to respond:" + info.serversYetToRespond());
    }
    if (info.allServersResponded()) {
      handledAllServersResponded(packet, info, replica);
    }
  }

  private static void sendReponsePacketToCCP(int id, int lnsQueryId, InetSocketAddress address, Set<String> guids,
          GnsApplicationInterface<String> app) throws JSONException {
    @SuppressWarnings("unchecked")
    SelectResponsePacket<String> response = SelectResponsePacket.makeSuccessPacketForGuidsOnly(id, null, lnsQueryId,
            -1, null, new JSONArray(guids));
    //try {
      app.getClientCommandProcessor().injectPacketIntoCCPQueue(response.toJSONObject());
      //app.sendToClient(address, response.toJSONObject());
//    } catch (IOException f) {
//      GNS.getLogger().severe("Unable to send success SelectResponsePacket: " + f);
//    }
  }

  private static void handledAllServersResponded(SelectResponsePacket<String> packet, NSSelectInfo<String> info, 
          GnsApplicationInterface<String> replica) throws JSONException {
    // If all the servers have sent us a response we're done.
    Set<String> guids = extractGuidsFromRecords(info.getResponsesAsSet());
    // Pull the records out of the info structure and send a response back to the LNS
    sendReponsePacketToCCP(packet.getId(), packet.getLnsQueryId(), packet.getCppAddress(), guids, replica);
    // we're done processing this select query
    queriesInProgress.remove(packet.getNsQueryId());
    // Now we update any group guid stuff
    if (info.getGroupBehavior().equals(GroupBehavior.GROUP_SETUP)) {
      if (AppReconfigurableNodeOptions.debuggingEnabled) {
        GNS.getLogger().fine("NS" + replica.getNodeID().toString() + " storing query string and other info");
      }
      // for setup we need to squirrel away the query for later lookups
      NSGroupAccess.updateQueryString(info.getGuid(), info.getQuery(), replica, packet.getCppAddress());
      NSGroupAccess.updateMinRefresh(info.getGuid(), info.getMinRefreshInterval(), replica, packet.getCppAddress());
    }
    if (info.getGroupBehavior().equals(GroupBehavior.GROUP_SETUP) || info.getGroupBehavior().equals(GroupBehavior.GROUP_LOOKUP)) {
      String guid = info.getGuid();
      if (AppReconfigurableNodeOptions.debuggingEnabled) {
        GNS.getLogger().fine("NS" + replica.getNodeID().toString() + " updating group members");
      }
      NSGroupAccess.updateMembers(guid, guids, replica, packet.getCppAddress());
      //NSGroupAccess.updateRecords(guid, processResponsesIntoJSONArray(info.getResponsesAsMap()), replica); 
      NSGroupAccess.updateLastUpdate(guid, new Date(), replica, packet.getCppAddress());
    }
  }

  private static Set<String> extractGuidsFromRecords(Set<JSONObject> records) {
    Set<String> result = new HashSet<String>();
    for (JSONObject json : records) {
      try {
        result.add(json.getString(NameRecord.NAME.getName()));
      } catch (JSONException e) {
      }
    }
    return result;
  }

  private static int addQueryInfo(Set<String> serverIds, SelectOperation selectOperation, 
          GroupBehavior groupBehavior, String query, int minRefreshInterval, String guid) {
    int id;
    do {
      id = randomID.nextInt();
    } while (queriesInProgress.containsKey(id));
    //Add query info
    NSSelectInfo<String> info = new NSSelectInfo<>(id, serverIds, selectOperation, groupBehavior, query, minRefreshInterval, guid);
    queriesInProgress.put(id, info);
    return id;
  }

  private static JSONArray getJSONRecordsForSelect(SelectRequestPacket<String> request, 
          GnsApplicationInterface<String> ar) throws FailedDBOperationException {
    JSONArray jsonRecords = new JSONArray();
    // actually only need name and values map... fix this
    AbstractRecordCursor cursor = null;
    switch (request.getSelectOperation()) {
      case EQUALS:
        cursor = NameRecord.selectRecords(ar.getDB(), request.getKey(), request.getValue());
        break;
      case NEAR:
        if (request.getValue() instanceof String) {
          cursor = NameRecord.selectRecordsNear(ar.getDB(), request.getKey(), (String) request.getValue(),
                  Double.parseDouble((String) request.getOtherValue()));
        } else {
          break;
        }
        break;
      case WITHIN:
        if (request.getValue() instanceof String) {
          cursor = NameRecord.selectRecordsWithin(ar.getDB(), request.getKey(), (String) request.getValue());
        } else {
          break;
        }
        break;
      case QUERY:
        if (AppReconfigurableNodeOptions.debuggingEnabled) {
          GNS.getLogger().fine("NS" + ar.getNodeID().toString() + " query: " + request.getQuery());
        }
        cursor = NameRecord.selectRecordsQuery(ar.getDB(), request.getQuery());
        break;
      default:
        break;
    }
    // think about returning a cursor that has prefetched a limited (100 which is like mongo limit)
    // number of records in it and the ability to fetch more
    while (cursor.hasNext()) {
      jsonRecords.put(cursor.nextJSONObject());
    }
    return jsonRecords;
  }

  // takes the JSON records that are returned from an NS and stuffs the into the NSSelectInfo record
  private static void processJSONRecords(JSONArray jsonArray, NSSelectInfo<String> info, 
          GnsApplicationInterface<String> ar) throws JSONException {
    int length = jsonArray.length();
    if (AppReconfigurableNodeOptions.debuggingEnabled) {
      GNS.getLogger().fine("NS" + ar.getNodeID().toString() + " processing " + length + " records");
    }
    // org.json sucks... should have converted a long time ago
    for (int i = 0; i < length; i++) {
      JSONObject record = jsonArray.getJSONObject(i);
      String name = record.getString(NameRecord.NAME.getName());
      if (info.addResponseIfNotSeenYet(name, record)) {
        if (AppReconfigurableNodeOptions.debuggingEnabled) {
          GNS.getLogger().fine("NS" + ar.getNodeID().toString() + " added record for " + name);
        }
      } else {
        if (AppReconfigurableNodeOptions.debuggingEnabled) {
          GNS.getLogger().fine("NS" + ar.getNodeID().toString() + " DID NOT ADD record for " + name);
        }
      }
    }
  }
}
