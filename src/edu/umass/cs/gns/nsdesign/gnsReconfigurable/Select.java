package edu.umass.cs.gns.nsdesign.gnsReconfigurable;
/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved
 */

import edu.umass.cs.gns.database.BasicRecordCursor;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nsdesign.clientsupport.NSGroupAccess;
import edu.umass.cs.gns.nsdesign.recordmap.NameRecord;
import edu.umass.cs.gns.nsdesign.packet.SelectRequestPacket;
import edu.umass.cs.gns.nsdesign.packet.SelectResponsePacket;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import edu.umass.cs.gns.nsdesign.packet.SelectRequestPacket.SelectOperation;
import edu.umass.cs.gns.nsdesign.packet.SelectRequestPacket.GroupBehavior;
import edu.umass.cs.gns.util.ResultValue;
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
 * On the request side when we get a GROUP_SETUP request we do the regular broadcast thing.
 *
 * On the response side for a GROUP_SETUP we do the regular collate thing and return the results,
 * plus we set the value of the group guid and the values of last_refreshed_time.
 * We need a GROUP info structure to hold these things.
 *
 * On the request side when we get a GROUP_LOOKUP request we need to
 * 1) Check to see if enough time has passed since the last update
 * (current time > last_refreshed_time + min_refresh_interval). If it has we
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
  private static ConcurrentMap<Integer, NSSelectInfo> queriesInProgress = new ConcurrentHashMap<Integer, NSSelectInfo>(10, 0.75f, 3);

  public static void handleSelectRequest(JSONObject incomingJSON, GnsReconfigurable replica) throws JSONException, UnknownHostException {
    SelectRequestPacket packet = new SelectRequestPacket(incomingJSON);
    if (packet.getNsQueryId() != -1) { // this is how we tell if it has been processed by the NS
      handleSelectRequestFromNS(incomingJSON, replica);
    } else {
      handleSelectRequestFromLNS(incomingJSON, replica);
    }
  }

  // handle a select request from an LNS
  // this node is the broadcaster and selector
  private static void handleSelectRequestFromLNS(JSONObject incomingJSON, GnsReconfigurable replica) throws JSONException, UnknownHostException {
    SelectRequestPacket packet = new SelectRequestPacket(incomingJSON);
    // special case handling of the GROUP_LOOK operation
    // If sufficient time hasn't passed we just send the current value back
    if (packet.getGroupBehavior().equals(GroupBehavior.GROUP_LOOKUP)) {
      // grab the timing parameters that we squirreled away from the SETUP
      Date lastUpdate = NSGroupAccess.getLastUpdate(packet.getGuid(), replica);
      int minRefreshInterval = NSGroupAccess.getMinRefresh(packet.getGuid(), replica);
      if (lastUpdate != null && minRefreshInterval != 0) {
        GNS.getLogger().info("GROUP_LOOKUP Request: " + new Date().getTime() + " - " + lastUpdate.getTime() + " <= " + minRefreshInterval);
        // if not enough time has passed we just return the current value of the group
        if (new Date().getTime() - lastUpdate.getTime() <= minRefreshInterval) {
          GNS.getLogger().info("GROUP_LOOKUP Request: Time has not elapsed. Returning current group value for " + packet.getGuid());
          ResultValue result = NSGroupAccess.lookupMembers(packet.getGuid(), true, replica);
          sendReponsePacketToLNS(packet.getId(), packet.getLnsQueryId(), packet.getLnsID(), result.toStringSet(), replica);
          return;
        }
      } else {
        GNS.getLogger().info("GROUP_LOOKUP Request: No Last Update Info ");
      }
    }
    // the code below executes for regualr selects and also for GROUP SETUP and GROUP LOOKUP but for lookup
    // only if enough time has elapsed since last lookup (see above)
    GNS.getLogger().info(packet.getSelectOperation().toString() + " Request: Forwarding request for " + packet.getGuid());
    // If it's not a group lookup or is but enough time has passed we do the usual thing
    // and send the request out to all the servers. We'll get a response sent  on the flipside.
    Set<Integer> serverIds = replica.getGNSNodeConfig().getAllNameServerIDs();
    // store the info for later
    int queryId = addQueryInfo(serverIds, packet.getSelectOperation(), packet.getGroupBehavior(),
            packet.getQuery(), packet.getMinRefreshInterval(), packet.getGuid());
    if (packet.getGroupBehavior().equals(GroupBehavior.GROUP_LOOKUP)) {
      // the query string is supplied with a lookup so we stuff in it there. It was saved from the SETUP operation.
      packet.setQuery(NSGroupAccess.getQueryString(packet.getGuid(), replica));
    }
    packet.setNsID(replica.getNodeID());
    packet.setNsQueryId(queryId); // Note: this also tells handleSelectRequest that it should go to NS now
    JSONObject outgoingJSON = packet.toJSONObject();
    GNS.getLogger().fine("NS" + replica.getNodeID() + " sending select " + outgoingJSON + " to " + serverIds);
    try {
      replica.getNioServer().sendToIDs(serverIds, outgoingJSON); // send to myself too
    } catch (IOException e) {
      GNS.getLogger().severe("Exception while sending select request: " + e);
    }
  }

  // handle a select request from the collecting NS
  // this node looks up the records and returns them
  private static void handleSelectRequestFromNS(JSONObject incomingJSON, GnsReconfigurable replica) throws JSONException {
    GNS.getLogger().fine("NS" + replica.getNodeID() + " recvd QueryRequest: " + incomingJSON);
    SelectRequestPacket request = new SelectRequestPacket(incomingJSON);
    try {
      // grab the records
      JSONArray jsonRecords = getJSONRecordsForSelect(request, replica);
      SelectResponsePacket response = SelectResponsePacket.makeSuccessPacketForRecordsOnly(request.getId(), request.getLnsID(),
              request.getLnsQueryId(), request.getNsQueryId(), replica.getNodeID(), jsonRecords);
      GNS.getLogger().fine("NS" + replica.getNodeID() + " sending back " + jsonRecords.length() + " records");
      // and send them back to the originating NS
      replica.getNioServer().sendToID(request.getNsID(), response.toJSONObject());
    } catch (Exception e) {
      GNS.getLogger().severe("Exception while handling select request: " + e);
      SelectResponsePacket failResponse = SelectResponsePacket.makeFailPacket(request.getId(), request.getLnsID(),
              request.getLnsQueryId(), request.getNsQueryId(), replica.getNodeID(), e.getMessage());
      try {
        replica.getNioServer().sendToID(request.getNsID(), failResponse.toJSONObject());
      } catch (IOException f) {
        GNS.getLogger().severe("Unable to send Failure SelectResponsePacket: " + f);
        return;
      }
    }
  }

  // this code runs in the collecing NS
  public static void handleSelectResponse(JSONObject json, GnsReconfigurable replica) throws JSONException {
    SelectResponsePacket packet = new SelectResponsePacket(json);
    GNS.getLogger().fine("NS" + replica.getNodeID() + " recvd from NS" + packet.getNameServer());
    NSSelectInfo info = queriesInProgress.get(packet.getNsQueryId());
    if (info == null) {
      GNS.getLogger().warning("NS" + replica.getNodeID() + " unabled to located query info:" + packet.getNsQueryId());
      return;
    }
    // if there is no error update our results list
    if (SelectResponsePacket.ResponseCode.NOERROR.equals(packet.getResponseCode())) {
      // stuff all the unique records into the info structure
      processJSONRecords(packet.getRecords(), info, replica);
    } else { // error response
      GNS.getLogger().fine("NS" + replica.getNodeID() + " processing error response: " + packet.getErrorMessage());
    }
    // Remove the NS ID from the list to keep track of who has responded
    info.removeServerID(packet.getNameServer());
    GNS.getLogger().fine("NS" + replica.getNodeID() + " servers yet to respond:" + info.serversYetToRespond());
    if (info.allServersResponded()) {
      handledAllServersResponded(packet, info, replica);
    }
  }

  private static void sendReponsePacketToLNS(int id, int lnsQueryId, int lnsId, Set<String> guids, GnsReconfigurable replica) throws JSONException {

    SelectResponsePacket response = SelectResponsePacket.makeSuccessPacketForGuidsOnly(id, -1, lnsQueryId,
            -1, -1, new JSONArray(guids));
    try {
      replica.getNioServer().sendToID(lnsId, response.toJSONObject());
    } catch (IOException f) {
      GNS.getLogger().severe("Unable to send success SelectResponsePacket: " + f);
    }
  }

  private static void handledAllServersResponded(SelectResponsePacket packet, NSSelectInfo info, GnsReconfigurable replica) throws JSONException {
    // If all the servers have sent us a response we're done.
    Set<String> guids = extractGuidsFromRecords(info.getResponsesAsSet());
    // Pull the records out of the info structure and send a response back to the LNS
    sendReponsePacketToLNS(packet.getId(), packet.getLnsQueryId(), packet.getLnsID(), guids, replica);
    // we're done processing this select query
    queriesInProgress.remove(packet.getNsQueryId());
    // Now we update any group guid stuff
    if (info.getGroupBehavior().equals(GroupBehavior.GROUP_SETUP)) {
      // for setup we need to squirrel away the query for later lookups
      NSGroupAccess.updateQueryString(info.getGuid(), info.getQuery(), replica);
      NSGroupAccess.updateMinRefresh(info.getGuid(), info.getMinRefreshInterval(), replica);
    }
    if (info.getGroupBehavior().equals(GroupBehavior.GROUP_SETUP) || info.getGroupBehavior().equals(GroupBehavior.GROUP_LOOKUP)) {
      String guid = info.getGuid();
      NSGroupAccess.updateMembers(guid, guids, replica);
      //NSGroupAccess.updateRecords(guid, processResponsesIntoJSONArray(info.getResponsesAsMap()), replica); 
      NSGroupAccess.updateLastUpdate(guid, new Date(), replica);
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

  private static int addQueryInfo(Set<Integer> serverIds, SelectOperation selectOperation, GroupBehavior groupBehavior, String query, int minRefreshInterval, String guid) {
    int id;
    do {
      id = randomID.nextInt();
    } while (queriesInProgress.containsKey(id));
    //Add query info
    NSSelectInfo info = new NSSelectInfo(id, serverIds, selectOperation, groupBehavior, query, minRefreshInterval, guid);
    queriesInProgress.put(id, info);
    return id;
  }

  private static JSONArray getJSONRecordsForSelect(SelectRequestPacket request, GnsReconfigurable ar) {
    JSONArray jsonRecords = new JSONArray();
    // actually only need name and values map... fix this
    BasicRecordCursor cursor = null;
    switch (request.getSelectOperation()) {
      case EQUALS:
        cursor = NameRecord.selectRecords(ar.getDB(), request.getKey().getName(), request.getValue());
        break;
      case NEAR:
        if (request.getValue() instanceof String) {
          cursor = NameRecord.selectRecordsNear(ar.getDB(), request.getKey().getName(), (String) request.getValue(),
                  Double.parseDouble((String) request.getOtherValue()));
        } else {
          break;
        }
        break;
      case WITHIN:
        if (request.getValue() instanceof String) {
          cursor = NameRecord.selectRecordsWithin(ar.getDB(), request.getKey().getName(), (String) request.getValue());
        } else {
          break;
        }
        break;
      case QUERY:
        GNS.getLogger().fine("NS" + ar.getNodeID() + " query: " + request.getQuery());
        cursor = NameRecord.selectRecordsQuery(ar.getDB(), request.getQuery());
        break;
      default:
        break;
    }
    // think about returning a cursor that has prefetched a limited (100 which is like mongo limit)
    // number of records in it and the ability to fetch more
    while (cursor.hasNext()) {
      jsonRecords.put(cursor.next());
    }
    return jsonRecords;
  }

  // takes the JSON records that are returned from each NS and 
  private static void processJSONRecords(JSONArray jsonArray, NSSelectInfo info, GnsReconfigurable ar) throws JSONException {
    int length = jsonArray.length();
    GNS.getLogger().fine("NS" + ar.getNodeID() + " processing " + length + " records");
    // org.json sucks... should have converted a long time ago
    for (int i = 0; i < length; i++) {
      JSONObject record = jsonArray.getJSONObject(i);
      String name = record.getString(NameRecord.NAME.getName());
      if (info.addResponseIfNotSeenYet(name, record)) {
        GNS.getLogger().fine("NS" + ar.getNodeID() + " added record for " + name);
      } else {
        GNS.getLogger().fine("NS" + ar.getNodeID() + " DID NOT ADD record for " + name);
      }
    }
  }
}
