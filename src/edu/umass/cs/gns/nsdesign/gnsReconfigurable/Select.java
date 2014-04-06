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
 * Here's what the handling NS does:
 *
 * On the request side when we get a GROUP_SETUP request we do the regular broadcast thing.
 * Also need to handle GROUP_SETUP for a group that already exists.
 *
 * On the response side for a GROUP_SETUP we do the regular collate thing and return the results,
 * plus we set the value of the group guid and the values of last_refreshed_time and the min_refresh_interval.
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

  public static void handleSelectRequestFromLNS(JSONObject incomingJSON, GnsReconfigurable replica) throws JSONException, UnknownHostException {
    SelectRequestPacket packet = new SelectRequestPacket(incomingJSON);

    Set<Integer> serverIds = replica.getGNSNodeConfig().getAllNameServerIDs();
    // store the into for later
    int queryId = addQueryInfo(serverIds, packet.getOperation(), packet.getGuid());
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

  public static void handleSelectRequestFromNS(JSONObject incomingJSON, GnsReconfigurable replica) throws JSONException {
    GNS.getLogger().info("NS" + replica.getNodeID() + " recvd QueryRequest: " + incomingJSON);
    SelectRequestPacket request = new SelectRequestPacket(incomingJSON);
    try {
      // grab the records
      JSONArray jsonRecords = getJSONRecordsForSelect(request, replica);
      SelectResponsePacket response = SelectResponsePacket.makeSuccessPacket(request.getId(), request.getLnsID(),
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

  public static void handleSelectResponse(JSONObject json, GnsReconfigurable replica) throws JSONException {
    SelectResponsePacket packet = new SelectResponsePacket(json);
    GNS.getLogger().info("NS" + replica.getNodeID() + " recvd from NS" + packet.getNameServer());
    NSSelectInfo info = queriesInProgress.get(packet.getNsQueryId());
    if (info == null) {
      GNS.getLogger().warning("NS" + replica.getNodeID() + " unabled to located query info:" + packet.getNsQueryId());
      return;
    }
    // if there is no error update our results list
    if (SelectResponsePacket.ResponseCode.NOERROR.equals(packet.getResponseCode())) {
      // stuff all the unique records into the info structure
      processJSONRecords(packet.getJsonArray(), info, replica);
    } else { // error response
      GNS.getLogger().fine("NS" + replica.getNodeID() + " processing error response: " + packet.getErrorMessage());
    }
    // Remove the NS ID from the list to keep track of who has responded
    info.removeServerID(packet.getNameServer());
    GNS.getLogger().info("NS" + replica.getNodeID() + " servers yet to respond:" + info.serversYetToRespond());
    if (info.allServersResponded()) {
      handledAllServersResponded(packet, info, replica);
    }
  }

  private static void handledAllServersResponded(SelectResponsePacket packet, NSSelectInfo info, GnsReconfigurable replica) throws JSONException {
    // If all the servers have sent us a response we're done.
    // Pull the records out of the info structure and send a response back to the LNS
    SelectResponsePacket response = SelectResponsePacket.makeSuccessPacket(packet.getId(), -1, packet.getLnsQueryId(),
            -1, -1, new JSONArray(info.getResponses()));
    try {
      replica.getNioServer().sendToID(packet.getLnsID(), response.toJSONObject());
    } catch (IOException f) {
      GNS.getLogger().severe("Unable to send success SelectResponsePacket: " + f);
      return;
    }
    // we're done processing this select query
    queriesInProgress.remove(packet.getNsQueryId());
    // Now we update any group guid stuff
    if (info.getOperation().equals(SelectOperation.GROUP_SETUP) || info.getOperation().equals(SelectOperation.GROUP_LOOKUP)) {
      String guid = info.getGuid();
      // since we can't decide we do both
      NSGroupAccess.updateMembers(guid, extractGuidsFromRecords(info.getResponses()), replica);
      // FIGURE OUT HOW TO DO THIS CORRECTLY
      //NSGroupAccess.updateRecords(guid, info.getResponses(), replica); 
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

  private static int addQueryInfo(Set<Integer> serverIds, SelectOperation operation, String guid) {
    int id;
    do {
      id = randomID.nextInt();
    } while (queriesInProgress.containsKey(id));
    //Add query info
    NSSelectInfo info = new NSSelectInfo(id, serverIds, operation, guid);
    queriesInProgress.put(id, info);
    return id;
  }

  private static JSONArray getJSONRecordsForSelect(SelectRequestPacket request, GnsReconfigurable ar) {
    JSONArray jsonRecords = new JSONArray();
    // actually only need name and values map... fix this
    BasicRecordCursor cursor = null;
    switch (request.getOperation()) {
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
      case GROUP_SETUP: // just like a query except we're creating a new group guid to maintain results
      case GROUP_LOOKUP: // just like a query except we're potentially updating the group guid to maintain results
        cursor = NameRecord.selectRecordsQuery(ar.getDB(), request.getQuery());
        break;
      default:
        break;
    }
    while (cursor.hasNext()) {
      jsonRecords.put(cursor.next());
    }
    return jsonRecords;
  }

  // takes the JSON records that are returned from each NS and 
  private static void processJSONRecords(JSONArray jsonArray, NSSelectInfo info, GnsReconfigurable ar) throws JSONException {
    int length = jsonArray.length();
    GNS.getLogger().fine("NS" + ar.getNodeID() + " processing " + length + " records");
    // org.json sucks... should have converted a long tine ago
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
