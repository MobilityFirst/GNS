package edu.umass.cs.gns.nsdesign.activeReplica;
/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved
 */

import edu.umass.cs.gns.database.BasicRecordCursor;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nameserver.NSSelectInfo;
import edu.umass.cs.gns.nameserver.recordmap.NameRecord;
import edu.umass.cs.gns.packet.SelectRequestPacket;
import edu.umass.cs.gns.packet.SelectResponsePacket;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 *
 * @author westy
 */
public class Select {

  private static Random randomID = new Random();
  private static ConcurrentMap<Integer, NSSelectInfo> queriesInProgress = new ConcurrentHashMap<Integer, NSSelectInfo>(10, 0.75f, 3);

  public static void handleSelectRequest(JSONObject incomingJSON, ActiveReplica ar) throws JSONException, UnknownHostException {
    SelectRequestPacket packet = new SelectRequestPacket(incomingJSON);
    if (packet.getNsQueryId() != -1) { // this is how we tell if it has been processed by the NS
      handleSelectRequestFromNS(incomingJSON, ar);
    } else {
      handleSelectRequestFromLNS(incomingJSON, ar);
    }
  }

  public static void handleSelectRequestFromLNS(JSONObject incomingJSON, ActiveReplica ar) throws JSONException, UnknownHostException {
    SelectRequestPacket packet = new SelectRequestPacket(incomingJSON);

    Set<Integer> serverIds = ar.getGNSNodeConfig().getAllNameServerIDs();
    // store the into for later
    int queryId = addQueryInfo(serverIds);
    packet.setNsID(ar.getNodeID());
    packet.setNsQueryId(queryId); // Note: this also tells handleSelectRequest that it should go to NS now
    JSONObject outgoingJSON = packet.toJSONObject();
    GNS.getLogger().fine("NS" + ar.getNodeID() + " sending select " + outgoingJSON + " to " + serverIds);
    try {
      ar.getNioServer().sendToIDs(serverIds, outgoingJSON); //, ar.getNodeID()
    } catch (IOException e) {
      GNS.getLogger().severe("Exception while sending select request: " + e);
    }
    // Abhigyan: There was a bug here in case there is just one name server. We were not checking whether all
    // name servers have responded in that case. Sending out the select request to all name servers (including myself)
    // fixed this problem.
  }

  public static void handleSelectRequestFromNS(JSONObject incomingJSON, ActiveReplica ar) throws JSONException {
    GNS.getLogger().fine("NS" + ar.getNodeID() + " recvd QueryRequest: " + incomingJSON);
    SelectRequestPacket request = new SelectRequestPacket(incomingJSON);
    try {
      // grab the records
      JSONArray jsonRecords = getJSONRecordsForSelect(request, ar);
      SelectResponsePacket response = SelectResponsePacket.makeSuccessPacket(request.getId(), request.getLnsID(),
              request.getLnsQueryId(), request.getNsQueryId(), ar.getNodeID(), jsonRecords);
      GNS.getLogger().fine("NS" + ar.getNodeID() + " sending back " + jsonRecords.length() + " records");
      // and send them back to the originating NS
      ar.getNioServer().sendToID(request.getNsID(), response.toJSONObject());
    } catch (Exception e) {
      GNS.getLogger().severe("Exception while handling select request: " + e);
      SelectResponsePacket failResponse = SelectResponsePacket.makeFailPacket(request.getId(), request.getLnsID(),
              request.getLnsQueryId(), request.getNsQueryId(), ar.getNodeID(), e.getMessage());
      try {
        ar.getNioServer().sendToID(request.getNsID(), failResponse.toJSONObject());
      } catch (IOException f) {
        GNS.getLogger().severe("Unable to send Failure SelectResponsePacket: " + f);
        return;
      }
    }
  }

  public static void handleSelectResponse(JSONObject json, ActiveReplica ar) throws JSONException {
    GNS.getLogger().fine("NS" + ar.getNodeID() + " recvd QueryResponse: " + json);
    SelectResponsePacket packet = new SelectResponsePacket(json);
    GNS.getLogger().fine("NS" + ar.getNodeID() + " recvd from NS" + packet.getNameServer());
    NSSelectInfo info = queriesInProgress.get(packet.getNsQueryId());
    GNS.getLogger().fine("NS" + ar.getNodeID() + " located query info:" + info.serversYetToRespond());
    // if there is no error update our results list
    if (SelectResponsePacket.ResponseCode.NOERROR.equals(packet.getResponseCode())) {
      // stuff all the unique records into the info structure
      processJSONRecords(packet.getJsonArray(), info, ar);
    } else { // error response
      GNS.getLogger().fine("NS" + ar.getNodeID() + " processing error response: " + packet.getErrorMessage());
      // SHOULD SEND BACK AN ERROR TO LNS HERE
    }
    GNS.getLogger().fine("NS" + ar.getNodeID() + " removing server " + packet.getNameServer());
    // Remove the NS ID from the list to keep track of who has responded
    info.removeServerID(packet.getNameServer());
    GNS.getLogger().fine("NS" + ar.getNodeID() + " servers yet to respond:" + info.serversYetToRespond());
    // If all the servers have sent us a response we're done.
    // Pull the records out of the info structure and send a response back to the LNS
    if (info.allServersResponded()) {
      SelectResponsePacket response = SelectResponsePacket.makeSuccessPacket(packet.getId(), -1, packet.getLnsQueryId(),
              -1, -1, new JSONArray(info.getResponses()));
      try {
        ar.getNioServer().sendToID(packet.getLnsID(), response.toJSONObject());
      } catch (IOException f) {
        GNS.getLogger().severe("Unable to send success SelectResponsePacket: " + f);
        return;
      }
      queriesInProgress.remove(packet.getNsQueryId());
    }
  }

  private static int addQueryInfo(Set<Integer> serverIds) {
    int id;
    do {
      id = randomID.nextInt();
    } while (queriesInProgress.containsKey(id));
    //Add query info
    NSSelectInfo info = new NSSelectInfo(id, serverIds);
    queriesInProgress.put(id, info);
    return id;
  }

  private static JSONArray getJSONRecordsForSelect(SelectRequestPacket request, ActiveReplica ar) {
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

  private static void processJSONRecords(JSONArray jsonArray, NSSelectInfo info, ActiveReplica ar) throws JSONException {
    int length = jsonArray.length();
    GNS.getLogger().fine("NS" + ar.getNodeID() + " processing " + length + " records");
    // org.json sucks... should have converted a long tine ago
    for (int i = 0; i < length; i++) {
      JSONObject record = jsonArray.getJSONObject(i);
      String name = record.getString(NameRecord.NAME.getName());
      if (info.addNewResponse(name, record)) {
        GNS.getLogger().fine("NS" + ar.getNodeID() + " added record for " + name);
      } else {
        GNS.getLogger().fine("NS" + ar.getNodeID() + " DID NOT ADD record for " + name);
      }
    }
  }
}
