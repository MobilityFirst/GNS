/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.nameserver;

import edu.umass.cs.gns.database.BasicRecordCursor;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.packet.SelectRequestPacket;
import edu.umass.cs.gns.packet.SelectResponsePacket;
import edu.umass.cs.gns.util.ConfigFileInfo;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 * @author westy
 */
public class Select {

  private static Random randomID = new Random();
  private static ConcurrentMap<Integer, NSSelectInfo> queriesInProgress = new ConcurrentHashMap<Integer, NSSelectInfo>(10, 0.75f, 3);

  public static void handleSelectRequest(JSONObject incomingJSON) throws JSONException, UnknownHostException {
    SelectRequestPacket packet = new SelectRequestPacket(incomingJSON);
    if (packet.getNsQueryId() != -1) { // this is how we tell if it has been processed by the NS
      handleSelectRequestFromNS(incomingJSON);
    } else {
      handleSelectRequestFromLNS(incomingJSON);
    }
  }

  public static void handleSelectRequestFromLNS(JSONObject incomingJSON) throws JSONException, UnknownHostException {
    SelectRequestPacket packet = new SelectRequestPacket(incomingJSON);

    Set<Integer> serverIds = ConfigFileInfo.getAllNameServerIDs();
    // store the into for later
    int queryId = addQueryInfo(serverIds);
    packet.setNsID(NameServer.nodeID);
    packet.setNsQueryId(queryId); // Note: this also tells handleSelectRequest that it should go to NS now
    JSONObject outgoingJSON = packet.toJSONObject();
    GNS.getLogger().fine("NS" + NameServer.nodeID + " sending select " + outgoingJSON + " to " + serverIds);
    // send to everybody except us (we'll do our stuff below)
    try {
      NameServer.tcpTransport.sendToIDs(serverIds, outgoingJSON, NameServer.nodeID);
    } catch (IOException e) {
      GNS.getLogger().severe("Exception while sending select request: " + e);
    }
    // Now add our responses
    NSSelectInfo info = queriesInProgress.get(packet.getNsQueryId());
    processJSONRecords(getJSONRecordsForSelect(packet), info);
    info.removeServerID(NameServer.nodeID);

  }

  public static void handleSelectRequestFromNS(JSONObject incomingJSON) throws JSONException {
    GNS.getLogger().fine("NS" + NameServer.nodeID + " recvd QueryRequest: " + incomingJSON);
    SelectRequestPacket request = new SelectRequestPacket(incomingJSON);
    try {
      // grab the records
      JSONArray jsonRecords = getJSONRecordsForSelect(request);
      SelectResponsePacket response = SelectResponsePacket.makeSuccessPacket(request.getId(), request.getLnsID(),
              request.getLnsQueryId(), request.getNsQueryId(), NameServer.nodeID, jsonRecords);
      GNS.getLogger().fine("NS" + NameServer.nodeID + " sending back " + jsonRecords.length() + " records");
      // and send them back to the originating NS
      NameServer.tcpTransport.sendToID(request.getNsID(), response.toJSONObject());
    } catch (Exception e) {
      GNS.getLogger().severe("Exception while handling select request: " + e);
      SelectResponsePacket failResponse = SelectResponsePacket.makeFailPacket(request.getId(), request.getLnsID(),
              request.getLnsQueryId(), request.getNsQueryId(), NameServer.nodeID, e.getMessage());
      try {
        NameServer.tcpTransport.sendToID(request.getNsID(), failResponse.toJSONObject());
      } catch (IOException f) {
        GNS.getLogger().severe("Unable to send Failure SelectResponsePacket: " + f);
        return;
      }
    }
  }

  public static void handleSelectResponse(JSONObject json) throws JSONException {
    GNS.getLogger().fine("NS" + NameServer.nodeID + " recvd QueryResponse: " + json);
    SelectResponsePacket packet = new SelectResponsePacket(json);
    GNS.getLogger().fine("NS" + NameServer.nodeID + " recvd from NS" + packet.getNameServer());
    NSSelectInfo info = queriesInProgress.get(packet.getNsQueryId());
    GNS.getLogger().fine("NS" + NameServer.nodeID + " located query info:" + info.serversYetToRespond());
    // if there is no error update our results list
    if (SelectResponsePacket.ResponseCode.NOERROR.equals(packet.getResponseCode())) {
      // stuff all the unique records into the info structure
      processJSONRecords(packet.getJsonArray(), info);
    } else { // error response
      GNS.getLogger().fine("NS" + NameServer.nodeID + " processing error response: " + packet.getErrorMessage());
      // SHOULD SEND BACK AN ERROR TO LNS HERE
    }
    GNS.getLogger().fine("NS" + NameServer.nodeID + " removing server " + packet.getNameServer());
    // Remove the NS ID from the list to keep track of who has responded
    info.removeServerID(packet.getNameServer());
    GNS.getLogger().fine("NS" + NameServer.nodeID + " servers yet to respond:" + info.serversYetToRespond());
    // If all the servers have sent us a response we're done.
    // Pull the records out of the info structure and send a response back to the LNS
    if (info.allServersResponded()) {
      SelectResponsePacket response = SelectResponsePacket.makeSuccessPacket(packet.getId(), -1, packet.getLnsQueryId(),
              -1, -1, new JSONArray(info.getResponses()));
      try {
        NameServer.tcpTransport.sendToID(packet.getLnsID(), response.toJSONObject());
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

  private static JSONArray getJSONRecordsForSelect(SelectRequestPacket request) {
    JSONArray jsonRecords = new JSONArray();
    // actually only need name and values map... fix this
    BasicRecordCursor cursor = null;
    switch (request.getOperation()) {
      case EQUALS:
        cursor = NameServer.selectRecords(request.getKey().getName(), request.getValue());
        break;
      case NEAR:
        if (request.getValue() instanceof String) {
          cursor = NameServer.selectRecordsNear(request.getKey().getName(), (String) request.getValue(),
                  Double.parseDouble((String) request.getOtherValue()));
        } else {
          break;
        }
        break;
      case WITHIN:
        if (request.getValue() instanceof String) {
          cursor = NameServer.selectRecordsWithin(request.getKey().getName(), (String) request.getValue());
        } else {
          break;
        }
        break;
      case QUERY:
        cursor = NameServer.selectRecordsQuery(request.getQuery());
        break;
      default:
        break;
    }
    while (cursor.hasNext()) {
      jsonRecords.put(cursor.next());
    }
    return jsonRecords;
  }

  private static void processJSONRecords(JSONArray jsonArray, NSSelectInfo info) throws JSONException {
    int length = jsonArray.length();
    GNS.getLogger().fine("NS" + NameServer.nodeID + " processing " + length + " records");
    // org.json sucks... should have converted a long tine ago
    for (int i = 0; i < length; i++) {
      JSONObject record = jsonArray.getJSONObject(i);
      String name = record.getString(NameRecord.NAME.getName());
      if (info.addNewResponse(name, record)) {
        GNS.getLogger().fine("NS" + NameServer.nodeID + " added record for " + name);
      } else {
        GNS.getLogger().fine("NS" + NameServer.nodeID + " DID NOT ADD record for " + name);
      }
    }
  }
}
