/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.nameserver;

import edu.umass.cs.gns.database.BasicRecordCursor;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nameserver.recordmap.NameRecord;
import edu.umass.cs.gns.packet.SelectRequestPacket;
import edu.umass.cs.gns.packet.SelectResponsePacket;
import edu.umass.cs.gns.util.ConfigFileInfo;
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
 * This class handles select operations which have a similar semantics to an SQL SELECT.
 * The semantics is that we want to look up all the records with a given value or whose
 * value falls in a given range or that more generally match a query.
 * 
 * On the request side when we get a GROUP_SETUP request we do the regular broadcast thing.
 * Also need to handle GROUP_SETUP for a group that already exists.
 * 
 * On the response side for a GROUP_SETUP we do the regular collate thing plus we
 * set the value of the group guid and the values of last_refreshed_time and the min_refresh_interval. 
 * We need a GROUP info structure to hold these things.
 * 
 * On the request side when we get a GROUP_LOOKUP request we need to 
 * 1) Check to see if enough time has passed since the last update 
 * (current time > last_refreshed_time + min_refresh_interval). If it has we
 * do the usual query broadcast. 
 * If not we send back the response with the current value of the group guid
 * 
 * On the response when see a GROUP_LOOKUP it means that  enough time has passed since the last update 
 * (in the other case the response is sent back request side).
 * We handle this exactly the same as we do GROUP_SETUP.
 * 
 *
 * 
 *
 * @author westy
 */
public class Select {

  private static Random randomID = new Random();
  private static ConcurrentMap<Integer, NSSelectInfo> queriesInProgress = new ConcurrentHashMap<Integer, NSSelectInfo>(10, 0.75f, 3);

  /**
   * Handles a SelectRequestPacket coming from an LNS or NS.
   *
   * @param incomingJSON
   * @throws JSONException
   * @throws UnknownHostException
   */
  public static void handleSelectRequest(JSONObject incomingJSON) throws JSONException, UnknownHostException {
    SelectRequestPacket packet = new SelectRequestPacket(incomingJSON);
    if (packet.getNsQueryId() != -1) { // this is how we tell if it has been processed by the NS
      // if it's from an NS that means we're going to lookup some records in our
      // database and send them back to the NS that send this to us
      handleSelectRequestFromNS(incomingJSON);
    } else {
      // if it's from an LNS we're going to handle the sending of queries and merging back together
      handleSelectRequestFromLNS(incomingJSON);
    }
  }

  private static void handleSelectRequestFromLNS(JSONObject incomingJSON) throws JSONException, UnknownHostException {
    SelectRequestPacket packet = new SelectRequestPacket(incomingJSON);
    switch (packet.getOperation()) {
      case EQUALS:
      case NEAR:
      case QUERY:
      case WITHIN:
        handleSelectRequestQueryFromLNS(packet);
        break;
      case GROUP_SETUP:
        GNS.getLogger().severe("Not yet implemented: " + packet.getOperation());
        break;
      case GROUP_LOOKUP:
        GNS.getLogger().severe("Not yet implemented: " + packet.getOperation());
        break;
      default:
        GNS.getLogger().severe("Unknown SelectRequestPacket operation: " + packet.getOperation());
        break;
    }
  }

  /**
   * Handles a SelectRequestPacket that has a simple query coming from an LNS. This NS is going to broadcast the packet to
   * a set of servers (currently all) and collate the results (also adding it's own results)
   * before sending them back to the LNS.
   *
   * @param incomingJSON
   * @throws JSONException
   * @throws UnknownHostException
   */
  private static void handleSelectRequestQueryFromLNS(SelectRequestPacket packet) throws JSONException, UnknownHostException {
    Set<Integer> serverIds = ConfigFileInfo.getAllNameServerIDs();
    // store the into for later
    int queryId = addQueryInfo(serverIds);
    packet.setNsID(NameServer.getNodeID());
    packet.setNsQueryId(queryId); // Note: this also tells handleSelectRequest that it should go to NS now
    JSONObject outgoingJSON = packet.toJSONObject();
    GNS.getLogger().fine("NS" + NameServer.getNodeID() + " sending select " + outgoingJSON + " to " + serverIds);
    try {
      // send it to everyone including ourself
      NameServer.getTcpTransport().sendToIDs(serverIds, outgoingJSON);
    } catch (IOException e) {
      GNS.getLogger().severe("Exception while sending select request: " + e);
    }
  }

  /**
   * Handles a SelectRequestPacket coming from another NS by collecting the
   * appropriate records from the database and sending them back to the collecting NS.
   *
   * @param incomingJSON
   * @throws JSONException
   */
  private static void handleSelectRequestFromNS(JSONObject incomingJSON) throws JSONException {
    GNS.getLogger().fine("NS" + NameServer.getNodeID() + " recvd QueryRequest: " + incomingJSON);
    SelectRequestPacket request = new SelectRequestPacket(incomingJSON);
    try {
      // grab the records
      JSONArray jsonRecords = getJSONRecordsForSelect(request);
      SelectResponsePacket response = SelectResponsePacket.makeSuccessPacket(request.getId(), request.getLnsID(),
              request.getLnsQueryId(), request.getNsQueryId(), NameServer.getNodeID(), jsonRecords);
      GNS.getLogger().fine("NS" + NameServer.getNodeID() + " sending back " + jsonRecords.length() + " records");
      // and send them back to the originating NS
      NameServer.getTcpTransport().sendToID(request.getNsID(), response.toJSONObject());
    } catch (Exception e) {
      GNS.getLogger().severe("Exception while handling select request: " + e);
      SelectResponsePacket failResponse = SelectResponsePacket.makeFailPacket(request.getId(), request.getLnsID(),
              request.getLnsQueryId(), request.getNsQueryId(), NameServer.getNodeID(), e.getMessage());
      try {
        NameServer.getTcpTransport().sendToID(request.getNsID(), failResponse.toJSONObject());
      } catch (IOException f) {
        GNS.getLogger().severe("Unable to send Failure SelectResponsePacket: " + f);
        return;
      }
    }
  }

  /**
   * Handles a SelectResponsePacket coming back from a NameServer.
   * This method is going to collect all the responses
   *
   * @param json
   * @throws JSONException
   */
  public static void handleSelectResponse(JSONObject json) throws JSONException {
    SelectResponsePacket packet = new SelectResponsePacket(json);
    GNS.getLogger().fine("NS" + NameServer.getNodeID() + " recvd from NS" + packet.getNameServer());
    NSSelectInfo info = queriesInProgress.get(packet.getNsQueryId());
    GNS.getLogger().fine("NS" + NameServer.getNodeID() + " located query info:" + info.serversYetToRespond());
    // if there is no error update our results list
    if (SelectResponsePacket.ResponseCode.NOERROR.equals(packet.getResponseCode())) {
      // stuff all the unique records into the info structure
      processJSONRecords(packet.getJsonArray(), info);
    } else { // error response
      GNS.getLogger().fine("NS" + NameServer.getNodeID() + " processing error response: " + packet.getErrorMessage());
      // SHOULD SEND BACK AN ERROR TO LNS HERE
    }
    GNS.getLogger().fine("NS" + NameServer.getNodeID() + " removing server " + packet.getNameServer());
    // Remove the NS ID from the list to keep track of who has responded
    info.removeServerID(packet.getNameServer());
    GNS.getLogger().fine("NS" + NameServer.getNodeID() + " servers yet to respond:" + info.serversYetToRespond());
    // If all the servers have sent us a response we're done.
    // Pull the records out of the info structure and send a response back to the LNS
    if (info.allServersResponded()) {
      SelectResponsePacket response = SelectResponsePacket.makeSuccessPacket(packet.getId(), -1, packet.getLnsQueryId(),
              -1, -1, new JSONArray(info.getResponses()));
      try {
        NameServer.getTcpTransport().sendToID(packet.getLnsID(), response.toJSONObject());
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
        cursor = NameRecord.selectRecords(NameServer.getRecordMap(), request.getKey().getName(), request.getValue());
        break;
      case NEAR:
        if (request.getValue() instanceof String) {
          cursor = NameRecord.selectRecordsNear(NameServer.getRecordMap(), request.getKey().getName(), (String) request.getValue(),
                  Double.parseDouble((String) request.getOtherValue()));
        } else {
          break;
        }
        break;
      case WITHIN:
        if (request.getValue() instanceof String) {
          cursor = NameRecord.selectRecordsWithin(NameServer.getRecordMap(), request.getKey().getName(), (String) request.getValue());
        } else {
          break;
        }
        break;
      case QUERY:
        cursor = NameRecord.selectRecordsQuery(NameServer.getRecordMap(), request.getQuery());
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
    GNS.getLogger().fine("NS" + NameServer.getNodeID() + " processing " + length + " records");
    // org.json sucks... should have converted a long tine ago
    for (int i = 0; i < length; i++) {
      JSONObject record = jsonArray.getJSONObject(i);
      String name = record.getString(NameRecord.NAME.getName());
      if (info.addNewResponse(name, record)) {
        GNS.getLogger().fine("NS" + NameServer.getNodeID() + " added record for " + name);
      } else {
        GNS.getLogger().fine("NS" + NameServer.getNodeID() + " DID NOT ADD record for " + name);
      }
    }
  }
}
