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

/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved
 */
import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnsserver.database.AbstractRecordCursor;
import edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;

import edu.umass.cs.gnsserver.gnsapp.clientSupport.NSGroupAccess;
import edu.umass.cs.gnsserver.gnsapp.deprecated.GNSApplicationInterface;
import edu.umass.cs.gnsserver.gnsapp.packet.SelectGroupBehavior;
import edu.umass.cs.gnsserver.gnsapp.packet.SelectOperation;
import edu.umass.cs.gnsserver.gnsapp.packet.SelectRequestPacket;
import edu.umass.cs.gnsserver.gnsapp.packet.SelectResponsePacket;
import edu.umass.cs.gnsserver.gnsapp.recordmap.NameRecord;
import edu.umass.cs.gnsserver.main.GNSConfig;
import edu.umass.cs.gnsserver.utils.ResultValue;
import edu.umass.cs.utils.Config;
import edu.umass.cs.utils.Util;

import java.net.InetSocketAddress;
import java.util.Date;
import java.util.HashSet;
import java.util.logging.Logger;

/**
 * This class handles select operations which have a similar semantics to an SQL SELECT.
 * The base architecture of the select methods is that we broadcast a query 
 * to all the servers and collate the responses.
 * The most general purpose select method is implemented in SelectQuery. It takes a 
 * mongo-style query and returns all the records that match that query.
 * Select, SelectNear, SelectWithin all handle specific types of queries. 
 * They remove the need for the user to understand mongo syntax, but are really not necessary.
 * 
 * SelectGroupSetupQuery and SelectGroupLookupQuery were implemented as 
 * prototypes of a full-fledged Context Notification Service. They use the 
 * underlying select architecture, but add on to it the notion that the 
 * results of the query are stored in a group guid. Lookups return the 
 * current value of the group guid as determined by executing the query 
 * with the optimization that lookups done more often than user specified interval simply return the last value.
 * 
 * The idea is that we want to look up all the records with a given value or whose
 * value falls in a given range or that more generally match a query.
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

  private static final Logger LOG = Logger.getLogger(Select.class.getName());

  /**
   * @return Logger used by most of the client support package.
   */
  public static final Logger getLogger() {
    return LOG;
  }

  private static boolean useLocalSelect = true; // false is the old stupid remote code, don't do it.

  private static final Random RANDOM_ID = new Random();
  private static final ConcurrentMap<Integer, NSSelectInfo<String>> QUERIES_IN_PROGRESS
          = new ConcurrentHashMap<>(10, 0.75f, 3);
  private static final ConcurrentMap<Integer, SelectResponsePacket<String>> QUERY_RESULT
          = new ConcurrentHashMap<>(10, 0.75f, 3);

  /**
   * Handles a select request that was received from a client.
   *
   * @param packet
   *
   * @param replica
   * @throws JSONException
   * @throws UnknownHostException
   * @throws FailedDBOperationException
   */
  public static void handleSelectRequest(SelectRequestPacket<String> packet,
          GNSApplicationInterface<String> replica) throws JSONException, UnknownHostException, FailedDBOperationException {
    //SelectRequestPacket<String> packet = new SelectRequestPacket<String>(incomingJSON, replica.getGNSNodeConfig());
    if (packet.getNsQueryId() != -1) { // this is how we tell if it has been processed by the NS
      handleSelectRequestFromNS(packet, replica);
    } else {
      throw new UnsupportedOperationException("SelectRequestPacket from client should not be coming here.");
    }
  }

  /* FIXME: arun: need to determine this timeout systematically, not an ad hoc constant.
   */
  private static final long SELECT_REQUEST_TIMEOUT = Config.getGlobalInt(GNSConfig.GNSC.SELECT_REQUEST_TIMEOUT);

  /**
   * Handle a select request from a client.
   * This node is the broadcaster and selector.
   *
   * @param packet
   * @param app
   * @return a select response packet
   * @throws JSONException
   * @throws UnknownHostException
   * @throws FailedDBOperationException
   */
  @SuppressWarnings("unchecked")
  public static SelectResponsePacket<String> handleSelectRequestFromClient(SelectRequestPacket<String> packet,
          GNSApplicationInterface<String> app) throws JSONException, UnknownHostException, FailedDBOperationException {
    // special case handling of the GROUP_LOOK operation
    // If sufficient time hasn't passed we just send the current value back
    if (packet.getGroupBehavior().equals(SelectGroupBehavior.GROUP_LOOKUP)) {
      // grab the timing parameters that we squirreled away from the SETUP
      Date lastUpdate = NSGroupAccess.getLastUpdate(packet.getGuid(), app.getRequestHandler());
      int minRefreshInterval = NSGroupAccess.getMinRefresh(packet.getGuid(), app.getRequestHandler());
      if (lastUpdate != null) {
        getLogger().log(Level.FINE,
                "GROUP_LOOKUP Request: {0} - {1} <= {2}", new Object[]{new Date().getTime(), lastUpdate.getTime(), minRefreshInterval});

        // if not enough time has passed we just return the current value of the group
        if (new Date().getTime() - lastUpdate.getTime() <= minRefreshInterval) {
          getLogger().log(Level.FINE,
                  "GROUP_LOOKUP Request: Time has not elapsed. Returning current group value for {0}", packet.getGuid());
          ResultValue result = NSGroupAccess.lookupMembers(packet.getGuid(), true, app.getRequestHandler());
          //sendReponsePacketToCaller(packet.getId(), packet.getClientAddress(), result.toStringSet(), app);
          return createReponsePacket(packet.getId(), packet.getClientAddress(), result.toStringSet(), app);
        }
      } else {
        getLogger().fine("GROUP_LOOKUP Request: No Last Update Info ");
      }
    }
    // the code below executes for regular selects and also for GROUP SETUP and GROUP LOOKUP but for lookup
    // only if enough time has elapsed since last lookup (see above)
    // OR in the anamolous situation where the update info could not be found
    getLogger().fine(packet.getSelectOperation().toString()
            + " Request: Forwarding request for "
            + packet.getGuid() != null ? packet.getGuid() : "non-guid select");

    // If it's not a group lookup or is but enough time has passed we do the usual thing
    // and send the request out to all the servers. We'll receive a response sent on the flipside.
    Set<String> serverIds = app.getGNSNodeConfig().getActiveReplicas();
    // store the info for later
    int queryId = addQueryInfo(serverIds, packet.getSelectOperation(), packet.getGroupBehavior(),
            packet.getQuery(), packet.getMinRefreshInterval(), packet.getGuid());
    if (packet.getGroupBehavior().equals(SelectGroupBehavior.GROUP_LOOKUP)) {
      // the query string is supplied with a lookup so we stuff in it there. It was saved from the SETUP operation.
      packet.setQuery(NSGroupAccess.getQueryString(packet.getGuid(), app.getRequestHandler()));
    }
    packet.setNameServerID(app.getNodeID());
    packet.setNsQueryId(queryId); // Note: this also tells handleSelectRequest that it should go to NS now
    JSONObject outgoingJSON = packet.toJSONObject();
    getLogger().log(Level.FINE, "NS {0} sending select {1} to {2}",
            new Object[]{app.getNodeID(), packet.getSummary(),
              Util.getOtherThan(serverIds, app.getNodeID())});
    try {
      // forward to all but self because...
      for (String serverId : (Set<String>) Util.getOtherThan(serverIds, app.getNodeID())) {
        app.sendToID(serverId, outgoingJSON);
      }

      // We handle our self by locally getting self-select records
      handleSelectResponse(getMySelectedRecords(packet, app), app);
      // Wait for responses, otherwise you are violating Replicable.execute(.)'s semantics.
      synchronized (QUERIES_IN_PROGRESS) {
        while (QUERIES_IN_PROGRESS.containsKey(queryId)) {
          try {
            QUERIES_IN_PROGRESS.wait(SELECT_REQUEST_TIMEOUT);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }
      }
      if (QUERY_RESULT.containsKey(queryId)) {
        return QUERY_RESULT.remove(queryId);
      }

    } catch (IOException | ClientException e) {
      getLogger().log(Level.SEVERE, "Exception while sending select request: {0}", e);
    }
    return null;
  }

  @SuppressWarnings("unchecked")
  private static SelectResponsePacket<String> getMySelectedRecords(
          SelectRequestPacket<String> request,
          GNSApplicationInterface<String> app) {
    SelectResponsePacket<String> response = null;
    try {
      // grab the records
      JSONArray jsonRecords = getJSONRecordsForSelect(request, app);
      response = SelectResponsePacket.makeSuccessPacketForRecordsOnly(
              request.getId(), request.getClientAddress(),
              request.getCcpQueryId(), request.getNsQueryId(),
              app.getNodeID(), jsonRecords);
      getLogger().log(
              Level.FINE,
              "NS {0} sending back {1} record(s) in response to self-select request {2}",
              new Object[]{app.getNodeID(), jsonRecords.length(),
                request.getSummary()});
    } catch (FailedDBOperationException e) {
      getLogger().log(Level.SEVERE, "Exception while handling self-select request: {0}",
              e.getMessage());
      //e.printStackTrace();
      response = SelectResponsePacket.makeFailPacket(request.getId(), request.getClientAddress(),
              request.getNsQueryId(), app.getNodeID(), e.getMessage());
    }
    return response;
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
  @SuppressWarnings("unchecked")
  private static void handleSelectRequestFromNS(SelectRequestPacket<String> request,
          GNSApplicationInterface<String> app) throws JSONException {
    getLogger().log(Level.FINE,
            "NS {0} {1} received query {2}",
            new Object[]{Select.class.getSimpleName(),
              app.getNodeID(), request.getSummary()});
    // SelectRequestPacket<String> request = new SelectRequestPacket<String>(incomingJSON, app.getGNSNodeConfig());
    try {
      // grab the records
      JSONArray jsonRecords = getJSONRecordsForSelect(request, app);
      @SuppressWarnings("unchecked")
      SelectResponsePacket<String> response = SelectResponsePacket.makeSuccessPacketForRecordsOnly(request.getId(),
              request.getClientAddress(),
              request.getCcpQueryId(), request.getNsQueryId(), app.getNodeID(), jsonRecords);
      getLogger().log(Level.FINE,
              "NS {0} sending back {1} record(s) in response to {2}",
              new Object[]{app.getNodeID(), jsonRecords.length(), request.getSummary()});
      // and send them back to the originating NS
      app.sendToID(request.getNameServerID(), response.toJSONObject());
    } catch (FailedDBOperationException | JSONException | IOException e) {
      getLogger().log(Level.SEVERE, "Exception while handling select request: {0}", e);
      e.printStackTrace();
      SelectResponsePacket<String> failResponse = SelectResponsePacket.makeFailPacket(request.getId(),
              request.getClientAddress(),
              request.getNsQueryId(), app.getNodeID(), e.getMessage());
      try {
        app.sendToID(request.getNameServerID(), failResponse.toJSONObject());
      } catch (IOException f) {
        getLogger().log(Level.SEVERE, "Unable to send Failure SelectResponsePacket: {0}", f);
      }
    }
  }

  /**
   * Handles a select response.
   * This code runs in the collecting NS.
   *
   * @param packet
   * @param replica
   * @throws JSONException
   * @throws edu.umass.cs.gnscommon.exceptions.client.ClientException
   * @throws java.io.IOException
   */
  public static void handleSelectResponse(SelectResponsePacket<String> packet,
          GNSApplicationInterface<String> replica) throws JSONException, ClientException, IOException {
    getLogger().log(Level.FINE,
            "NS {0} recvd from NS {1}",
            new Object[]{replica.getNodeID(),
              packet.getNameServerID()});
    NSSelectInfo<String> info = QUERIES_IN_PROGRESS.get(packet.getNsQueryId());
    if (info == null) {
      getLogger().log(Level.WARNING,
              "NS {0} unabled to located query info:{1}",
              new Object[]{replica.getNodeID(), packet.getNsQueryId()});
      return;
    }
    // if there is no error update our results list
    if (SelectResponsePacket.ResponseCode.NOERROR.equals(packet.getResponseCode())) {
      // stuff all the unique records into the info structure
      processJSONRecords(packet.getRecords(), info, replica);
    } else {
      // error response
      getLogger().log(Level.FINE,
              "NS {0} processing error response: {1}",
              new Object[]{replica.getNodeID(), packet.getErrorMessage()});
    }
    // Remove the NS ID from the list to keep track of who has responded
    boolean allServersResponded = false;
    /* synchronization needed, otherwise assertion in app.sendToClient
     * implying that an outstanding request is always found gets violated. */
    synchronized (info) {
      // Remove the NS ID from the list to keep track of who has responded
      info.removeServerID(packet.getNameServerID());
      allServersResponded = info.allServersResponded();
    }
    if (allServersResponded) {
      handledAllServersResponded(packet, info, replica);
    } else {
      getLogger().log(Level.FINE,
              "NS{0} servers yet to respond:{1}",
              new Object[]{replica.getNodeID(), info.serversYetToRespond()});
    }
  }

  @SuppressWarnings("unchecked")
  private static SelectResponsePacket<String> createReponsePacket(long id,
          InetSocketAddress address, Set<String> guids,
          GNSApplicationInterface<String> app) throws JSONException {
    return SelectResponsePacket.makeSuccessPacketForGuidsOnly(id, null, -1, null, new JSONArray(guids));
  }

  private static void handledAllServersResponded(SelectResponsePacket<String> packet, NSSelectInfo<String> info,
          GNSApplicationInterface<String> replica) throws JSONException, ClientException, IOException {
    // If all the servers have sent us a response we're done.
    Set<String> guids = extractGuidsFromRecords(info.getResponsesAsSet());

    // must be done before the notify below
    // we're done processing this select query
    QUERIES_IN_PROGRESS.remove(packet.getNsQueryId());

    // Pull the records out of the info structure
    SelectResponsePacket<String> response = createReponsePacket(packet.getId(), packet.getReturnAddress(), guids, replica);
    // and put the result where the coordinator can see it.
    QUERY_RESULT.put(packet.getNsQueryId(), response);
    // and let the coordinator know the value is there
    if (GNSApp.DELEGATE_CLIENT_MESSAGING) {
      synchronized (QUERIES_IN_PROGRESS) {
        QUERIES_IN_PROGRESS.notify();
      }
    }
    //sendReponsePacketToCaller(packet.getId(), packet.getReturnAddress(), guids, replica);
    // Now we update any group guid stuff
    if (info.getGroupBehavior().equals(SelectGroupBehavior.GROUP_SETUP)) {
      getLogger().log(Level.FINE,
              "NS{0} storing query string and other info", replica.getNodeID());
      // for setup we need to squirrel away the query for later lookups
      NSGroupAccess.updateQueryString(info.getGuid(), info.getQuery(), replica.getRequestHandler());
      NSGroupAccess.updateMinRefresh(info.getGuid(), info.getMinRefreshInterval(), replica.getRequestHandler());
    }
    if (info.getGroupBehavior().equals(SelectGroupBehavior.GROUP_SETUP) || info.getGroupBehavior().equals(SelectGroupBehavior.GROUP_LOOKUP)) {
      String guid = info.getGuid();
      getLogger().log(Level.FINE, "NS{0} updating group members", replica.getNodeID());
      NSGroupAccess.updateMembers(guid, guids, replica.getRequestHandler(), packet.getReturnAddress());
      //NSGroupAccess.updateRecords(guid, processResponsesIntoJSONArray(info.getResponsesAsMap()), replica); 
      NSGroupAccess.updateLastUpdate(guid, new Date(), replica.getRequestHandler());
    }
  }

  private static Set<String> extractGuidsFromRecords(Set<JSONObject> records) {
    Set<String> result = new HashSet<>();
    for (JSONObject json : records) {
      try {
        result.add(json.getString(NameRecord.NAME.getName()));
      } catch (JSONException e) {
      }
    }
    return result;
  }

  private static int addQueryInfo(Set<String> serverIds, SelectOperation selectOperation,
          SelectGroupBehavior groupBehavior, String query, int minRefreshInterval, String guid) {
    int id;
    do {
      id = RANDOM_ID.nextInt();
    } while (QUERIES_IN_PROGRESS.containsKey(id));
    //Add query info
    NSSelectInfo<String> info = new NSSelectInfo<>(id, serverIds, selectOperation, groupBehavior, query, minRefreshInterval, guid);
    QUERIES_IN_PROGRESS.put(id, info);
    return id;
  }

  private static JSONArray getJSONRecordsForSelect(SelectRequestPacket<String> request,
          GNSApplicationInterface<String> ar) throws FailedDBOperationException {
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
        getLogger().log(Level.FINE, "NS{0} query: {1}",
                new Object[]{ar.getNodeID(), request.getQuery()});
        cursor = NameRecord.selectRecordsQuery(ar.getDB(), request.getQuery());
        break;
      default:
        break;
    }
    // think about returning a cursor that has prefetched a limited (100 which is like mongo limit)
    // number of records in it and the ability to fetch more
    while (cursor != null && cursor.hasNext()) {
      jsonRecords.put(cursor.nextJSONObject());
    }
    return jsonRecords;
  }

  // takes the JSON records that are returned from an NS and stuffs the into the NSSelectInfo record
  private static void processJSONRecords(JSONArray jsonArray, NSSelectInfo<String> info,
          GNSApplicationInterface<String> ar) throws JSONException {
    int length = jsonArray.length();
    getLogger().log(Level.FINE,
            "NS{0} processing {1} records", new Object[]{ar.getNodeID(), length});
    // org.json sucks... should have converted a long time ago
    for (int i = 0; i < length; i++) {
      JSONObject record = jsonArray.getJSONObject(i);
      String name = record.getString(NameRecord.NAME.getName());
      if (info.addResponseIfNotSeenYet(name, record)) {
        getLogger().log(Level.FINE, "NS{0} added record for {1}", new Object[]{ar.getNodeID(), name});
      } else {
        getLogger().log(Level.FINE, "NS{0} DID NOT ADD record for {1}", new Object[]{ar.getNodeID(), name});
      }
    }
  }

//  /**
//   * Returns true if new local select code should be used.
//   * Or obsolete remote local select should not be used.
//   *
//   * @return the USE_LOCAL_SELECT
//   */
//  public static boolean useLocalSelect() {
//    return useLocalSelect;
//  }

}
