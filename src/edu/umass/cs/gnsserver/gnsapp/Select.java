
package edu.umass.cs.gnsserver.gnsapp;


import edu.umass.cs.gnscommon.exceptions.client.ClientException;
import edu.umass.cs.gnsserver.database.AbstractRecordCursor;
import edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException;
import edu.umass.cs.gnscommon.exceptions.server.InternalRequestException;
import edu.umass.cs.gnscommon.packets.PacketUtils;

import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.GroupAccess;
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
import edu.umass.cs.gnsserver.interfaces.InternalRequestHeader;
import edu.umass.cs.gnsserver.main.GNSConfig;
import edu.umass.cs.gnsserver.utils.ResultValue;
import edu.umass.cs.utils.Config;
import edu.umass.cs.utils.Util;

import java.net.InetSocketAddress;
import java.util.Date;
import java.util.HashSet;
import java.util.logging.Logger;


public class Select {

  private static final Logger LOG = Logger.getLogger(Select.class.getName());


  public static final Logger getLogger() {
    return LOG;
  }

  private static boolean useLocalSelect = true; // false is the old stupid remote code, don't do it.

  private static final Random RANDOM_ID = new Random();
  private static final ConcurrentMap<Integer, NSSelectInfo<String>> QUERIES_IN_PROGRESS
          = new ConcurrentHashMap<>(10, 0.75f, 3);
  private static final ConcurrentMap<Integer, SelectResponsePacket<String>> QUERY_RESULT
          = new ConcurrentHashMap<>(10, 0.75f, 3);


  public static void handleSelectRequest(SelectRequestPacket<String> packet,
          GNSApplicationInterface<String> replica) throws JSONException, UnknownHostException, FailedDBOperationException {
    //SelectRequestPacket<String> packet = new SelectRequestPacket<String>(incomingJSON, replica.getGNSNodeConfig());
    if (packet.getNsQueryId() != -1) { // this is how we tell if it has been processed by the NS
      handleSelectRequestFromNS(packet, replica);
    } else {
      throw new UnsupportedOperationException("SelectRequestPacket from client should not be coming here.");
    }
  }


  private static final long SELECT_REQUEST_TIMEOUT = Config.getGlobalInt(GNSConfig.GNSC.SELECT_REQUEST_TIMEOUT);


  @SuppressWarnings("unchecked")
  public static SelectResponsePacket<String> handleSelectRequestFromClient(InternalRequestHeader header, SelectRequestPacket<String> packet,
          GNSApplicationInterface<String> app) throws JSONException, UnknownHostException, FailedDBOperationException, InternalRequestException {
    // special case handling of the GROUP_LOOK operation
    // If sufficient time hasn't passed we just send the current value back
    if (packet.getGroupBehavior().equals(SelectGroupBehavior.GROUP_LOOKUP)) {
      // grab the timing parameters that we squirreled away from the SETUP
      Date lastUpdate = NSGroupAccess.getLastUpdate(header, packet.getGuid(), app.getRequestHandler());
      int minRefreshInterval = NSGroupAccess.getMinRefresh(header, packet.getGuid(), app.getRequestHandler());
      if (lastUpdate != null) {
        getLogger().log(Level.FINE,
                "GROUP_LOOKUP Request: {0} - {1} <= {2}", new Object[]{new Date().getTime(), lastUpdate.getTime(), minRefreshInterval});

        // if not enough time has passed we just return the current value of the group
        if (new Date().getTime() - lastUpdate.getTime() <= minRefreshInterval) {
          getLogger().log(Level.FINE,
                  "GROUP_LOOKUP Request: Time has not elapsed. Returning current group value for {0}", packet.getGuid());
          ResultValue result = NSGroupAccess.lookupMembers(header, packet.getGuid(), true, app.getRequestHandler());
          //sendReponsePacketToCaller(packet.getId(), packet.getClientAddress(), result.toStringSet(), app);
          return createReponsePacket(header, packet.getId(), packet.getClientAddress(), result.toStringSet(), app);
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
      packet.setQuery(NSGroupAccess.getQueryString(header, packet.getGuid(), app.getRequestHandler()));
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


  public static void handleSelectResponse(SelectResponsePacket<String> packet,
          GNSApplicationInterface<String> replica) throws JSONException, ClientException, IOException, InternalRequestException {
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

    synchronized (info) {
      // Remove the NS ID from the list to keep track of who has responded
      info.removeServerID(packet.getNameServerID());
      allServersResponded = info.allServersResponded();
    }
    if (allServersResponded) {
      handledAllServersResponded(PacketUtils.getInternalRequestHeader(packet), packet, info, replica);
    } else {
      getLogger().log(Level.FINE,
              "NS{0} servers yet to respond:{1}",
              new Object[]{replica.getNodeID(), info.serversYetToRespond()});
    }
  }

  @SuppressWarnings("unchecked")
  private static SelectResponsePacket<String> createReponsePacket(InternalRequestHeader header, long id,
          InetSocketAddress address, Set<String> guids,
          GNSApplicationInterface<String> app) throws JSONException {
    return SelectResponsePacket.makeSuccessPacketForGuidsOnly(id, null, -1, null, new JSONArray(guids));
  }

  private static void handledAllServersResponded(InternalRequestHeader header, SelectResponsePacket<String> packet, NSSelectInfo<String> info,
          GNSApplicationInterface<String> replica) throws JSONException, ClientException, IOException, InternalRequestException {
    // If all the servers have sent us a response we're done.
    Set<String> guids = extractGuidsFromRecords(info.getResponsesAsSet());

    // must be done before the notify below
    // we're done processing this select query
    QUERIES_IN_PROGRESS.remove(packet.getNsQueryId());

    // Pull the records out of the info structure
    SelectResponsePacket<String> response = createReponsePacket(header, packet.getId(), packet.getReturnAddress(), guids, replica);
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
      NSGroupAccess.updateQueryString(header, info.getGuid(), info.getQuery(), replica.getRequestHandler());
      NSGroupAccess.updateMinRefresh(header, info.getGuid(), info.getMinRefreshInterval(), replica.getRequestHandler());
    }
    if (info.getGroupBehavior().equals(SelectGroupBehavior.GROUP_SETUP) || info.getGroupBehavior().equals(SelectGroupBehavior.GROUP_LOOKUP)) {
      String guid = info.getGuid();
      getLogger().log(Level.FINE, "NS{0} updating group members", replica.getNodeID());
      GroupAccess.addToGroup(header, guid, new ResultValue(guids), null, null, null, null, 
              replica.getRequestHandler());
      //NSGroupAccess.updateMembers(header, guid, guids, replica.getRequestHandler());
      //NSGroupAccess.updateRecords(guid, processResponsesIntoJSONArray(info.getResponsesAsMap()), replica); 
      NSGroupAccess.updateLastUpdate(header, guid, new Date(), replica.getRequestHandler());
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

//
//  public static boolean useLocalSelect() {
//    return useLocalSelect;
//  }

}
