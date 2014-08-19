/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.clientsupport;

import edu.umass.cs.gns.localnameserver.LocalNameServer;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nsdesign.packet.SelectRequestPacket;
import edu.umass.cs.gns.nsdesign.packet.SelectRequestPacket.SelectOperation;
import edu.umass.cs.gns.nsdesign.packet.SelectRequestPacket.GroupBehavior;
import edu.umass.cs.gns.nsdesign.packet.SelectResponsePacket;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static edu.umass.cs.gns.nsdesign.packet.Packet.getPacketType;

/**
 * Handles the set of select commands all of which return a set of guids based on a query.
 * Also handles context sensitive group guid setup and maintainence.
 *
 * @author westy
 */
public class SelectHandler {

  /**
   * The default interval (in seconds) before which a query will not be refreshed. In other words
   * if you wait this interval you will get the latest from the database, otherwise you will get the
   * cached value.
   */
  public static final int DEFAULT_MIN_REFRESH_INTERVAL = 60; //seconds
  /**
   * An invalid interval value.
   */
  public static final int INVALID_REFRESH_INTERVAL = -1;

  private static final Object monitor = new Object();
  private static ConcurrentMap<Integer, SelectResponsePacket> resultsMap = new ConcurrentHashMap<Integer, SelectResponsePacket>(10, 0.75f, 3);
  private static Random randomID = new Random();

  /**
   * Sends the basic Select query which returns a list of guids that have contain the given key / value pair.
   *
   * @param operation
   * @param key
   * @param value
   * @param otherValue
   * @return
   */
  public static String sendSelectRequest(SelectOperation operation, String key, Object value, Object otherValue) {
    int id = nextRequestID();
    return sendSelectHelper(id, new SelectRequestPacket(id, LocalNameServer.getNodeID(), operation, GroupBehavior.NONE, key, value, otherValue));
  }

  /**
   * Sends the a Select query which returns a list of guids match the given query.
   *
   * @param query
   * @return
   */
  public static String sendSelectQuery(String query) {
    int id = nextRequestID();
    return sendSelectHelper(id, SelectRequestPacket.MakeQueryRequest(id, LocalNameServer.getNodeID(), query));
  }

  /**
   * Sends the Select query which sets up a group guid whose members match the given query.
   * Interval is the minimum refresh interval of the query. See <code>DEFAULT_MIN_REFRESH_INTERVAL</code>.
   *
   * @param query
   * @param guid
   * @param interval
   * @return
   */
  public static String sendGroupGuidSetupSelectQuery(String query, String guid, int interval) {
    int id = nextRequestID();
    if (interval == INVALID_REFRESH_INTERVAL) {
      interval = DEFAULT_MIN_REFRESH_INTERVAL;
    }
    return sendSelectHelper(id, SelectRequestPacket.MakeGroupSetupRequest(id, LocalNameServer.getNodeID(), query, guid, interval));
  }

  /**
   * Sends the Select query which sets up a group guid whose members match the given query.
   *
   * @param query
   * @param guid
   * @return
   */
  public static String sendGroupGuidSetupSelectQuery(String query, String guid) {
    return sendGroupGuidSetupSelectQuery(query, guid, DEFAULT_MIN_REFRESH_INTERVAL);
  }

  /**
   * Sends the Select query which returns the members of a previously created group guid.
   *
   * @param guid
   * @return
   */
  public static String sendGroupGuidLookupSelectQuery(String guid) {
    int id = nextRequestID();
    return sendSelectHelper(id, SelectRequestPacket.MakeGroupLookupRequest(id, LocalNameServer.getNodeID(), guid));
  }

  private static String sendSelectHelper(int id, SelectRequestPacket sendPacket) {
    try {
      Intercessor.injectPacketIntoLNSQueue(sendPacket.toJSONObject());
    } catch (JSONException e) {
      GNS.getLogger().warning("Ignoring JSON error while sending Select request: " + e);
      e.printStackTrace();
      return null;
    }
    waitForResponsePacket(id);
    SelectResponsePacket packet = resultsMap.get(id);
    if (SelectResponsePacket.ResponseCode.NOERROR.equals(packet.getResponseCode())) {
      JSONArray json = packet.getGuids();
      if (json != null) {
        return json.toString();
      } else {
        return null;
      }
    } else {
      return Defs.BADRESPONSE + " " + Defs.SELECTERROR + " " + packet.getErrorMessage();
    }
  }

  /**
   * Processes incoming SelectResponsePacket packets.
   * 
   * @param json
   */
  public static void processSelectResponsePackets(JSONObject json) {
    try {
      switch (getPacketType(json)) {
        case SELECT_RESPONSE:
          try {
            SelectResponsePacket response = new SelectResponsePacket(json);
            int id = response.getId();
            GNS.getLogger().fine("Processing SelectResponse for " + id);
            synchronized (monitor) {
              resultsMap.put(id, response);
              monitor.notifyAll();
            }
          } catch (JSONException e) {
            GNS.getLogger().warning("JSON error during SelectResponse processing: " + e);
          }
          break;
      }
    } catch (JSONException e) {
      GNS.getLogger().warning("JSON error while getting packet type: " + e);
    }
  }

  private static void waitForResponsePacket(int sequenceNumber) {
    try {
      synchronized (monitor) {
        while (!resultsMap.containsKey(sequenceNumber)) {
          monitor.wait();
        }
      }
    } catch (InterruptedException x) {
      GNS.getLogger().severe("Wait for update success confirmation packet was interrupted " + x);
    }
  }

  private static int nextRequestID() {
    int id;
    do {
      id = randomID.nextInt();
    } while (resultsMap.containsKey(id));
    return id;
  }

  public static String Version = "$Revision$";
}
