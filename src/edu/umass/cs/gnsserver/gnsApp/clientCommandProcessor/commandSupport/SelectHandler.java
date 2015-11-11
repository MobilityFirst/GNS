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
package edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.commandSupport;

import edu.umass.cs.gnscommon.GnsProtocol;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.demultSupport.ClientRequestHandlerInterface;
import edu.umass.cs.gnsserver.main.GNS;
import edu.umass.cs.gnsserver.gnsApp.packet.SelectRequestPacket;
import edu.umass.cs.gnsserver.gnsApp.packet.SelectRequestPacket.SelectOperation;
import edu.umass.cs.gnsserver.gnsApp.packet.SelectRequestPacket.GroupBehavior;
import edu.umass.cs.gnsserver.gnsApp.packet.SelectResponsePacket;
import edu.umass.cs.nio.interfaces.Stringifiable;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static edu.umass.cs.gnsserver.gnsApp.packet.Packet.getPacketType;

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
  private static ConcurrentMap<Integer, SelectResponsePacket<String>> resultsMap = new ConcurrentHashMap<Integer, SelectResponsePacket<String>>(10, 0.75f, 3);
  private static Random randomID = new Random();

  /**
   * Sends the basic Select query which returns a list of guids that have contain the given key / value pair.
   * The returned value is encoded as a JSON Array string.
   *
   * @param operation
   * @param key
   * @param value
   * @param otherValue
   * @param handler
   * @return the list of guids in a string encoded as a JSON Array
   */
  public static String sendSelectRequest(SelectOperation operation, String key, Object value, Object otherValue, ClientRequestHandlerInterface handler) {
    int id = nextRequestID();
    return sendSelectHelper(id, new SelectRequestPacket(id, handler.getNodeAddress(), operation, 
            GroupBehavior.NONE, key, value, otherValue), handler);
  }

  /**
   * Sends the a Select query which returns a list of guids match the given query.
   * The returned value is encoded as a JSON Array string.
   *
   * @param query
   * @param handler
   * @return the list of guids in a string encoded as a JSON Array
   */
  public static String sendSelectQuery(String query, ClientRequestHandlerInterface handler) {
    int id = nextRequestID();
    return sendSelectHelper(id, SelectRequestPacket.MakeQueryRequest(id, handler.getNodeAddress(), query), handler);
  }

  /**
   * Sends the Select query which sets up a group guid whose members match the given query.
   * Interval is the minimum refresh interval of the query. See <code>DEFAULT_MIN_REFRESH_INTERVAL</code>.
   *
   * @param query
   * @param guid
   * @param interval
   * @param handler
   * @return the list of guids in a string encoded as a JSON Array
   */
  public static String sendGroupGuidSetupSelectQuery(String query, String guid, int interval, ClientRequestHandlerInterface handler) {
    int id = nextRequestID();
    if (interval == INVALID_REFRESH_INTERVAL) {
      interval = DEFAULT_MIN_REFRESH_INTERVAL;
    }
    return sendSelectHelper(id, SelectRequestPacket.MakeGroupSetupRequest(id, handler.getNodeAddress(), query, guid, interval), 
            handler);
  }

  /**
   * Sends the Select query which sets up a group guid whose members match the given query.
   * The returned value is encoded as a JSON Array string.
   * 
   * @param query
   * @param guid
   * @param handler
   * @return the list of guids in a string encoded as a JSON Array
   */
  public static String sendGroupGuidSetupSelectQuery(String query, String guid, ClientRequestHandlerInterface handler) {
    return sendGroupGuidSetupSelectQuery(query, guid, DEFAULT_MIN_REFRESH_INTERVAL, handler);
  }

  /**
   * Sends the Select query which returns the members of a previously created group guid.
   * The returned value is encoded as a JSON Array string.
   *
   * @param guid
   * @param handler
   * @return the list of guids in a string encoded as a JSON Array
   */
  public static String sendGroupGuidLookupSelectQuery(String guid, ClientRequestHandlerInterface handler) {
    int id = nextRequestID();
    return sendSelectHelper(id, SelectRequestPacket.MakeGroupLookupRequest(id, handler.getNodeAddress(), guid), handler);
  }

  private static String sendSelectHelper(int id, SelectRequestPacket sendPacket, ClientRequestHandlerInterface handler) {
    try {
      handler.getIntercessor().injectPacketIntoCCPQueue(sendPacket.toJSONObject());
    } catch (JSONException e) {
      GNS.getLogger().warning("Ignoring JSON error while sending Select request: " + e);
      e.printStackTrace();
      return null;
    }
    waitForResponsePacket(id);
    SelectResponsePacket<String> packet = resultsMap.get(id);
    if (SelectResponsePacket.ResponseCode.NOERROR.equals(packet.getResponseCode())) {
      JSONArray json = packet.getGuids();
      if (json != null) {
        return json.toString();
      } else {
        return null;
      }
    } else {
      return GnsProtocol.BAD_RESPONSE + " " + GnsProtocol.SELECTERROR + " " + packet.getErrorMessage();
    }
  }

  /**
   * Processes incoming SelectResponsePacket packets.
   * 
   * @param json
   * @param unstringer
   */
  public static void processSelectResponsePackets(JSONObject json, Stringifiable<String> unstringer) {
    try {
      switch (getPacketType(json)) {
        case SELECT_RESPONSE:
          try {
            SelectResponsePacket<String> response = new SelectResponsePacket<String>(json, unstringer);
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
}
