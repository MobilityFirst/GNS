/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.client;

import edu.umass.cs.gns.localnameserver.LocalNameServer;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nameserver.NameRecordKey;
import static edu.umass.cs.gns.packet.Packet.*;
import edu.umass.cs.gns.packet.QueryRequestPacket;
import edu.umass.cs.gns.packet.QueryResponsePacket;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 * @author westy
 */
public class SelectQueryHandler {

  private static final Object monitor = new Object();
  private static ConcurrentMap<Integer, JSONArray> result = new ConcurrentHashMap<Integer, JSONArray>(10, 0.75f, 3);
  private static Random randomID = new Random();

  public static String sendQueryRequest(NameRecordKey key, Object value) {
    int id = nextRequestID();
    try {
      Intercessor.getInstance().sendPacket(new QueryRequestPacket(id, key, value, LocalNameServer.nodeID).toJSONObject());
    } catch (JSONException e) {
      GNS.getLogger().warning("Ignoring JSON error while sending QUERY request: " + e);
      e.printStackTrace();
      return null;
    }
    waitForResponsePacket(id);
    JSONArray json = result.get(id);
    if (json != null) {
      return json.toString();
    } else {
      return null;
    }
  }

  public static void processQueryResponsePackets(JSONObject json) {
    try {
      switch (getPacketType(json)) {
        case QUERY_RESPONSE:
          try {
            QueryResponsePacket response = new QueryResponsePacket(json);
            int id = response.getId();
            GNS.getLogger().info("Processing QueryResponse for " + id);
            synchronized (monitor) {
              result.put(id, response.getJsonArray());
              monitor.notifyAll();
            }
          } catch (JSONException e) {
            GNS.getLogger().warning("JSON error during QueryResponse processing: " + e);
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
        while (!result.containsKey(sequenceNumber)) {
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
    } while (result.containsKey(id));
    return id;
  }
}
