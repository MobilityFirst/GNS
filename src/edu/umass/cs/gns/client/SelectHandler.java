/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.client;

import edu.umass.cs.gns.localnameserver.LocalNameServer;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nameserver.NameRecord;
import edu.umass.cs.gns.nameserver.NameRecordKey;
import static edu.umass.cs.gns.packet.Packet.*;
import edu.umass.cs.gns.packet.SelectRequestPacket;
import edu.umass.cs.gns.packet.SelectResponsePacket;
import java.util.Iterator;
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
public class SelectHandler {

  private static final Object monitor = new Object();
  private static ConcurrentMap<Integer, JSONArray> resultsMap = new ConcurrentHashMap<Integer, JSONArray>(10, 0.75f, 3);
  private static Random randomID = new Random();

  public static String sendSelectRequest(NameRecordKey key, Object value) {
    int id = nextRequestID();
    try {
      Intercessor.getInstance().sendPacket(new SelectRequestPacket(id, key, value, LocalNameServer.nodeID).toJSONObject());
    } catch (JSONException e) {
      GNS.getLogger().warning("Ignoring JSON error while sending Select request: " + e);
      e.printStackTrace();
      return null;
    }
    waitForResponsePacket(id);
    JSONArray json = resultsMap.get(id);
    if (json != null) {
      return buildResultString(json);
    } else {
      return null;
    }
  }

  private static String buildResultString(JSONArray json) {
    // extract the name and values from the returned records
    JSONArray result = new JSONArray();
    for (int i = 0; i < json.length(); i++) {
      JSONObject jsonRecord = new JSONObject();
      try {
        jsonRecord.put("GUID", json.getJSONObject(i).getString(NameRecord.NAME.getName()));
        extractFieldsIntoJSONObject(json.getJSONObject(i).getJSONObject(NameRecord.VALUES_MAP.getName()), jsonRecord);
        result.put(jsonRecord);
      } catch (JSONException e) {
        GNS.getLogger().warning("Ignoring JSON error processing Select response: " + e);
      }
    }
    return result.toString();
  }

  private static JSONObject extractFieldsIntoJSONObject(JSONObject record, JSONObject JSONresult) {
    Iterator<String> iter = record.keys();
    while (iter.hasNext()) {
      String key = iter.next();
      if (!GNS.isInternalField(key)) {
        try {
          JSONresult.put(key, record.getJSONArray(key));
        } catch (JSONException e) {
          GNS.getLogger().warning("Ignoring JSON error while extracting result from Select response: " + e);
        }
      }
    }
    return JSONresult;
  }

  public static void processSelectResponsePackets(JSONObject json) {
    try {
      switch (getPacketType(json)) {
        case SELECT_RESPONSE:
          try {
            SelectResponsePacket response = new SelectResponsePacket(json);
            int id = response.getId();
            GNS.getLogger().info("Processing SelectResponse for " + id);
            synchronized (monitor) {
              resultsMap.put(id, response.getJsonArray());
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
