/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.clientsupport;

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
  private static ConcurrentMap<Integer, SelectResponsePacket> resultsMap = new ConcurrentHashMap<Integer, SelectResponsePacket>(10, 0.75f, 3);
  private static Random randomID = new Random();
  
  public static String sendSelectRequest(SelectRequestPacket.SelectOperation operation, NameRecordKey key, Object value, Object otherValue) {
    int id = nextRequestID();
    return sendSelectHelper(id, new SelectRequestPacket(id, LocalNameServer.nodeID, operation, key, value, otherValue)); 
  }
  
  public static String sendSelectQuery(String query) {
    int id = nextRequestID();
    return sendSelectHelper(id, SelectRequestPacket.MakeQueryRequest(id, LocalNameServer.nodeID, query));
  }
  
  public static String sendSelectHelper(int id, SelectRequestPacket sendPacket) {
    try {
      Intercessor.sendPacket(sendPacket.toJSONObject());
    } catch (JSONException e) {
      GNS.getLogger().warning("Ignoring JSON error while sending Select request: " + e);
      e.printStackTrace();
      return null;
    }
    waitForResponsePacket(id);
    SelectResponsePacket packet = resultsMap.get(id);
    if (SelectResponsePacket.ResponseCode.NOERROR.equals(packet.getResponseCode())) {
      JSONArray json = packet.getJsonArray();
      if (json != null) {
        return buildResultString(json);
      } else {
        return null;
      }
    } else {
      return Defs.BADRESPONSE + " " + Defs.SELECTERROR + " " + packet.getErrorMessage();
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
