/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.nsdesign.clientsupport;

import edu.umass.cs.gns.clientsupport.QueryResult;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.util.NameRecordKey;
import edu.umass.cs.gns.nsdesign.gnsReconfigurable.GnsReconfigurable;
import edu.umass.cs.gns.nsdesign.packet.DNSPacket;
import edu.umass.cs.gns.util.ConsistentHashing;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * This class handles sending DNS queries from one NS to another. 
 * Currently it is used by the code that does ACL checks in the NS to look up GUID info.
 * 
 * Note: Will be reimplementing this as a new class that sends requests to an LNS instead.
 * This will be replaced by LNSQueryHandler when we get that one working.
 *
 * 
 * @author westy
 */
@Deprecated
public class SiteToSiteQueryHandler {

  private static final Object monitor = new Object();
  private static ConcurrentMap<Integer, QueryResult> queryResultMap = new ConcurrentHashMap<Integer, QueryResult>(10, 0.75f, 3);
  private static ConcurrentMap<Integer, Integer> outStandingQueries = new ConcurrentHashMap<Integer, Integer>(10, 0.75f, 3);
  private static Random randomID = new Random();

  public static QueryResult sendQuery(String name, String key, GnsReconfigurable activeReplica) {
    GNS.getLogger().fine("Sending query: " + name + " " + key);
    int id = nextRequestID();
    // use this to filter out everything but the first responder
    outStandingQueries.put(id, id);
    // send a bunch out
    for (int server : ConsistentHashing.getReplicaControllerSet(name)) {
      sendQueryInternal(id, server, name, key, activeReplica);
    }
    // now we wait until the first correct packet comes back
    waitForResponsePacket(id);
    QueryResult result = queryResultMap.get(id);
    queryResultMap.remove(id);
    return result;
  }

  private static void sendQueryInternal(int queryId, int recipientId, String name, String key, GnsReconfigurable activeReplica) {
    GNS.getLogger().fine("Sending query " + queryId + " to " + recipientId + " for " + name + " / " + key);
    DNSPacket queryrecord = new DNSPacket(activeReplica.getNodeID(), queryId, name, new NameRecordKey(key), null, null, null);
    JSONObject json;
    try {
      json = queryrecord.toJSONObjectQuestion();
      activeReplica.getNioServer().sendToID(recipientId, json);
    } catch (JSONException e) {
      GNS.getLogger().severe("Problem converting packet to JSON Object:" + e);
    } catch (IOException e) {
      GNS.getLogger().severe("Problem sending packet to NS " + recipientId + ": " + e);
    }
  }

  public static void handleDNSResponsePacket(DNSPacket dnsResponsePacket, GnsReconfigurable activeReplica) {
    int id = dnsResponsePacket.getQueryId();
    if (!dnsResponsePacket.containsAnyError()) {
      //Packet is a response and does not have a response error
      synchronized (monitor) {
        if (outStandingQueries.remove(id) != null) {
          GNS.getLogger().finer("First Response (" + id + "): "
                  + dnsResponsePacket.getGuid() + "/" + dnsResponsePacket.getKey() + " Successful Received");

          queryResultMap.put(id, new QueryResult(dnsResponsePacket.getRecordValue(), activeReplica.getNodeID()));
          monitor.notifyAll();
        } else {
          GNS.getLogger().finer("Later Response (" + id + "): "
                  + dnsResponsePacket.getGuid() + "/" + dnsResponsePacket.getKey() + " Successful Received");
        }
      }
    } else {
      synchronized (monitor) {
        if (outStandingQueries.remove(id) != null) {
          GNS.getLogger().finer("First Response (" + id + "): "
                  + dnsResponsePacket.getGuid() + "/" + dnsResponsePacket.getKey()
                  + " Error Received: " + dnsResponsePacket.getHeader().getResponseCode().name());
          queryResultMap.put(id, new QueryResult(dnsResponsePacket.getHeader().getResponseCode(), activeReplica.getNodeID()));
          monitor.notifyAll();
        } else {
          GNS.getLogger().finer("Later Response (" + id + "): "
                  + dnsResponsePacket.getGuid() + "/" + dnsResponsePacket.getKey()
                  + " Error Received: " + dnsResponsePacket.getHeader().getResponseCode().name());
        }
      }
    }
  }

  private static void waitForResponsePacket(int id) {
    try {
      synchronized (monitor) {
        while (!queryResultMap.containsKey(id)) {
          monitor.wait();
        }
        GNS.getLogger().fine("Query id response received: " + id);
      }
    } catch (InterruptedException x) {
      GNS.getLogger().severe("Wait for update success confirmation packet was interrupted " + x);
    }
  }

  private static int nextRequestID() {
    int id;
    do {
      id = randomID.nextInt();
    } while (queryResultMap.containsKey(id));
    return id;
  }
  public static String Version = "$Revision: 481 $";
}
