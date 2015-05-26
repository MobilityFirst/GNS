/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.newApp.clientSupport;

import edu.umass.cs.gns.newApp.clientCommandProcessor.commandSupport.QueryResult;
import edu.umass.cs.gns.database.ColumnFieldType;
import edu.umass.cs.gns.main.GNS;

import edu.umass.cs.gns.newApp.GnsApplicationInterface;
import edu.umass.cs.gns.newApp.packet.DNSPacket;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * This class handles sending DNS queries from a NameServer back to a Local Name Server.
 *
 * @author westy
 */
public class LNSQueryHandler {

  private static final Object monitor = new Object();
  private static final ConcurrentMap<Integer, QueryResult> queryResultMap = new ConcurrentHashMap<Integer, QueryResult>(10, 0.75f, 3);
  private static final ConcurrentMap<Integer, Integer> outStandingQueries = new ConcurrentHashMap<Integer, Integer>(10, 0.75f, 3);
  private static final Random randomID = new Random();

  /**
   * Sends a DNS query from this Name Server to a Local Name Server.
   * 
   * @param name
   * @param key
   * @param activeReplica
   * @param lnsAddress
   * @return 
   */
  public static QueryResult sendQuery(String name, String key, GnsApplicationInterface activeReplica, 
          InetSocketAddress lnsAddress) {
    return sendQuery(name, key, ColumnFieldType.USER_JSON, activeReplica, lnsAddress);
  }
  
  /**
   * Sends a DNS query from this Name Server to a Local Name Server.
   * The value returned in the QueryResult will be an old-style values list.
   * 
   * @param name
   * @param key
   * @param activeReplica
   * @param lnsAddress
   * @return 
   */
  public static QueryResult sendListFieldQuery(String name, String key, GnsApplicationInterface activeReplica, 
          InetSocketAddress lnsAddress) {
    return sendQuery(name, key, ColumnFieldType.LIST_STRING, activeReplica, lnsAddress);
  }
  
  /**
   * Sends a DNS query from this Name Server to a Local Name Server
   * Returns the entire guid record in a QueryResult.
   *
   * @param name
   * @param key
   * @param activeReplica
   * @return the entire guid record in a QueryResult.
   */
  private static QueryResult sendQuery(String name, String key, ColumnFieldType returnFormat, GnsApplicationInterface activeReplica, 
          InetSocketAddress lnsAddress) {
    GNS.getLogger().fine("Node " + activeReplica.getNodeID() + "; Sending query: " + name + " " + key);
    int id = nextRequestID();
    // use this to filter out everything but the first responder
    outStandingQueries.put(id, id);
    sendQueryInternal(id, lnsAddress, name, key, returnFormat, activeReplica);
    // now we wait until the packet comes back
    waitForResponsePacket(id);
    QueryResult result = queryResultMap.get(id);
    queryResultMap.remove(id);
    return result;
  }

  private static void sendQueryInternal(int queryId, InetSocketAddress lnsAddress, String name, String key, 
          ColumnFieldType returnFormat, GnsApplicationInterface activeReplica) {
    DNSPacket queryrecord = new DNSPacket(activeReplica.getNodeID(), queryId, name, key, null,
            returnFormat,
            null, null, null);
    JSONObject json;
    try {
      json = queryrecord.toJSONObjectQuestion();
      GNS.getLogger().info("########## Node " + activeReplica.getNodeID() + "; Sending query " + queryId + " to " + lnsAddress
              + " for " + name + " / " + key + ": " + json);
      activeReplica.getNioServer().sendToAddress(lnsAddress, json);
    } catch (JSONException e) {
      GNS.getLogger().severe("Problem converting packet to JSON Object:" + e);
    } catch (IOException e) {
      GNS.getLogger().severe("Problem sending packet to NS " + lnsAddress + ": " + e);
    }
  }

  /**
   * Handles a DNS query response coming back to this NameServer from a Local Name Server
   *
   * @param dnsResponsePacket
   * @param activeReplica
   */
  public static void handleDNSResponsePacket(DNSPacket dnsResponsePacket, GnsApplicationInterface activeReplica) {
    int id = dnsResponsePacket.getQueryId();
    if (!dnsResponsePacket.containsAnyError()) {
      //Packet is a response and does not have a response error
      synchronized (monitor) {
        if (outStandingQueries.remove(id) != null) {
          GNS.getLogger().fine("First success response (" + id + "): "
                  + dnsResponsePacket.getGuid() + "/" + dnsResponsePacket.getKeyOrKeysString()+ " Successful Received");

          queryResultMap.put(id, new QueryResult(dnsResponsePacket.getRecordValue(), activeReplica.getNodeID()));
          monitor.notifyAll();
        } else {
          GNS.getLogger().fine("Later success response (" + id + "): "
                  + dnsResponsePacket.getGuid() + "/" + dnsResponsePacket.getKeyOrKeysString()+ " Successful Received");
        }
      }
    } else {
      synchronized (monitor) {
        if (outStandingQueries.remove(id) != null) {
          GNS.getLogger().fine("First error response (" + id + "): "
                  + dnsResponsePacket.getGuid() + "/" + dnsResponsePacket.getKeyOrKeysString()
                  + " Error Received: " + dnsResponsePacket.getHeader().getResponseCode().name());
          queryResultMap.put(id, new QueryResult(dnsResponsePacket.getHeader().getResponseCode(), activeReplica.getNodeID()));
          monitor.notifyAll();
        } else {
          GNS.getLogger().fine("Later error response (" + id + "): "
                  + dnsResponsePacket.getGuid() + "/" + dnsResponsePacket.getKeyOrKeysString()
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

}
