/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.localnameserver;

/**
 * User: abhigyan
 * Date: 8/30/13
 * Time: 3:33 PM
 * To change this template use File | Settings | File Templates.
 */

import edu.umass.cs.gns.clientsupport.Intercessor;
import edu.umass.cs.gns.exceptions.CancelExecutorTaskException;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.ReplicationFrameworkType;
import edu.umass.cs.gns.main.StartLocalNameServer;
import edu.umass.cs.gns.nameserver.NameRecordKey;
import edu.umass.cs.gns.nameserver.ResultValue;
import edu.umass.cs.gns.packet.DNSPacket;
import edu.umass.cs.gns.packet.DNSRecordType;
import edu.umass.cs.gns.packet.NSResponseCode;
import edu.umass.cs.gns.packet.RequestActivesPacket;
import edu.umass.cs.gns.util.BestServerSelection;
import edu.umass.cs.gns.util.ConfigFileInfo;
import edu.umass.cs.gns.util.HashFunction;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.HashSet;
import java.util.Set;
import java.util.TimerTask;
import java.util.logging.Level;

public class DNSRequestTask extends TimerTask {

  DNSPacket incomingPacket;
  long receivedTime; // overall latency
  private int transmissionCount = 0;
  private int lookupNumber;
  private int queryId = 0;
  private int numRestarts;
  private HashSet<Integer> nameserversQueried;
  private int coordinatorID = -1;

  public DNSRequestTask(DNSPacket incomingPacket,
          long receivedTime,
          int lookupNumber, int queryId,
          HashSet<Integer> nameserversQueried, int numRestarts) {
    this.incomingPacket = incomingPacket;
    this.receivedTime = receivedTime;
    this.lookupNumber = lookupNumber;
    this.nameserversQueried = nameserversQueried;
    this.numRestarts = numRestarts;
  }

  @Override
  public void run() {
    try {
      transmissionCount++;
      if (System.currentTimeMillis() - receivedTime > StartLocalNameServer.maxQueryWaitTime) {
        // send error response to user and log error
        if (StartLocalNameServer.debugMode) {
          GNS.getLogger().fine("Query timeout exceeded. " + incomingPacket.getKey() + " " + incomingPacket.getGuid());
        }

        DNSRequestInfo query = LocalNameServer.removeDNSRequestInfo(queryId);
        if (query != null) {
          if (StartLocalNameServer.debugMode) {
            GNS.getLogger().fine("3.2.1 Query Info Removed." + query.getId());
          }
        } else {
          if (StartLocalNameServer.debugMode) {
            GNS.getLogger().fine("3.2.2 Query Info Does not exist.");
          }
        }
        if (query == null && queryId != 0) {
          // means query response received
        } else {
          returnErrorResponseToSender(incomingPacket, NSResponseCode.ERROR);
          logFailureMessage();
        }
        throw new CancelExecutorTaskException();
      }
      if (transmissionCount > 1) {
        if (queryId != 0 && LocalNameServer.containsDNSRequestInfo(queryId) == false) {
          if (StartLocalNameServer.debugMode) {
            GNS.getLogger().fine("Query ID not found. Response recvd or invalid "
                    + "active error. Query ID\t" + queryId + "\t" + transmissionCount + "\t" + nameserversQueried + "\t");
          }
          throw new CancelExecutorTaskException();
          //    	return;
        }
      }

      int ns = -1;
      if (StartLocalNameServer.replicateAll) {
        ns = BestServerSelection.getSmallestLatencyNS(ConfigFileInfo.getAllNameServerIDs(), nameserversQueried);
      } else {
        CacheEntry cacheEntry = LocalNameServer.getCacheEntry(incomingPacket.getGuid());

        if (cacheEntry != null) {

          ResultValue value = cacheEntry.getValue(incomingPacket.getKey());

          if (value != null) {
            if (transmissionCount > 1) {
              LocalNameServer.removeQueryInfo(queryId);
            }
            loggingForAddressInCache();
            sendCachedReplyToUser(value, cacheEntry.getTTL());

            throw new CancelExecutorTaskException();
//                  return;
          }
        }
        if (cacheEntry == null) {
          RequestActivesPacket pkt = new RequestActivesPacket(incomingPacket.getGuid(), LocalNameServer.nodeID);
          pkt.setActiveNameServers(HashFunction.getPrimaryReplicas(incomingPacket.getGuid()));
          cacheEntry = LocalNameServer.addCacheEntry(pkt);
        }

        if (cacheEntry == null || cacheEntry.isValidNameserver() == false) {
          GNS.getLogger().severe("Invalid name server for " + incomingPacket.getGuid());
          if (transmissionCount > 1) {
            LocalNameServer.removeQueryInfo(queryId);
          }

          DNSRequestTask queryTaskObject = new DNSRequestTask(
                  incomingPacket,
                  receivedTime,
                  lookupNumber,
                  0,
                  new HashSet<Integer>(), numRestarts + 1);

          PendingTasks.addToPendingRequests(incomingPacket.getGuid(),
                  queryTaskObject, StartLocalNameServer.queryTimeout,
                  getErrorPacket(incomingPacket), getFailureLogMessage(lookupNumber, incomingPacket.getKey(), incomingPacket.getGuid(), transmissionCount, receivedTime, numRestarts + 1, -1, nameserversQueried), 0);
          throw new CancelExecutorTaskException();
        }

        if (StartLocalNameServer.loadDependentRedirection) {
          ns = LocalNameServer.getBestActiveNameServerFromCache(incomingPacket.getGuid(), nameserversQueried);
        } else if (StartLocalNameServer.replicationFramework == ReplicationFrameworkType.BEEHIVE) {
          ns = LocalNameServer.getBeehiveNameServer(nameserversQueried, cacheEntry);
        } else {
          coordinatorID = LocalNameServer.getDefaultCoordinatorReplica(incomingPacket.getGuid(),
                  cacheEntry.getActiveNameServers());
          ns = BestServerSelection.getSmallestLatencyNS(cacheEntry.getActiveNameServers(), nameserversQueried);
        }
      }

      if (ns >= 0) {
        nameserversQueried.add(ns);
        //Save query information at the local name server to match response
        if (transmissionCount == 1) {
          //Get a unique id for this query
          queryId = LocalNameServer.addDNSRequestInfo(incomingPacket.getGuid(), incomingPacket.getKey(), ns,
                  receivedTime, "x", lookupNumber, incomingPacket, numRestarts);
        } else {
          DNSRequestInfo info = LocalNameServer.getDNSRequestInfo(queryId);
          if (info != null) {
            info.setNameserverID(ns);
          }
        }

        int clientQueryID = incomingPacket.getQueryId();
        // set this information in anticipation of creating the json object below
        incomingPacket.setSenderId(LocalNameServer.nodeID);
        incomingPacket.getHeader().setId(queryId);
        JSONObject json;
        try {
          json = incomingPacket.toJSONObjectQuestion();
          GNS.getLogger().fine(">>>>>>>>>>>>>Send to node = " + ns + "  DNS Request = " + json);
        } catch (JSONException e) {
          if (StartLocalNameServer.debugMode) {
            GNS.getLogger().fine("Error Converting Query to JSON Object.");
          }
          return;
        }
        // we're setting this back to it's original value here, right?
        // seems like a better solution is to have a separate field for the id for the LNS
        // and the client
        // using this for double duty is just asking for trouble
        incomingPacket.getHeader().setId(clientQueryID);

        LocalNameServer.sendToNS(json, ns);
      }
    } catch (Exception e) {
      if (e.getClass().equals(CancelExecutorTaskException.class)) {
        throw new RuntimeException();
      }
      GNS.getLogger().severe("Exception Exception Exception .... ");
      e.printStackTrace();
    }
  }

  /**
   * Send error response to users.
   *
   * @param dnsPacket
   * @param errorCode
   * @param address
   * @param port
   * @throws org.json.JSONException
   */
  private void returnErrorResponseToSender(DNSPacket dnsPacket, NSResponseCode errorCode) {

    dnsPacket.getHeader().setResponseCode(errorCode);
    dnsPacket.getHeader().setQRCode(DNSRecordType.RESPONSE);

    try {
      Intercessor.handleIncomingPackets(dnsPacket.toJSONObject());
      if (StartLocalNameServer.debugMode) {
        GNS.getLogger().fine("Error sent --> " + dnsPacket.toJSONObject().toString());
      }
    } catch (JSONException e) {
      GNS.getLogger().severe("Problem converting packet to JSON: " + e);
    }
  }

  // why does this totally ignore the error code that exists in the incoming packet?
  public static JSONObject getErrorPacket(DNSPacket dnsPacketIn) {
    try {
      DNSPacket dnsPacketOut = new DNSPacket(dnsPacketIn.toJSONObjectQuestion());
      dnsPacketOut.getHeader().setResponseCode(NSResponseCode.ERROR);
      dnsPacketOut.getHeader().setQRCode(DNSRecordType.RESPONSE);
      return dnsPacketOut.toJSONObject();
    } catch (JSONException e) {
      GNS.getLogger().severe("Problem converting packet to JSON: " + e);
    }
    return null;
  }

  /**
   * Log data for entries already in cache.
   */
  private void loggingForAddressInCache() {
    NameRecordKey nameRecordKey = incomingPacket.getKey();
    String name = incomingPacket.getGuid();
    if (StartLocalNameServer.debugMode) {
      GNS.getLogger().fine("Valid Address in cache... "
              + "Time:" + LocalNameServer.timeSinceAddressCached(name, nameRecordKey) + "ms");
    }

    DNSRequestInfo tempQueryInfo = new DNSRequestInfo(-1, incomingPacket.getGuid(), incomingPacket.getKey(),
            receivedTime, -1, "NA", lookupNumber, incomingPacket, numRestarts);
    tempQueryInfo.setRecvTime(System.currentTimeMillis());
    String stats = tempQueryInfo.getLookupStats();
    GNS.getStatLogger().info("Success-LookupRequest\t" + stats);
    if (GNS.getLogger().isLoggable(Level.FINER)) {
      if (StartLocalNameServer.debugMode) {
        GNS.getLogger().finer(LocalNameServer.cacheLogString("LNS CACHE: "));
      }
      if (StartLocalNameServer.debugMode) {
        GNS.getLogger().finer(LocalNameServer.nameRecordStatsMapLogString());
      }
    }
  }

  /**
   * Send DNS Query reply to User
   */
  private void sendCachedReplyToUser(ResultValue value, int TTL) {
    if (StartLocalNameServer.debugMode) {
      GNS.getLogger().fine("Send response from cache: " + incomingPacket.getGuid());
    }
    DNSPacket outgoingPacket = new DNSPacket(incomingPacket.getHeader().getId(), incomingPacket.getGuid(), incomingPacket.getKey(), value, TTL, new HashSet<Integer>());
    try {
      Intercessor.handleIncomingPackets(outgoingPacket.toJSONObject());
    } catch (JSONException e) {
      GNS.getLogger().severe("Problem converting packet to JSON: " + e);
    }
  }

  private void logFailureMessage() {
    GNS.getStatLogger().fine(getFailureLogMessage(lookupNumber, incomingPacket.getKey(),
            incomingPacket.getGuid(), transmissionCount, receivedTime, numRestarts, coordinatorID, nameserversQueried));
  }

  public static String getFailureLogMessage(int lookupNumber, NameRecordKey recordKey, String name,
          int transmissionCount, long receivedTime, int numRestarts,
          int coordinatorID, Set<Integer> nameserversQueried) {
    String failureCode = "Failed-LookupNoActiveResponse";
    if (nameserversQueried == null || nameserversQueried.isEmpty()) {
      failureCode = "Failed-LookupNoPrimaryResponse";
    }

    return (failureCode + "\t"
            + lookupNumber + "\t"
            + recordKey + "\t"
            + name + "\t"
            + transmissionCount + "\t"
            + receivedTime + "\t"
            + numRestarts + "\t"
            + coordinatorID + "\t"
            + nameserversQueried);
  }
}
