/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.localnameserver;

import edu.umass.cs.gns.exceptions.CancelExecutorTaskException;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nsdesign.packet.DNSPacket;
import edu.umass.cs.gns.nsdesign.packet.DNSRecordType;
import edu.umass.cs.gns.nsdesign.replicationframework.RandomReplication;
import edu.umass.cs.gns.nsdesign.replicationframework.ReplicationFrameworkType;
import edu.umass.cs.gns.util.NSResponseCode;
import edu.umass.cs.gns.util.NameRecordKey;
import edu.umass.cs.gns.util.ResultValue;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.HashSet;
import java.util.TimerTask;
import java.util.logging.Level;

/**
 * Handles a lookup request from client, and either sends response to client from cache or sends a lookup to
 * an active replica. This is a timer task which is executed repeatedly until either of these cases happen:
 * (1) a response is sent to client from the local name server's cache.
 * (2) name server responds to lookup request.
 * (3) max wait time for a request is exceeded, in which case, we send error message to client
 * (4) local name server's cache does not have active replicas for a name. In this case, we start the process
 * of obtaining current set of actives for the name.
 *
 *
 * @see edu.umass.cs.gns.localnameserver.Lookup
 * @see edu.umass.cs.gns.localnameserver.DNSRequestInfo
 * @see edu.umass.cs.gns.nsdesign.packet.DNSPacket
 *
 * User: abhigyan
 * Date: 8/30/13
 * Time: 3:33 PM
 */
public class DNSRequestTask extends TimerTask {

  ClientRequestHandlerInterface handler;
  DNSPacket incomingPacket;
  long receivedTime; // overall latency
  private int transmissionCount = 0;
  private final int lookupNumber;
  private int queryId = 0;
  private final int numInvalidActiveError;
  private final HashSet<Integer> nameserversQueried;
  private int coordinatorID = -1; // it would be nice to know what the purpose of this is!

  public DNSRequestTask(ClientRequestHandlerInterface handler,
          DNSPacket incomingPacket,
          long receivedTime,
          int lookupNumber, int queryId,
          HashSet<Integer> nameserversQueried, int numInvalidActiveError) {
    this.handler = handler;
    this.incomingPacket = incomingPacket;
    this.receivedTime = receivedTime;
    this.lookupNumber = lookupNumber;
    this.nameserversQueried = nameserversQueried;
    this.numInvalidActiveError = numInvalidActiveError;
  }

  @Override
  // Pretty much the same code as in SendUpdatesTask
  public void run() {
    try {
      transmissionCount++;

      if (isMaxWaitTimeExceeded(handler) || isResponseReceived(handler)) {
        throw new CancelExecutorTaskException();
      }

      CacheEntry cacheEntry = handler.getCacheEntry(incomingPacket.getGuid());
      // if a non-expired value exists in the cache send that and we are done
      if (maybeSendReplyFromCache(cacheEntry, handler)) {
        throw new CancelExecutorTaskException();
      }

      // IF we don't have one or more valid active replicas in the cache entry
      // we need to request a new set for this name.
      if (cacheEntry == null || cacheEntry.isValidNameserver() == false) {
        requestNewActives();
        // Cancel the task now. 
        // When the request is satisfied this current task will be rescheduled.
        throw new CancelExecutorTaskException();
      }

      // the cache containts a set of valid active replicas
      int ns = selectNS(cacheEntry);

      sendLookupToNS(ns);

    } catch (Exception e) {
      if (e.getClass().equals(CancelExecutorTaskException.class)) {
        throw new RuntimeException();
      }
      GNS.getLogger().severe("Exception Exception Exception .... ");
      e.printStackTrace();
    }
  }

  private boolean isResponseReceived(ClientRequestHandlerInterface handler) {
    if (transmissionCount > 1) {
      if (queryId != 0 && handler.containsDNSRequestInfo(queryId) == false) {
        if (handler.getParameters().isDebugMode()) {
          GNS.getLogger().fine("Query ID not found. Response recvd or invalid "
                  + "active error. Query ID\t" + queryId + "\t" + transmissionCount + "\t" + nameserversQueried + "\t");
        }
        return true;
      }
    }
    return false;
  }

  private boolean isMaxWaitTimeExceeded(ClientRequestHandlerInterface handler) {
    if (System.currentTimeMillis() - receivedTime > handler.getParameters().getMaxQueryWaitTime()) {
      // send error response to user and log error
      if (handler.getParameters().isDebugMode()) {
        GNS.getLogger().fine("Query max wait time exceeded. " + incomingPacket.getKey() + " " + incomingPacket.getGuid()
                + "Wait time: " + (System.currentTimeMillis() - receivedTime) + " Max wait: " + handler.getParameters().getMaxQueryWaitTime());
      }

      DNSRequestInfo query = handler.removeDNSRequestInfo(queryId);
      if (queryId == 0 || query != null) {
        returnErrorResponseToSender(incomingPacket, NSResponseCode.ERROR, handler);
        logFailureMessage();
      }
      return true;
    }
    return false;
  }

  private boolean maybeSendReplyFromCache(CacheEntry cacheEntry, ClientRequestHandlerInterface handler) {
    if (cacheEntry != null) {
      ResultValue value = cacheEntry.getValue(incomingPacket.getKey());

      if (value != null) {
        if (transmissionCount > 1) {
          handler.removeDNSRequestInfo(queryId);
        }
        loggingForAddressInCache();
        sendCachedReplyToUser(value, cacheEntry.getTTL(), handler);
        return true;
      }
    }
    return false;
  }

  /**
   * Log data for entries already in cache.
   */
  private void loggingForAddressInCache() {
    NameRecordKey nameRecordKey = incomingPacket.getKey();
    String name = incomingPacket.getGuid();
    if (handler.getParameters().isDebugMode()) {
      GNS.getLogger().fine("Valid Address in cache... "
              + "Time:" + handler.timeSinceAddressCached(name, nameRecordKey) + "ms");
    }

    DNSRequestInfo tempQueryInfo = new DNSRequestInfo(-1, incomingPacket.getGuid(), incomingPacket.getKey(),
            receivedTime, -1, "NA", lookupNumber, incomingPacket, numInvalidActiveError);
    tempQueryInfo.setRecvTime(System.currentTimeMillis());
    String stats = tempQueryInfo.getLookupStats();
    GNS.getStatLogger().info("Success-Lookup-CacheHit\t" + stats);
    if (GNS.getLogger().isLoggable(Level.FINER)) {
      if (handler.getParameters().isDebugMode()) {
        GNS.getLogger().finer(handler.getCacheLogString("LNS CACHE: "));
      }
      if (handler.getParameters().isDebugMode()) {
        GNS.getLogger().finer(handler.getNameRecordStatsMapLogString());
      }
    }
  }

  /**
   * Send DNS Query reply that we found in the cache back to the User
   */
  private void sendCachedReplyToUser(ResultValue value, int TTL, ClientRequestHandlerInterface handler) {
    if (handler.getParameters().isDebugMode()) {
      GNS.getLogger().fine("Send response from cache: " + incomingPacket.getGuid());
    }
    DNSPacket outgoingPacket = new DNSPacket(incomingPacket.getSourceId(), incomingPacket.getHeader().getId(), incomingPacket.getGuid(), incomingPacket.getKey(), value, TTL, new HashSet<Integer>());
    try {
      Lookup.sendDNSResponseBackToSource(outgoingPacket, handler);
    } catch (JSONException e) {
      GNS.getLogger().severe("Problem converting packet to JSON: " + e);
    }
  }

  private void requestNewActives() {
    if (handler.getParameters().isDebugMode()) {
      GNS.getLogger().fine("Invalid name server for " + incomingPacket.getGuid());
    }
    if (transmissionCount > 1) {
      handler.removeDNSRequestInfo(queryId);
    }

    boolean firstInvalidActiveError = (numInvalidActiveError == 0);
    DNSRequestTask queryTaskObject = new DNSRequestTask(handler,
            incomingPacket, receivedTime, lookupNumber, 0, new HashSet<Integer>(), numInvalidActiveError + 1);

    String getFailureLogMessage = DNSRequestInfo.getFailureLogMessage(lookupNumber, incomingPacket.getKey(),
            incomingPacket.getGuid(), transmissionCount, receivedTime, numInvalidActiveError + 1, -1, nameserversQueried);
    PendingTasks.addToPendingRequests(incomingPacket.getGuid(), queryTaskObject,
            handler.getParameters().getQueryTimeout(), Lookup.getErrorPacket(incomingPacket), getFailureLogMessage,
            firstInvalidActiveError);
  }

  private int selectNS(CacheEntry cacheEntry) {
    int ns;
    if (handler.getParameters().getReplicationFramework() == ReplicationFrameworkType.BEEHIVE) {
      ns = RandomReplication.getBeehiveNameServer(handler.getGnsNodeConfig(), cacheEntry.getActiveNameServers(),
              nameserversQueried);
    } else {
      coordinatorID = handler.getDefaultCoordinatorReplica(incomingPacket.getGuid(),
              cacheEntry.getActiveNameServers());
      ns = handler.getGnsNodeConfig().getClosestServer(cacheEntry.getActiveNameServers(), nameserversQueried);
    }
    return ns;
  }

  private void sendLookupToNS(int ns) {
    if (ns >= 0) {
      nameserversQueried.add(ns);
      //Save query information at the local name server to match response
      if (transmissionCount == 1) {
        //Get a unique id for this query
        queryId = handler.addDNSRequestInfo(incomingPacket.getGuid(), incomingPacket.getKey(), ns,
                receivedTime, "x", lookupNumber, incomingPacket, numInvalidActiveError);
      } else {
        DNSRequestInfo info = handler.getDNSRequestInfo(queryId);
        if (info != null) {
          info.setNameserverID(ns);
        }
      }

      int clientQueryID = incomingPacket.getQueryId();
      // set this information in anticipation of creating the json object below
      incomingPacket.setLnsId(handler.getNodeID());
      incomingPacket.getHeader().setId(queryId);
      JSONObject json;
      try {
        json = incomingPacket.toJSONObjectQuestion();
        if (handler.getParameters().isDebugMode()) {
          GNS.getLogger().fine(">>>>>>>>>>>>>Send to node = " + ns + "  DNS Request = " + json);
        }
      } catch (JSONException e) {
        if (handler.getParameters().isDebugMode()) {
          GNS.getLogger().fine("Error Converting Query to JSON Object.");
        }
        return;
      }
      // we're setting this back to it's original value here, right?
      // seems like a better solution is to have a separate field for the id for the LNS
      // and the client
      // using this for double duty is just asking for trouble
      incomingPacket.getHeader().setId(clientQueryID);

      handler.sendToNS(json, ns);
    }
  }

  private void logFailureMessage() {
    GNS.getStatLogger().fine(DNSRequestInfo.getFailureLogMessage(lookupNumber, incomingPacket.getKey(),
            incomingPacket.getGuid(), transmissionCount, receivedTime, numInvalidActiveError, coordinatorID, nameserversQueried));
  }

  /**
   * Send error response to users.
   *
   * @param dnsPacket
   * @param errorCode
   */
  private void returnErrorResponseToSender(DNSPacket dnsPacket, NSResponseCode errorCode, ClientRequestHandlerInterface handler) {

    dnsPacket.getHeader().setResponseCode(errorCode);
    dnsPacket.getHeader().setQRCode(DNSRecordType.RESPONSE);

    try {
      Lookup.sendDNSResponseBackToSource(dnsPacket, handler);
      //Intercessor.handleIncomingPackets(dnsPacket.toJSONObject());
      if (handler.getParameters().isDebugMode()) {
        GNS.getLogger().fine("Error sent --> " + dnsPacket.toJSONObject().toString());
      }

    } catch (JSONException e) {
      GNS.getLogger().severe("Problem converting packet to JSON: " + e);
    }
  }

}
