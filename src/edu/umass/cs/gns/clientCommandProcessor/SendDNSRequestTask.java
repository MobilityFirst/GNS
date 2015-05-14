/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.clientCommandProcessor;

import edu.umass.cs.gns.exceptions.CancelExecutorTaskException;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nsdesign.Config;
import edu.umass.cs.gns.nsdesign.packet.DNSPacket;
import edu.umass.cs.gns.nsdesign.replicationframework.BeehiveReplication;
import edu.umass.cs.gns.nsdesign.replicationframework.ReplicationFrameworkType;
import edu.umass.cs.gns.util.ResultValue;
import edu.umass.cs.gns.util.Util;
import org.json.JSONException;
import org.json.JSONObject;
import java.util.HashSet;
import java.util.Set;
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
 * @param <NodeIDType>
 * @see edu.umass.cs.gns.clientCommandProcessor.Lookup
 * @see edu.umass.cs.gns.clientCommandProcessor.DNSRequestInfo
 * @see edu.umass.cs.gns.nsdesign.packet.DNSPacket
 *
 * User: abhigyan
 * Date: 8/30/13
 * Time: 3:33 PM
 */
public class SendDNSRequestTask<NodeIDType> extends TimerTask {

  private final ClientRequestHandlerInterface<NodeIDType> handler;
  private final DNSPacket<NodeIDType> incomingPacket;
  private final int lnsReqID;

  private final HashSet<NodeIDType> nameserversQueried = new HashSet<>();

  private int timeoutCount = -1;

  private int requestActivesCount = -1;

  public SendDNSRequestTask(int lnsReqID, ClientRequestHandlerInterface<NodeIDType> handler,
          DNSPacket<NodeIDType> incomingPacket) {
    this.lnsReqID = lnsReqID;
    this.handler = handler;
    this.incomingPacket = incomingPacket;
  }

  @Override
  // Pretty much the same code as in SendUpdatesTask
  public void run() {
    try {
      timeoutCount++;
      if (handler.getParameters().isDebugMode()) {
        GNS.getLogger().info("ENTER name = " + incomingPacket.getGuid() + "/" + incomingPacket.getKey() + " timeout = " + timeoutCount);
      }

      if (isMaxWaitTimeExceeded(handler) || isResponseReceived(handler)) {
        throw new CancelExecutorTaskException();
      }

      CacheEntry<NodeIDType> cacheEntry = handler.getCacheEntry(incomingPacket.getGuid());
      // if a non-expired value exists in the cache send that and we are done
      if (maybeSendReplyFromCache(cacheEntry, handler)) {
        throw new CancelExecutorTaskException();
      }

      // If we don't have one or more valid active replicas in the cache entry
      // we need to request a new set for this name.
      if (cacheEntry == null || !cacheEntry.isValidNameserver()) {
        if (handler.getParameters().isDebugMode()) {
          GNS.getLogger().info("Requesting new actives for " + incomingPacket.getGuid());
        }
        requestNewActives(handler);
        // Cancel the task now. 
        // When the new actives are received, a new task in place of this task will be rescheduled.
        throw new CancelExecutorTaskException();
      }

      // the cache contains a set of valid active replicas
      NodeIDType ns = selectNS(cacheEntry.getActiveNameServers());

      sendLookupToNS(ns);

    } catch (Exception e) {
      if (e.getClass().equals(CancelExecutorTaskException.class)) {
        throw new RuntimeException();
      }
      GNS.getLogger().severe("Problem sending a DNS request: " + e);
      e.printStackTrace();
    }
  }

  private boolean isResponseReceived(ClientRequestHandlerInterface<NodeIDType> handler) {
    DNSRequestInfo<NodeIDType> info = (DNSRequestInfo) handler.getRequestInfo(lnsReqID);
    if (info == null) {
      if (handler.getParameters().isDebugMode()) {
        GNS.getLogger().fine("Query ID. Response recvd "
                + ". Query ID\t" + lnsReqID + "\t" + timeoutCount + "\t" + Util.setOfNodeIdToString(nameserversQueried) + "\t");
      }
      return true;
    } else if (requestActivesCount == -1) {
      requestActivesCount = info.getNumLookupActives();
    } else if (requestActivesCount != info.getNumLookupActives()) { //
      // invalid active response received in this case
      if (handler.getParameters().isDebugMode()) {
        GNS.getLogger().fine("Invalid active response received. Cancel task. " + lnsReqID + "\t" + incomingPacket
                + " Request actives count: " + requestActivesCount + " Request actives count: " + info.getNumLookupActives());
      }
      return true;
    }
    return false;
  }

  private boolean isMaxWaitTimeExceeded(ClientRequestHandlerInterface<NodeIDType> handler) throws JSONException {
    DNSRequestInfo<NodeIDType> requestInfo = (DNSRequestInfo) handler.getRequestInfo(lnsReqID);
    if (requestInfo != null) {
      if (System.currentTimeMillis() - requestInfo.getStartTime() > handler.getParameters().getMaxQueryWaitTime()) {
        // remove from request info as LNS must clear all state for this request
        requestInfo = (DNSRequestInfo) handler.removeRequestInfo(lnsReqID);
        if (requestInfo != null) {
          // send error response to user and log error
          if (handler.getParameters().isDebugMode()) {
            GNS.getLogger().info("Query max wait time exceeded. " + incomingPacket.getGuid() + "/" + incomingPacket.getKey()
                    + "Wait time: " + (System.currentTimeMillis() - requestInfo.getStartTime())
                    + " Max wait: " + handler.getParameters().getMaxQueryWaitTime());
          }
          Lookup.sendDNSResponseBackToSource(new DNSPacket<NodeIDType>(requestInfo.getErrorMessage(), handler.getGnsNodeConfig()), handler);
          requestInfo.setSuccess(false);
          requestInfo.setFinishTime();
          //requestInfo.addEventCode(LNSEventCode.MAX_WAIT_ERROR);
          //GNS.getStatLogger().fine(requestInfo.getLogString());
          return true;
        }
      }
    }
    return false;
  }

  private boolean maybeSendReplyFromCache(CacheEntry cacheEntry, ClientRequestHandlerInterface<NodeIDType> handler) {
    if (cacheEntry != null && incomingPacket.getKey() != null) { // key can be null if request is multi-field
      ResultValue value = cacheEntry.getValueAsArray(incomingPacket.getKey());
      if (value != null) {
        DNSRequestInfo info = (DNSRequestInfo) handler.removeRequestInfo(lnsReqID);
        if (info != null) {
          loggingForAddressInCache(info);
          GNS.getLogger().info("Replying from cache " + incomingPacket.getGuid() + "/" + incomingPacket.getKey());
          sendCachedReplyToUser(value, cacheEntry.getTTL(), handler);

        }
        return true;
      }
    }
    return false;
  }

  /**
   * Log data for entries already in cache.
   */
  private void loggingForAddressInCache(DNSRequestInfo info) {
    String nameRecordKey = incomingPacket.getKey();
    String name = incomingPacket.getGuid();
    if (handler.getParameters().isDebugMode()) {
      GNS.getLogger().fine("Valid Address in cache... "
              + "Time:" + handler.timeSinceAddressCached(name, nameRecordKey) + "ms");
    }
    info.setFinishTime();
    info.setSuccess(true);
    info.setCacheHit(true);
    //info.addEventCode(LNSEventCode.CACHE_HIT);
    //GNS.getStatLogger().info(info.getLogString());
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
  private void sendCachedReplyToUser(ResultValue value, int TTL, ClientRequestHandlerInterface<NodeIDType> handler) {
    if (handler.getParameters().isDebugMode()) {
      GNS.getLogger().fine("Send response from cache: " + incomingPacket.getGuid());
    }
    DNSPacket<NodeIDType> outgoingPacket = new DNSPacket<NodeIDType>(incomingPacket.getSourceId(), incomingPacket.getHeader().getId(),
            incomingPacket.getGuid(), incomingPacket.getKey(), value, TTL, new HashSet<Integer>());
    try {
      Lookup.sendDNSResponseBackToSource(outgoingPacket, handler);
    } catch (JSONException e) {
      GNS.getLogger().severe("Problem converting packet to JSON: " + e);
    }
  }

  private void requestNewActives(ClientRequestHandlerInterface<NodeIDType> handler) {
    if (handler.getParameters().isDebugMode()) {
      GNS.getLogger().info("Invalid name server for " + incomingPacket.getGuid());
    }
    SendDNSRequestTask queryTaskObject = new SendDNSRequestTask<NodeIDType>(lnsReqID, handler, incomingPacket);
    PendingTasks.addToPendingRequests(handler.getRequestInfo(lnsReqID), queryTaskObject,
            handler.getParameters().getQueryTimeout(), handler);
  }

  private NodeIDType selectNS(Set<NodeIDType> replicas) {
    NodeIDType ns;
    if (handler.getParameters().getReplicationFramework() == ReplicationFrameworkType.BEEHIVE) {
      ns = (NodeIDType) BeehiveReplication.getBeehiveNameServer(handler.getGnsNodeConfig(), (Set) replicas,
              nameserversQueried);
    } else {
      ns = (NodeIDType) handler.getGnsNodeConfig().getClosestServer(replicas, nameserversQueried);
    }
    return ns;
  }

  private void sendLookupToNS(NodeIDType ns) {
    if (ns != null) {
      nameserversQueried.add(ns);

      DNSRequestInfo reqInfo = (DNSRequestInfo) handler.getRequestInfo(lnsReqID);
//      if (reqInfo != null) {
//        reqInfo.addEventCode(LNSEventCode.CONTACT_ACTIVE);
//      }
      int clientQueryID = incomingPacket.getQueryId();

      // set this information in anticipation of creating the json object below
      //incomingPacket.setLnsId(handler.getNodeID());
      incomingPacket.setLnsAddress(handler.getNodeAddress());
      incomingPacket.getHeader().setId(lnsReqID);
      JSONObject json;
      try {
        json = incomingPacket.toJSONObjectQuestion();
        if (handler.getParameters().isDebugMode()) {
          GNS.getLogger().fine(">>>>>>>>>>>>>Send to node = " + ns.toString() + "  DNS Request = " + json);
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

}
