/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.localnameserver;


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
import edu.umass.cs.gns.util.BestServerSelection;
import java.util.HashSet;
import java.util.TimerTask;
import java.util.logging.Level;
import org.json.JSONException;
import org.json.JSONObject;


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
 * @see edu.umass.cs.gns.packet.DNSPacket
 *
 * User: abhigyan
 * Date: 8/30/13
 * Time: 3:33 PM
 */
public class DNSRequestTask extends TimerTask {

  DNSPacket incomingPacket;
  long receivedTime; // overall latency
  private int transmissionCount = 0;
  private int lookupNumber;
  private int queryId = 0;
  private int numInvalidActiveError;
  private HashSet<Integer> nameserversQueried;
  private int coordinatorID = -1;

  public DNSRequestTask(DNSPacket incomingPacket,
                        long receivedTime,
                        int lookupNumber, int queryId,
                        HashSet<Integer> nameserversQueried, int numInvalidActiveError) {
    this.incomingPacket = incomingPacket;
    this.receivedTime = receivedTime;
    this.lookupNumber = lookupNumber;
    this.nameserversQueried = nameserversQueried;
    this.numInvalidActiveError = numInvalidActiveError;
  }

  @Override
  public void run() {
    try {
      transmissionCount++;

      if (isMaxWaitTimeExceeded() || isResponseReceived()) {
        throw new CancelExecutorTaskException();
      }

      CacheEntry cacheEntry = LocalNameServer.getCacheEntry(incomingPacket.getGuid());

      if(replySentFromCache(cacheEntry)) {
        throw new CancelExecutorTaskException();
      }

      if (cacheEntry == null || cacheEntry.isValidNameserver() == false) {
        requestNewActives();
        throw new CancelExecutorTaskException();
      }

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

  private boolean isResponseReceived() {
    if (transmissionCount > 1) {
      if (queryId != 0 && LocalNameServer.containsDNSRequestInfo(queryId) == false) {
        if (StartLocalNameServer.debugMode) {
          GNS.getLogger().fine("Query ID not found. Response recvd or invalid "
                  + "active error. Query ID\t" + queryId + "\t" + transmissionCount + "\t" + nameserversQueried + "\t");
        }
        return true;
      }
    }
    return false;
  }

  private boolean isMaxWaitTimeExceeded() {
    if (System.currentTimeMillis() - receivedTime > StartLocalNameServer.maxQueryWaitTime) {
      // send error response to user and log error
      if (StartLocalNameServer.debugMode) {
        GNS.getLogger().fine("Query max wait time exceeded. " + incomingPacket.getKey() + " " + incomingPacket.getGuid());
      }

      DNSRequestInfo query = LocalNameServer.removeDNSRequestInfo(queryId);
      if (queryId == 0 || query != null) {
        returnErrorResponseToSender(incomingPacket, NSResponseCode.ERROR);
        logFailureMessage();
      }
      return true;
    }
    return false;
  }


  private boolean replySentFromCache(CacheEntry cacheEntry) {
    if (cacheEntry != null) {
//          boolean isReplySent = sendCachedReplyToUser();
      ResultValue value = cacheEntry.getValue(incomingPacket.getKey());

      if (value != null) {
        if (transmissionCount > 1) {
          LocalNameServer.removeDNSRequestInfo(queryId);
        }
        loggingForAddressInCache();
        sendCachedReplyToUser(value, cacheEntry.getTTL());
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
    if (StartLocalNameServer.debugMode) {
      GNS.getLogger().fine("Valid Address in cache... "
              + "Time:" + LocalNameServer.timeSinceAddressCached(name, nameRecordKey) + "ms");
    }

    DNSRequestInfo tempQueryInfo = new DNSRequestInfo(-1, incomingPacket.getGuid(), incomingPacket.getKey(),
            receivedTime, -1, "NA", lookupNumber, incomingPacket, numInvalidActiveError);
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

  private void requestNewActives() {
    GNS.getLogger().fine("Invalid name server for " + incomingPacket.getGuid());
    if (transmissionCount > 1) {
      LocalNameServer.removeDNSRequestInfo(queryId);
    }

    boolean firstInvalidActiveError = (numInvalidActiveError == 0);
    DNSRequestTask queryTaskObject = new DNSRequestTask(
            incomingPacket, receivedTime, lookupNumber, 0, new HashSet<Integer>(), numInvalidActiveError + 1);

    String getFailureLogMessage = DNSRequestInfo.getFailureLogMessage(lookupNumber, incomingPacket.getKey(),
            incomingPacket.getGuid(), transmissionCount, receivedTime, numInvalidActiveError + 1, -1, nameserversQueried);
    PendingTasks.addToPendingRequests(incomingPacket.getGuid(), queryTaskObject,
            StartLocalNameServer.queryTimeout,  Lookup.getErrorPacket(incomingPacket), getFailureLogMessage,
            firstInvalidActiveError);
  }

  private int selectNS(CacheEntry cacheEntry) {
    int ns;
    if (StartLocalNameServer.loadDependentRedirection) {
      ns = BestServerSelection.getBestActiveNameServerFromCache(cacheEntry, nameserversQueried);
    } else if (StartLocalNameServer.replicationFramework == ReplicationFrameworkType.BEEHIVE) {
      ns = BestServerSelection.getBeehiveNameServer(nameserversQueried, cacheEntry);
    } else {
      coordinatorID = LocalNameServer.getDefaultCoordinatorReplica(incomingPacket.getGuid(),
              cacheEntry.getActiveNameServers());
      ns = BestServerSelection.getSmallestLatencyNS(cacheEntry.getActiveNameServers(), nameserversQueried);
    }
    return ns;
  }

  private void sendLookupToNS(int ns) {
    if (ns >= 0) {
      nameserversQueried.add(ns);
      //Save query information at the local name server to match response
      if (transmissionCount == 1) {
        //Get a unique id for this query
        queryId = LocalNameServer.addDNSRequestInfo(incomingPacket.getGuid(), incomingPacket.getKey(), ns,
                receivedTime, "x", lookupNumber, incomingPacket, numInvalidActiveError);
      } else {
        DNSRequestInfo info = LocalNameServer.getDNSRequestInfo(queryId);
        if (info != null) {
          info.setNameserverID(ns);
        }
      }

      int clientQueryID = incomingPacket.getQueryId();
      // set this information in anticipation of creating the json object below
      incomingPacket.setSenderId(LocalNameServer.getNodeID());
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

}
