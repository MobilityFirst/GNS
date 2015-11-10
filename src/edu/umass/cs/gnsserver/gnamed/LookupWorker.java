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
 *  Initial developer(s): Westy, Emmanuel Cecchet
 *
 */
package edu.umass.cs.gnsserver.gnamed;

import edu.umass.cs.gnsserver.main.GNS;
import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.demultSupport.ClientRequestHandlerInterface;
import edu.umass.cs.utils.DelayProfiler;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.xbill.DNS.Flags;
import org.xbill.DNS.Message;
import org.xbill.DNS.Rcode;
import org.xbill.DNS.SimpleResolver;
import org.xbill.DNS.Cache;
import org.xbill.DNS.SetResponse;
import org.xbill.DNS.RRset;
import org.xbill.DNS.Section;
import org.xbill.DNS.Credibility;

/**
 * This class defines a LookupWorker which handles a single query.
 *
 * DNS requests can be handled just by the GNS server or by the GNS server
 * with a DNS server as a fallback.
 * When using DNS as a fallback we send out parallel requests and whichever returns
 * first is returned to the client as the answer.
 *
 * @author westy
 * @version 1.0
 */
public class LookupWorker implements Runnable {

  private final SimpleResolver dnsServer;
  private final SimpleResolver gnsServer;
  private final Cache dnsCache;
  private final DatagramSocket socket;
  private final DatagramPacket incomingPacket;
  private final byte[] incomingData;
  private final ClientRequestHandlerInterface handler;

  /**
   * Creates a new <code>LookupWorker</code> object which handles the parallel GNS and DNS requesting.
   *
   * @param socket
   * @param incomingPacket
   * @param incomingData
   * @param dnsServer (might be null meaning don't send requests to a DNS server)
   * @param gnsServer (might be null gns requests are resolved locally)
   * @param dnsCache (might be null meaning DNS responses are not cached)
   * @param handler
   */
  public LookupWorker(DatagramSocket socket, DatagramPacket incomingPacket, byte[] incomingData, SimpleResolver gnsServer,
          SimpleResolver dnsServer, Cache dnsCache, ClientRequestHandlerInterface handler) {
    this.socket = socket;
    this.incomingPacket = incomingPacket;
    this.incomingData = incomingData;
    this.dnsServer = dnsServer;
    this.dnsCache = dnsCache;
    this.gnsServer = gnsServer;
    this.handler = handler;
  }

  /**
   * @see java.lang.Thread#run()
   */
  @Override
  public void run() {
    long startTime = System.currentTimeMillis();
    Message query;
    Message response;
    int maxLength;

    // create a Message from the query data;
    try {
      query = new Message(incomingData);
    } catch (IOException e) {
      // Send out an error response.
      sendResponse(NameResolution.formErrorMessage(incomingData).toWire());
      return;
    }
    // THE MEAT IS IN HERE. Try to get a response from the GNS or DNS servers.
    response = generateReply(query);
    long postStart = System.currentTimeMillis();
    if (response == null) { // means we don't need to do anything
      return;
    }
    if (query.getOPT() != null) {
      maxLength = Math.max(query.getOPT().getPayloadSize(), 512);
    } else {
      maxLength = 512;
    }
    if (NameResolution.debuggingEnabled) {
      GNS.getLogger().info("Q/R: " + NameResolution.queryAndResponseToString(query, response));
    }
    // Send out the response.
    DelayProfiler.updateDelay("LookupWorker.postGenerate", postStart);
    long sendStart = System.currentTimeMillis();
    sendResponse(response.toWire(maxLength));
    DelayProfiler.updateDelay("LookupWorker.sendResponse", sendStart);
    DelayProfiler.updateDelay("LookupWorker", startTime);
  }

  /**
   * Queries DNS and/or GNS servers for DNS records.
   *
   * Note: a null return value means that the caller doesn't need to do
   * anything. Currently this only happens if this is an AXFR request over TCP.
   */
  private Message generateReply(Message query) {
    long startTime = System.currentTimeMillis();
    if (NameResolution.debuggingEnabled) {
      GNS.getLogger().fine("Incoming request: " + query.toString());
    }

    // If it's not a query we just ignore it.
    if (query.getHeader().getFlag(Flags.QR)) {
      return null;
    }

    long checkStart = System.currentTimeMillis();
    // Check for wierd queries we can't handle.
    Message errorMessage;
    if ((errorMessage = NameResolution.checkForErroneousQueries(query)) != null) {
      return errorMessage;
    }
    DelayProfiler.updateDelay("checkForErroneousQueries", checkStart);

    // If we're not consulting the DNS server as well just send the query to GNS.
    if (dnsServer == null) {
      Message result = NameResolution.lookupGnsServer(query, handler);
      DelayProfiler.updateDelay("generateReply", startTime);
      return result;
    }

    // Otherwise as a first step before performing GNS/DNS lookup we check our own local cache.
    if (dnsCache != null) {
      Message tempQuery = (Message) query.clone();
      Message result = NameResolution.lookupDnsCache(tempQuery, dnsCache);
      if (result.getHeader().getRcode() == Rcode.NOERROR) {
        GNS.getLogger().info("Responding the request from cache " + NameResolution.queryAndResponseToString(query, result));
        return result;
      }
    }

    // Create a clone of the query for duplicating the request to GNS and DNS
    Message dnsQuery = (Message) query.clone();
    List<GnsDnsLookupTask> tasks;
    if (gnsServer == null) {
      // We make two tasks to check the DNS and GNS in parallel
      tasks = Arrays.asList(
              // Create GNS lookup task
              new GnsDnsLookupTask(query, handler),
              // Create DNS lookup task
              new GnsDnsLookupTask(dnsQuery, dnsServer, handler));
    } else {
      tasks = Arrays.asList(
              // Create GNS lookup task
              new GnsDnsLookupTask(query, gnsServer, true, /* isGNS */ handler),
              // Create DNS lookup task
              new GnsDnsLookupTask(dnsQuery, dnsServer, false, /* isGNS */ handler));
    }

    // A little bit of overkill for two tasks, but it's really not that much longer (if any) than
    // the altenative. Plus it's cool and trendy to use futures.
    ExecutorService executor = Executors.newFixedThreadPool(2);
    ExecutorCompletionService<Message> completionService = new ExecutorCompletionService<Message>(executor);
    List<Future<Message>> futures = new ArrayList<Future<Message>>(2);
    for (Callable<Message> task : tasks) {
      futures.add(completionService.submit(task));
    }
    Message successResponse = null;
    Message errorResponse = null;
    // loop throught the tasks getting results as they complete
    for (GnsDnsLookupTask task : tasks) { // this is just doing things twice btw
      try {
        Message result = completionService.take().get();
        if (result.getHeader().getRcode() == Rcode.NOERROR) {
          successResponse = result;
          break;
        } else {
          // squirrel this away for later in case we get no successes
          errorResponse = result;
        }
      } catch (ExecutionException e) {
        if (NameResolution.debuggingEnabled) {
          GNS.getLogger().warning("Problem handling lookup task: " + e);
        }
      } catch (InterruptedException e) {
        if (NameResolution.debuggingEnabled) {
          GNS.getLogger().warning("Lookup task interrupted: " + e);
        }
      }
    }
    // Shutdown the executor threadpool
    executor.shutdown();
    if (successResponse != null) {
      // Cache the successful response
      try {
        SetResponse addMsgResponse = dnsCache.addMessage(successResponse);
        if (!addMsgResponse.isSuccessful()) {
          RRset[] answers = successResponse.getSectionRRsets(Section.ANSWER);
          boolean isAuth = successResponse.getHeader().getFlag(Flags.AA);
          int qClass = successResponse.getQuestion().getDClass();
          for (int i = 0; i < answers.length; i++) {
            if (answers[i].getDClass() != qClass) {
              continue;
            }
            int cred = getCred(Section.ANSWER, isAuth);
            dnsCache.addRRset(answers[i], cred);
            GNS.getLogger().info("Records added to cache " + answers[i].toString());
          }
        }
      } catch (NullPointerException e) {
        GNS.getLogger().warning("Failed to add a dns response to cache" + e);
      }
      return successResponse;
    } else if (errorResponse != null) {
      // currently this is returning the second error response... do we care?
      return errorResponse;
    } else {
      return NameResolution.errorMessage(query, Rcode.NXDOMAIN);
    }
  }

  /**
   * Returns a response to the sender.
   *
   * @param responseBytes
   */
  private void sendResponse(byte[] responseBytes) {
    DatagramPacket outgoingPacket = new DatagramPacket(responseBytes, responseBytes.length, incomingPacket.getAddress(), incomingPacket.getPort());
    try {
      socket.send(outgoingPacket);
      GNS.getLogger().fine("Response sent to " + incomingPacket.getAddress().toString() + " " + incomingPacket.getPort());
    } catch (IOException e) {
      GNS.getLogger().severe("Failed to send response" + e);
    }
  }

  private final int getCred(int section, boolean isAuth) {
    if (section == Section.ANSWER) {
      if (isAuth) {
        return Credibility.AUTH_ANSWER;
      } else {
        return Credibility.NONAUTH_ANSWER;
      }
    } else if (section == Section.AUTHORITY) {
      if (isAuth) {
        return Credibility.AUTH_AUTHORITY;
      } else {
        return Credibility.NONAUTH_AUTHORITY;
      }
    } else if (section == Section.ADDITIONAL) {
      return Credibility.ADDITIONAL;
    } else {
      throw new IllegalArgumentException("getCred: invalid section");
    }
  }
}
