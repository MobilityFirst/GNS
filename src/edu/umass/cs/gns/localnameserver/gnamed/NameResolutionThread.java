/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.localnameserver.gnamed;

import edu.umass.cs.gns.main.GNS;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.xbill.DNS.Flags;
import org.xbill.DNS.Message;
import org.xbill.DNS.Rcode;
import org.xbill.DNS.SimpleResolver;

//import edu.umass.cs.gns.client.UniversalGnsClient;
/**
 * This class defines a NameResolutionThread.
 *
 * @author <a href="mailto:manu@frogthinker.org">Emmanuel Cecchet</a>
 * @version 1.0
 */
public class NameResolutionThread extends Thread {

  private final SimpleResolver dnsServer;
  private final DatagramSocket socket;
  private final DatagramPacket incomingPacket;
  private final byte[] incomingData;

  /**
   * Creates a new <code>NameResolutionThread</code> object
   *
   * @param socket
   * @param incomingPacket
   * @param incomingData
   * @param dnsServer (might be null meaning don't send requests to a DNS server)
   */
  public NameResolutionThread(DatagramSocket socket, DatagramPacket incomingPacket, byte[] incomingData, SimpleResolver dnsServer) {
    this.socket = socket;
    this.incomingPacket = incomingPacket;
    this.incomingData = incomingData;
    this.dnsServer = dnsServer;
  }

  /**
   * @see java.lang.Thread#run()
   */
  @Override
  public void run() {
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
    // Try to get a response from the GNS or DNS servers.
    response = generateReply(query);
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
    sendResponse(response.toWire(maxLength));
  }

  /**
   * Queries DNS and/or GNS servers for DNS records.
   * 
   * Note: a null return value means that the caller doesn't need to do
   * anything. Currently this only happens if this is an AXFR request over TCP.
   */
  private Message generateReply(Message query) {
    if (NameResolution.debuggingEnabled) {
      GNS.getLogger().fine("Incoming request: " + query.toString());
    }

    // If it's not a query we just ignore it.
    if (query.getHeader().getFlag(Flags.QR)) {
      return null;
    }

    // Check for wierd queries we can't handle.
    Message errorMessage;
    if ((errorMessage = NameResolution.checkForErroneousQueries(query)) != null) {
      return errorMessage;
    }
    
    // If we're not consulting the DNS server as well just send the query to GNS.
    if (dnsServer == null) {
      return NameResolution.forwardToGnsServer(query);
    }

    // Otherwise we make two tasks to check the DNS and GNS in parallel
    List<NameResolutionWorker> tasks = Arrays.asList(
            new NameResolutionWorker(NameResolutionWorker.WorkerClass.DNS, query, dnsServer),
            new NameResolutionWorker(NameResolutionWorker.WorkerClass.GNS, query));

    // A little bit of overkill for two tasks, but it's really not that much longer (if any) than
    // the altenative. Plus it's cool and trendy to use futures.
    Executor executor = Executors.newFixedThreadPool(2);
    ExecutorCompletionService<Message> completionService = new ExecutorCompletionService<Message>(executor);
    List<Future<Message>> futures = new ArrayList<Future<Message>>(2);
    for (Callable<Message> task : tasks) {
      futures.add(completionService.submit(task));
    }
    Message successResponse = null;
    Message errorResponse = null;
    // loop throught the tasks getting results as they complete
    for (NameResolutionWorker task : tasks) { // this is just doing things twice btw
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

      } catch (InterruptedException e) {

      }
    }
    if (successResponse != null) {
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
      synchronized (socket) {
        socket.send(outgoingPacket);
      }
    } catch (IOException e) {
      GNS.getLogger().severe("Failed to send response" + e);
    }
  }

}
