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
  //
  private final boolean debuggingEnabled = true;

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
    // Send out the response.
    sendResponse(response.toWire(maxLength));
  }

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

  /**
   * Note: a null return value means that the caller doesn't need to do
   * anything. Currently this only happens if this is an AXFR request over TCP.
   */
  private Message generateReply(Message query) {
    if (debuggingEnabled) {
      GNS.getLogger().info("Incoming request: " + query.toString());
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
      return NameResolution.forwardToDnsServer(dnsServer, query);
    }

    // Otherwise we make two tasks to check the DNS and GNS in parallel
    List<NameResolutionWorker> tasks = Arrays.asList(
            new NameResolutionWorker(NameResolutionWorker.WorkerClass.DNS, query, dnsServer),
            new NameResolutionWorker(NameResolutionWorker.WorkerClass.GNS, query));

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
//  
//  public Message forwardToDnsServer(Message query) {
//    try {
//      Message dnsResponse = dnsServer.send(query);
//      if (debuggingEnabled) {
//        GNS.getLogger().info("DNS response " + Rcode.string(dnsResponse.getHeader().getRcode()) + " with "
//                + dnsResponse.getSectionArray(Section.ANSWER).length + " answer, "
//                + dnsResponse.getSectionArray(Section.AUTHORITY).length + " authoritative and "
//                + dnsResponse.getSectionArray(Section.ADDITIONAL).length + " additional records");
//      }
//      if (isReasonableResponse(dnsResponse)) {
//        if (debuggingEnabled) {
//          GNS.getLogger().info("Outgoing response from DNS: " + dnsResponse.toString());
//        }
//        return dnsResponse;
//      }
//    } catch (IOException e) {
//      GNS.getLogger().warning("DNS resolution failed for " + query + ": " + e);
//    }
//    return errorMessage(query, Rcode.NXDOMAIN);
//  }
//
//  public Message forwardToGnsServer(Message query) {
//    // check for queries we can't handle
//    int type = query.getQuestion().getType();
//    // Was the query legitimate or implemented?
//    if (!Type.isRR(type) && type != Type.ANY) {
//      return errorMessage(query, Rcode.NOTIMP);
//    }
//
//    // extract the domain (guid) and field from the query
//    final String fieldName = Type.string(query.getQuestion().getType());
//    final Name requestedName = query.getQuestion().getName();
//    final String domainName = requestedName.toString();
//    if (debuggingEnabled) {
//      GNS.getLogger().info("Trying GNS lookup for field " + fieldName + " in domain " + domainName);
//    }
//
//    String guid = AccountAccess.lookupGuid(domainName);
//
//    if (guid == null) {
//      if (debuggingEnabled) {
//        GNS.getLogger().info("GNS lookup: Domain " + domainName + " not found, returning NXDOMAIN result.");
//      }
//      return errorMessage(query, Rcode.NXDOMAIN);
//    }
//
//    CommandResponse fieldResponse = FieldAccess.lookup(guid, fieldName, null, null, null, null);
//    if (fieldResponse.isError()) {
//      if (debuggingEnabled) {
//        GNS.getLogger().info("GNS lookup: Field " + fieldName + " in domain " + domainName + " not found, returning NXDOMAIN result.");
//      }
//      return errorMessage(query, Rcode.NXDOMAIN);
//    }
//    final String ip = fieldResponse.getReturnValue();
//    if (debuggingEnabled) {
//      GNS.getLogger().info("Returning A Record with IP " + ip + " for " + requestedName);
//    }
//    // we'll need to change this to return other record types
//    ARecord gnsARecord;
//    try {
//      gnsARecord = new ARecord(requestedName, DClass.IN, 60, InetAddress.getByName(ip));
//    } catch (UnknownHostException e) {
//      return errorMessage(query, Rcode.NXDOMAIN);
//    }
//
//    Message response = new Message(query.getHeader().getID());
//    response.getHeader().setFlag(Flags.QR);
//    if (query.getHeader().getFlag(Flags.RD)) {
//      response.getHeader().setFlag(Flags.RA);
//    }
//    response.addRecord(query.getQuestion(), Section.QUESTION);
//    response.getHeader().setFlag(Flags.AA);
//
//    // Write the response
//    response.addRecord(gnsARecord, Section.ANSWER);
//    if (debuggingEnabled) {
//      GNS.getLogger().info("Outgoing response from GNS: " + response.toString());
//    }
//    return response;
//  }
//
//  /**
//   * Returns a Message with and error in it if the query is not good.
//   *
//   * @param query
//   * @return
//   */
//  private Message checkForErroneousQueries(Message query) {
//    Header header = query.getHeader();
//    // if there is an error we return an error
//    if (header.getRcode() != Rcode.NOERROR) {
//      return errorMessage(query, Rcode.FORMERR);
//    }
//    // we also don't support any weird operations
//    if (header.getOpcode() != Opcode.QUERY) {
//      return errorMessage(query, Rcode.NOTIMP);
//    }
//    return null;
//  }
//
//  /**
//   * Returns true if the response looks ok.
//   * Checks for errors and also 0 length answers.
//   *
//   * @param dnsResponse
//   * @return
//   */
//  private boolean isReasonableResponse(Message dnsResponse) {
//    Integer dnsRcode = null;
//    if (dnsResponse != null) {
//      dnsRcode = dnsResponse.getHeader().getRcode();
//    }
//    // If DNS resolution returned something useful return that
//    if (dnsRcode != null && dnsRcode == Rcode.NOERROR
//            // no error and some useful return value
//            && (dnsResponse.getSectionArray(Section.ANSWER).length > 0
//            || dnsResponse.getSectionArray(Section.AUTHORITY).length > 0)) {
//      // other things we could check for, but do we need to?
//      //( && dnsRcode != Rcode.NXDOMAIN && dnsRcode != Rcode.SERVFAIL)
//      return true;
//    } else {
//      return false;
//    }
//  }
//
//  private Message buildErrorMessage(Header header, int rcode, Record question) {
//    Message response = new Message();
//    response.setHeader(header);
//    for (int i = 0; i < 4; i++) {
//      response.removeAllRecords(i);
//    }
//    if (rcode == Rcode.SERVFAIL) {
//      response.addRecord(question, Section.QUESTION);
//    }
//    header.setRcode(rcode);
//    return response;
//  }
//
//  private Message formErrorMessage(byte[] in) {
//    Header header;
//    try {
//      header = new Header(in);
//    } catch (IOException e) {
//      return null;
//    }
//    return buildErrorMessage(header, Rcode.FORMERR, null);
//  }
//
//  private Message errorMessage(Message query, int rcode) {
//    return buildErrorMessage(query.getHeader(), rcode, query.getQuestion());
//  }
}
