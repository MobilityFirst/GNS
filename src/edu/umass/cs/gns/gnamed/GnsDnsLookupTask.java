/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.gnamed;

import edu.umass.cs.gns.newApp.clientCommandProcessor.EnhancedClientRequestHandlerInterface;
import edu.umass.cs.gns.newApp.clientCommandProcessor.demultSupport.ClientRequestHandlerInterface;
import java.util.concurrent.Callable;
import org.xbill.DNS.Message;
import org.xbill.DNS.SimpleResolver;

/**
 * Implements the callable that we use to implement parallel GNS and DNS lookups.
 * 
 * @author westy
 */
public class GnsDnsLookupTask implements Callable<Message> {

  enum WorkerClass {

    DNS, GNS, GNSLOCAL
  }

  private final WorkerClass workerClass;
  private final Message query;
  private Message response;
  private final SimpleResolver nameServer; // It is used for both dnsServer and gnsServer
  private EnhancedClientRequestHandlerInterface handler;

  /**
   * Creates a worker task that handles a query using the GNS.
   * 
   * @param query 
   */
  GnsDnsLookupTask(Message query, EnhancedClientRequestHandlerInterface handler) {
    this.workerClass = WorkerClass.GNSLOCAL;
    this.query = query;
    this.nameServer = null;
    this.handler = handler;
  }
  
  /**
   * Creates a worker task that handles a query using the DNS
   * 
   * @param query 
   * @param dnsServer
   */
  GnsDnsLookupTask(Message query, SimpleResolver dnsServer, EnhancedClientRequestHandlerInterface handler) {
    this.workerClass = WorkerClass.DNS;
    this.query = query;
    this.nameServer = dnsServer;
    this.handler = handler;
  }

  /**
   * Creates a worker task that handles a query using the DNS
   *
   * @param query
   * @param nameServer
   */
  GnsDnsLookupTask(Message query, SimpleResolver nameServer, Boolean isGNS, EnhancedClientRequestHandlerInterface handler) {
    if (isGNS) {
      this.workerClass = WorkerClass.GNS;
    } else {
      this.workerClass = WorkerClass.DNS;
    }
    this.query = query;
    this.nameServer = nameServer;
    this.handler = handler;
  }

  @Override
  public Message call() {
    switch (workerClass) {
      case DNS:
        response = NameResolution.forwardToDnsServer(nameServer, query);
        break;
      case GNS:
        response = NameResolution.forwardToGnsServer(nameServer, query);
        break;
      case GNSLOCAL:
        response = NameResolution.lookupGnsServer(query, handler);
        break;
    }
    return response;
  }

  public Message getResponse() {
    return response;
  }
}
