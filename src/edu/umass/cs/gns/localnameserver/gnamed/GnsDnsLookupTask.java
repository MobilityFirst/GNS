/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.localnameserver.gnamed;

import java.util.concurrent.Callable;
import org.xbill.DNS.Message;
import org.xbill.DNS.SimpleResolver;

/**
 * Implements the callable that we use to implement parallel GNS and DNS lookups.
 * 
 * @author westy
 */
public class GnsDnsLookupTask implements Callable {

  enum WorkerClass {

    DNS, GNS
  }

  private final WorkerClass workerClass;
  private final Message query;
  private Message response;
  private final SimpleResolver dnsServer; // will be null for GNS worker

  /**
   * Creates a worker task that handles a query using the GNS.
   * 
   * @param query 
   */
  GnsDnsLookupTask(Message query) {
    this.workerClass = WorkerClass.GNS;
    this.query = query;
    this.dnsServer = null;
  }
  
  /**
   * Creates a worker task that handles a query using the DNS
   * 
   * @param query 
   * @param dnsServer
   */
  GnsDnsLookupTask(Message query, SimpleResolver dnsServer) {
    this.workerClass = WorkerClass.DNS;
    this.query = query;
    this.dnsServer = dnsServer;
  }

  @Override
  public Message call() {
    switch (workerClass) {
      case DNS:
        response = NameResolution.forwardToDnsServer(dnsServer, query);
        break;
      case GNS:
        response = NameResolution.forwardToGnsServer(query);
        break;
    }
    return response;
  }

  public Message getResponse() {
    return response;
  }
  
}
