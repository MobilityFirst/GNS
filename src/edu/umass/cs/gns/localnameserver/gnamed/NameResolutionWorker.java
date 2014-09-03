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
 * Implements a callable that we can use to implement parallel GNS and DNS lookups.
 * 
 * @author westy
 */
public class NameResolutionWorker implements Callable {

  enum WorkerClass {

    DNS, GNS
  }

  private final WorkerClass workerClass;
  private final Message query;
  private Message response;
  private SimpleResolver dnsServer; // will be null for GNS worker

  NameResolutionWorker(WorkerClass workerClass, Message query) {
    this.workerClass = workerClass;
    this.query = query;
    this.dnsServer = null;
  }
  
  NameResolutionWorker(WorkerClass workerClass, Message query, SimpleResolver dnsServer) {
    this.workerClass = workerClass;
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
