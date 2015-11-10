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

import edu.umass.cs.gnsserver.gnsApp.clientCommandProcessor.demultSupport.ClientRequestHandlerInterface;
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
  private ClientRequestHandlerInterface handler;

  /**
   * Creates a worker task that handles a query using the GNS.
   * 
   * @param query 
   */
  GnsDnsLookupTask(Message query, ClientRequestHandlerInterface handler) {
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
  GnsDnsLookupTask(Message query, SimpleResolver dnsServer, ClientRequestHandlerInterface handler) {
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
  GnsDnsLookupTask(Message query, SimpleResolver nameServer, Boolean isGNS, ClientRequestHandlerInterface handler) {
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

  /**
   * Returns the response.
   * 
   * @return the response
   */
  public Message getResponse() {
    return response;
  }
}
