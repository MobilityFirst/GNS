
package edu.umass.cs.gnsserver.gnamed;

import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.ClientRequestHandlerInterface;
import org.xbill.DNS.Message;
import org.xbill.DNS.SimpleResolver;

import java.util.concurrent.Callable;


public class LookupTask implements Callable<Message> {

  private final WorkerClass workerClass;
  private final Message query;
  private Message response;
  private final SimpleResolver nameServer; // It is used for both dnsServer and gnsServer
  private final ClientRequestHandlerInterface handler;


  LookupTask(Message query, ClientRequestHandlerInterface handler) {
    this.workerClass = WorkerClass.GNSLOCAL;
    this.query = query;
    this.nameServer = null;
    this.handler = handler;
  }
  

  LookupTask(Message query, SimpleResolver dnsServer, ClientRequestHandlerInterface handler) {
    this.workerClass = WorkerClass.DNS;
    this.query = query;
    this.nameServer = dnsServer;
    this.handler = handler;
  }


  LookupTask(Message query, SimpleResolver nameServer, Boolean isGNS, ClientRequestHandlerInterface handler) {
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
