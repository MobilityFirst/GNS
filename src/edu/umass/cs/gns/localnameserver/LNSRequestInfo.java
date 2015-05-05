/*
 * Copyright (C) 2015
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.localnameserver;

/**
 * Class represents the abstract class in which LNS stores info for each ongoing request,
 * from the time it is received by a LNS until a success/failure response is returned.
 * Only a single instance of this class is defined during the lifetime of a request.
 *
 * Some fields in this class are necessary to implement functionality of a local name server, while
 * some are there only to collect and log statistics for a request.
 *
 */
public class LNSRequestInfo {

  private final String name;
  
  private final String clientHost;
  
  private final int clientPort;

  /** Unique request ID assigned to this request by the local name server */
  private final int lnsReqID;

  /** Time that CPP started processing this request */
  protected final long startTime;

  /** True if CPP is requesting current set of active replicas for this request. False, otherwise */
  private boolean lookupActives = false;

  /** Number of times this request has initiated lookup actives operation.
   * This could be always zero for some types of requests (namely ADD and REMOVE) that are
   * sent only to replica controllers (and never to active replicas) */
  private int numLookupActives = 0;

  // Instrumentation
  
  /** Time that LNS completed processing this request */
  private long finishTime =  -1;

  /** Whether requests is finally successful or not. */
  private boolean success = false;

public LNSRequestInfo(int lnsReqId, String name, String host, int port) {
    this.lnsReqID = lnsReqId;
    this.name = name;
    this.clientHost = host;
    this.clientPort = port;
    this.startTime = System.currentTimeMillis();
    this.numLookupActives = 0;
  }

  public synchronized String getName() {
    return name;
  }
  
  public String getHost() {
    return clientHost;
  }

  public int getPort() {
    return clientPort;
  }

  public synchronized int getLNSReqID() {
    return lnsReqID;
  }

  public synchronized long getStartTime() {
    return startTime;
  }

  /** 
   * Time duration for which the request was under processing at the local name server.
   * @return 
   */
  public synchronized long getResponseLatency() {
    return finishTime - startTime;
  }

  public synchronized long getFinishTime() {
    return finishTime;
  }

  public synchronized void setFinishTime() {
    this.finishTime = System.currentTimeMillis();
  }

  public synchronized boolean isSuccess() {
    return success;
  }

  public synchronized void setSuccess(boolean success) {
    this.success = success;
  }

  public synchronized boolean isLookupActives() {
    return lookupActives;
  }

  /** 
   * Returns true if actives are currently not being requested currently. Otherwise false.
   * @return 
   */
  public synchronized boolean setLookupActives() {
    if (lookupActives) {
      return false;
    } else {
      lookupActives = true;
      numLookupActives += 1;
      return true;
    }
  }

  /** After active replicas are received for this request, reset the lookup actives variables */
  public synchronized void unsetLookupActives() {
    assert lookupActives;
    lookupActives = false;
  }

  public synchronized int getNumLookupActives() {
    return numLookupActives;
  }

}
