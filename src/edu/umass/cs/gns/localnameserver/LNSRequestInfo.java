/*
 * Copyright (C) 2015
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.localnameserver;

import edu.umass.cs.gns.nsdesign.packet.CommandPacket;

/**
 * Class represents the abstract class in which LNS stores info for each ongoing request,
 * from the time it is received by a LNS until a success/failure response is returned.
 * Only a single instance of this class is defined during the lifetime of a request.

 Some fields in this class are necessary to implement functionality of a local serviceName server, while
 some are there only to collect and log statistics for a request.
 *
 */
public class LNSRequestInfo {

  private final CommandPacket commandPacket;

  /** Unique request ID assigned to this request by the local name server */
  private final int lnsReqID;

  /** Time that CPP started processing this request. */
  protected final long startTime;
  
  /** Time that LNS completed processing this request */
  private long finishTime =  -1;

  /** Whether requests is finally successful or not. */
  private boolean success = false;

public LNSRequestInfo(int lnsReqId, CommandPacket commandPacket) {
    this.lnsReqID = lnsReqId;
    this.startTime = System.currentTimeMillis();
    this.commandPacket = commandPacket;
  }

  public synchronized String getServiceName() {
    return commandPacket.getServiceName();
  }

  public CommandPacket getCommandPacket() {
    return commandPacket;
  }

  public String getCommandType() {
    return commandPacket.getCommandName();
  }
  
  public String getHost() {
    return commandPacket.getSenderAddress();
  }

  public int getPort() {
    return commandPacket.getSenderPort();
  }

  public synchronized int getLNSReqID() {
    return lnsReqID;
  }

  public synchronized long getStartTime() {
    return startTime;
  }

  /** 
   * Time duration for which the request was under processing at the local serviceName server.
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

}
