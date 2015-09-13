/*
 * Copyright (C) 2015
 * University of Massachusetts
 * All Rights Reserved 
 *
 * Initial developer(s): Westy.
 */
package edu.umass.cs.gns.localnameserver;

import edu.umass.cs.gns.gnsApp.packet.CommandPacket;

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

  /** Time that CCP started processing this request. */
  protected final long startTime;
  
  /** Time that LNS completed processing this request */
  private long finishTime =  -1;

  /** Whether requests is finally successful or not. */
  private boolean success = false;

  /**
   *
   * @param lnsReqId
   * @param commandPacket
   */
  public LNSRequestInfo(int lnsReqId, CommandPacket commandPacket) {
    this.lnsReqID = lnsReqId;
    this.startTime = System.currentTimeMillis();
    this.commandPacket = commandPacket;
  }

  /**
   * Returns the service name.
   * 
   * @return the service name
   */
  public synchronized String getServiceName() {
    return commandPacket.getServiceName();
  }

  /**
   * Returns the command packet.
   * 
   * @return the command packet
   */
  public CommandPacket getCommandPacket() {
    return commandPacket;
  }

  /**
   * Returns the command type.
   * 
   * @return the command type
   */
  public String getCommandType() {
    return commandPacket.getCommandName();
  }
  
  /**
   * Returns the host.
   * 
   * @return the host
   */
  public String getHost() {
    return commandPacket.getSenderAddress();
  }

  /**
   * Returns the port.
   * 
   * @return the port
   */
  public int getPort() {
    return commandPacket.getSenderPort();
  }

  /**
   * Returns the LNS request id.
   * 
   * @return the LNS request id
   */
  public synchronized int getLNSReqID() {
    return lnsReqID;
  }

  /**
   * Returns the start time.
   * 
   * @return the start time
   */
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

  /**
   * Returns the finish time.
   * 
   * @return the finish time
   */
  public synchronized long getFinishTime() {
    return finishTime;
  }

  /**
   * Sets the finish time.
   */
  public synchronized void setFinishTime() {
    this.finishTime = System.currentTimeMillis();
  }

  /**
   * Returns the success value.
   * 
   * @return true or false
   */
  public synchronized boolean isSuccess() {
    return success;
  }

  /**
   * Sets the success value.
   * 
   * @param success
   */
  public synchronized void setSuccess(boolean success) {
    this.success = success;
  }

}
