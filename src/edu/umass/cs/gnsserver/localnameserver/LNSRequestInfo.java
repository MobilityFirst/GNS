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
 *  Initial developer(s): Westy
 *
 */
package edu.umass.cs.gnsserver.localnameserver;

import java.net.InetSocketAddress;

import edu.umass.cs.gnscommon.CommandType;
import edu.umass.cs.gnscommon.packets.CommandPacket;

/**
 * Class represents the abstract class in which LNS stores info for each ongoing request,
 * from the time it is received by a LNS until a success/failure response is returned.
 * Only a single instance of this class is defined during the lifetime of a request.
 *
 * Some fields in this class are necessary to implement functionality of a local serviceName server, while
 * some are there only to collect and log statistics for a request.
 *
 */
public class LNSRequestInfo {

  private final CommandPacket commandPacket;

  /**
   * Unique request ID assigned to this request by the local name server
   */
  private final long lnsReqID;

  /**
   * Time that CCP started processing this request.
   */
  protected final long startTime;

  /**
   * Time that LNS completed processing this request
   */
  private long finishTime = -1;

  /**
   * Whether requests is finally successful or not.
   */
  private boolean success = false;
  
  private final InetSocketAddress sender;

  /**
   *
   * @param lnsReqId
   * @param commandPacket
   * @param sender
   */
  public LNSRequestInfo(long lnsReqId, CommandPacket commandPacket, InetSocketAddress sender) {
    this.lnsReqID = lnsReqId;
    this.startTime = System.currentTimeMillis();
    this.commandPacket = commandPacket;
    this.sender = sender;
    if(sender==null) throw new RuntimeException("Can not instantiate LNSRequestInfo with null sender");
  }

  /**
   * Returns the service name.
   *
   * @return the service name
   */
  public synchronized String getServiceName() {
    // arun: not needed any more, I think
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
  public CommandType getCommandType() {
    return commandPacket.getCommandType();
  }

  /**
   * Returns the host.
   *
   * @return the host
   */
  public String getHost() {
    return this.sender.getAddress().getHostAddress();//commandPacket.getSenderAddress();
  }

  /**
   * Returns the port.
   *
   * @return the port
   */
  public int getPort() {
    return this.sender.getPort();//commandPacket.getSenderPort();
  }

  /**
   * Returns the LNS request id.
   *
   * @return the LNS request id
   */
  public synchronized long getLNSReqID() {
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
   *
   * @return the latency
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

  @Override
  public String toString() {
    return this.getCommandType().name() + ":" + this.getServiceName() + ":" + this.lnsReqID;
  }

  // arun
//  public String getCommandName() {
//    return this.commandPacket.getCommandName();
//  }
}
