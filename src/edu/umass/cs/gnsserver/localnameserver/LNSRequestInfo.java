
package edu.umass.cs.gnsserver.localnameserver;

import edu.umass.cs.gnscommon.CommandType;
import edu.umass.cs.gnscommon.packets.CommandPacket;

import java.net.InetSocketAddress;


public class LNSRequestInfo {

  private final CommandPacket commandPacket;


  private final long lnsReqID;


  protected final long startTime;


  private long finishTime = -1;


  private boolean success = false;
  
  private final InetSocketAddress sender;


  public LNSRequestInfo(long lnsReqId, CommandPacket commandPacket, InetSocketAddress sender) {
    this.lnsReqID = lnsReqId;
    this.startTime = System.currentTimeMillis();
    this.commandPacket = commandPacket;
    this.sender = sender;
    if(sender==null) throw new RuntimeException("Can not instantiate LNSRequestInfo with null sender");
  }


  public synchronized String getServiceName() {
    // arun: not needed any more, I think
    return commandPacket.getServiceName();
  }


  public CommandPacket getCommandPacket() {
    return commandPacket;
  }


  public CommandType getCommandType() {
    return commandPacket.getCommandType();
  }


  public String getHost() {
    return this.sender.getAddress().getHostAddress();//commandPacket.getSenderAddress();
  }


  public int getPort() {
    return this.sender.getPort();//commandPacket.getSenderPort();
  }


  public synchronized long getLNSReqID() {
    return lnsReqID;
  }


  public synchronized long getStartTime() {
    return startTime;
  }


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

  @Override
  public String toString() {
    return this.getCommandType().name() + ":" + this.getServiceName() + ":" + this.lnsReqID;
  }

  // arun
//  public String getCommandName() {
//    return this.commandPacket.getCommandName();
//  }
}
