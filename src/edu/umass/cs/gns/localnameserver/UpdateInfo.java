package edu.umass.cs.gns.localnameserver;

import edu.umass.cs.gns.packet.ConfirmUpdateLNSPacket;
import java.util.Set;

public class UpdateInfo {

  /**
   * Unique ID for each update request *
   */
  private int id;
  /**
   * Host/domain name updated *
   */
  private String name;
  /**
   * System time when update was transmitted from the local name server *
   */
  private long sendTime;
  /**
   * ID of the name server where update was sent *
   */
  private int nameserverID;
  /* so we can send a response back */
  public String senderAddress;
  public int senderPort;

  public UpdateInfo(int id, String name, long sendTime, int nameserverId, String senderAddress, int senderPort) {
    this.id = id;
    this.name = name;
    this.sendTime = sendTime;
    this.nameserverID = nameserverId;
    this.senderAddress = senderAddress;
    this.senderPort = senderPort;

  }

  /**
   * Returns a String representation of QueryInfo
   */
  @Override
  public String toString() {
    StringBuilder str = new StringBuilder();
    str.append("ID:" + id);
    str.append(" Name:" + name);
    str.append(" Time:" + sendTime);
    str.append(" NS_ID:" + nameserverID);
    return str.toString();
  }

  public int getID() {
    return id;
  }

  public String getName() {
    return name;
  }

  public String getSenderAddress() {
    return senderAddress;
  }

  public int getSenderPort() {
    return senderPort;
  }

  public String getUpdateStats(ConfirmUpdateLNSPacket confirmPkt, String name) {
    long latency = System.currentTimeMillis() - sendTime;
    String msg = "Success-UpdateRequest\t" + name + "\t" + latency
            + "\t" + 3 + "\t" + 0
            + "\t" + LocalNameServer.nodeID + "\t" + confirmPkt.getRequestID() + "\t" + System.currentTimeMillis();
    return msg;
  }

  public long getLatency() {
    return System.currentTimeMillis() - sendTime;
  }

  public String getUpdateFailedStats(Set<Integer> activesQueried, int lnsID, int requestID) {
    long latency = System.currentTimeMillis() - sendTime;
    String msg = "Failed-UpdateRequest\t" + name + "\t" + latency
            + "\t" + null + "\t" + activesQueried
            + "\t" + lnsID + "\t" + requestID + "\t" + System.currentTimeMillis();
    return msg;
  }
}
