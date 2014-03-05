package edu.umass.cs.gns.localnameserver;

import edu.umass.cs.gns.packet.ConfirmUpdateLNSPacket;
import edu.umass.cs.gns.packet.UpdateAddressPacket;
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

  private UpdateAddressPacket updateAddressPacket;
  private int numRestarts;
  public UpdateInfo(int id, String name, long sendTime, int nameserverId,
                    UpdateAddressPacket updateAddressPacket1, int numRestarts) {
    this.id = id;
    this.name = name;
    this.sendTime = sendTime;
    this.nameserverID = nameserverId;
    this.numRestarts = numRestarts;
    this.updateAddressPacket = updateAddressPacket1;
  }

  /**
   * Returns a String representation of QueryInfo
   */
  @Override
  public synchronized String toString() {
    StringBuilder str = new StringBuilder();
    str.append("ID:" + id);
    str.append(" Name:" + name);
    str.append(" Time:" + sendTime);
    str.append(" NS_ID:" + nameserverID);
    return str.toString();
  }

  public synchronized int getID() {
    return id;
  }

  public synchronized  String getName() {
    return name;
  }

  public synchronized String getUpdateStats(ConfirmUpdateLNSPacket confirmPkt, String name) {
    long latency = System.currentTimeMillis() - sendTime;
    int numTransmissions = 0;
    String msg = "Success-UpdateRequest\t" + name + "\t" + latency
            + "\t" + numTransmissions + "\t" + nameserverID
            + "\t" + LocalNameServer.getNodeID() + "\t" + confirmPkt.getRequestID() + "\t" + numRestarts + "\t" + System.currentTimeMillis();
    return msg;
  }

  public synchronized void setSendTime(long sendTime) {
    this.sendTime = sendTime;
  }

  public synchronized long getSendTime() {
    return  sendTime;
  }

  public synchronized void setNameserverID(int nameserverID) {
    this.nameserverID = nameserverID;
  }

  public synchronized int getNumRestarts() {
    return numRestarts;
  }

  public synchronized long getLatency() {
    return System.currentTimeMillis() - sendTime;
  }


  public synchronized String getUpdateFailedStats(Set<Integer> activesQueried, int lnsID, int requestID,
                                                  int coordinatorID) {
    long latency = System.currentTimeMillis() - sendTime;
    String msg = "Failed-UpdateNoActiveResponse\t" + name + "\t" + latency
            + "\t" + activesQueried.size() + "\t" + activesQueried
            + "\t" + lnsID + "\t" + requestID + "\t" + numRestarts + "\t" + coordinatorID + "\t" + System.currentTimeMillis();
    return msg;
  }

  public static String getUpdateFailedStats(String name, Set<Integer> activesQueried, int lnsID, int requestID,
                                            long sendTime, int numRestarts, int coordinatorID) {
    String queryStatus = "Failed-UpdateNoActiveResponse";
    if (activesQueried.size() == 0) queryStatus = "Failed-UpdateNoPrimaryResponse";

    long latency = System.currentTimeMillis() - sendTime;
    String msg = queryStatus + "\t" + name + "\t" + latency
            + "\t" + activesQueried.size() + "\t" + activesQueried
            + "\t" + lnsID + "\t" + requestID + "\t" + numRestarts + "\t" + coordinatorID + "\t" + System.currentTimeMillis();
    return msg;
  }

  /**
   * @return the updateAddressPacket
   */
  public UpdateAddressPacket getUpdateAddressPacket() {
    return updateAddressPacket;
  }


}
