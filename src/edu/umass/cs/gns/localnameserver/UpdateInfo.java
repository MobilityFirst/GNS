package edu.umass.cs.gns.localnameserver;

import edu.umass.cs.gns.nsdesign.packet.*;
import java.util.Set;

/**************************************************************
 * This class stores information not just update requests, but also
 * add and remove requests transmitted by the local name
 * server. It contains a few fields only for logging statistics
 * related to the requests.
 *************************************************************/
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

  private BasicPacket basicPacket;

  /** number of times this request has received invalid active error */
  private int numInvalidActiveError;

  public UpdateInfo(int id, String name, long sendTime, int nameserverId,
                    BasicPacket updateAddressPacket1, int numInvalidActiveError) {
    this.id = id;
    this.name = name;
    this.sendTime = sendTime;
    this.nameserverID = nameserverId;
    this.numInvalidActiveError = numInvalidActiveError;
    this.basicPacket = updateAddressPacket1;
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

  public synchronized String getUpdateStats(ConfirmUpdatePacket confirmPkt) {
    long latency = System.currentTimeMillis() - sendTime;
    int numTransmissions = 0;

    String success = confirmPkt.isSuccess() ?"Success" : "Failed";
    String requestType = null;
    if (confirmPkt.getType().equals(Packet.PacketType.CONFIRM_ADD))
      requestType = success + "-Add";
    else if (confirmPkt.getType().equals(Packet.PacketType.CONFIRM_REMOVE))
      requestType = success + "-Remove";
    else if (confirmPkt.getType().equals(Packet.PacketType.CONFIRM_UPDATE))
      requestType = success + "-Update";
    return requestType + "\t" + name + "\t" + latency
            + "\t" + numTransmissions + "\t" + nameserverID
            + "\t" + LocalNameServer.getNodeID() + "\t" + confirmPkt.getRequestID() + "\t" + numInvalidActiveError + "\t" + System.currentTimeMillis();
  }


  public synchronized long getSendTime() {
    return  sendTime;
  }

  public synchronized void setNameserverID(int nameserverID) {
    this.nameserverID = nameserverID;
  }

  public synchronized int getNumInvalidActiveError() {
    return numInvalidActiveError;
  }

  public synchronized long getLatency() {
    return System.currentTimeMillis() - sendTime;
  }


  public synchronized String getUpdateFailedStats(Set<Integer> activesQueried, int lnsID, int requestID,
                                                  int coordinatorID) {
    String requestType = "Update";
    if (basicPacket.getType().equals(Packet.PacketType.ADD_RECORD)) requestType = "Add";
    else if (basicPacket.getType().equals(Packet.PacketType.REMOVE_RECORD)) requestType = "Remove";
    long latency = System.currentTimeMillis() - sendTime;
    return  "Failed-"+ requestType + "NoActiveResponse\t" + name + "\t" + latency + "\t" + activesQueried.size() +
            "\t" + activesQueried + "\t" + lnsID + "\t" + requestID + "\t" + numInvalidActiveError + "\t" +
            coordinatorID + "\t" + System.currentTimeMillis();
  }

  public static String getUpdateFailedStats(String name, Set<Integer> activesQueried, int lnsID, int requestID,
                                            long sendTime, int numRestarts, int coordinatorID, Packet.PacketType packetType) {
    String requestType = "Update";
    if (packetType.equals(Packet.PacketType.ADD_RECORD)) requestType = "Add";
    else if (packetType.equals(Packet.PacketType.REMOVE_RECORD)) requestType = "Remove";
    String queryStatus = "Failed-"+ requestType + "NoActiveResponse";
    if (activesQueried.size() == 0) queryStatus = "Failed-"+ requestType + "NoPrimaryResponse";

    long latency = System.currentTimeMillis() - sendTime;
    return queryStatus + "\t" + name + "\t" + latency + "\t" + activesQueried.size() + "\t" + activesQueried +
            "\t" + lnsID + "\t" + requestID + "\t" + numRestarts + "\t" + coordinatorID + "\t" +
            System.currentTimeMillis();

  }

  /**
   * @return the basicPacket
   */
  public BasicPacket getUpdatePacket() {
    return basicPacket;
  }


}
