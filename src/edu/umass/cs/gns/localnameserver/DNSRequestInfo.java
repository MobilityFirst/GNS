package edu.umass.cs.gns.localnameserver;

import edu.umass.cs.gns.nameserver.NameRecordKey;
import edu.umass.cs.gns.packet.DNSPacket;
import java.net.InetAddress;

/**************************************************************
 * This class represents a data structure to store information
 * about queries (name lookup) transmitted by the local name
 * server
 * 
 *************************************************************/
public class DNSRequestInfo {

  public final static String CONTACT_PRIMARY = "contact-primary";
  public final static String INVALID_ACTIVE_NS = "invalid-active-ns";
  /** Unique ID for each query **/
  public int id;
  /** Host/domain name in the query **/
  public String qName;
  /** System time when query was transmitted from the local name server **/
  private long lookupRecvdTime;
  /** System time when a response for this query was received at a local name server.**/
  private long recvTime = -1;
  // ABHIGYAN: Parameters for user sending the DNS query.
  public DNSPacket incomingPacket;
  public InetAddress senderAddress;
  public int senderPort;

  /**************************************************************
   * Constructs a QueryInfo object with the following parameters
   * @param id Query id
   * @param name Host/Domain name
   * @param time System time when query was transmitted
   * @param nameserverID Response name server ID
   * @param queryStatus Query Status
   **************************************************************/
  public DNSRequestInfo(int id, String name, NameRecordKey recordKey, long time,
          int nameserverID, String queryStatus, int lookupNumber,
          DNSPacket incomingPacket, InetAddress senderAddress, int senderPort) {
    this.id = id;
    this.qName = name;
    this.lookupRecvdTime = time;

    this.incomingPacket = incomingPacket;
    this.senderAddress = senderAddress;
    this.senderPort = senderPort;
  }

  /**
   *  
   * @return
   */
  public long getLookupRecvdTime() {
    return lookupRecvdTime;
  }

  /**************************************************************
   * Returns a String representation of QueryInfo
   **************************************************************/
  @Override
  public String toString() {
    StringBuilder str = new StringBuilder();
    str.append("ID:" + id);
    str.append(" Name:" + qName);
    str.append(" Key:NA");// + qRecordKey.getName());str
    str.append(" Time:" + lookupRecvdTime);
    str.append(" Transmission:NA");
    return str.toString();
  }

  public synchronized String getLookupStats() {
    //Response Information: Time(ms) ActiveNS Ping(ms) Name NumTransmission LNS Timestamp(systime)
    StringBuilder str = new StringBuilder();
//		str.append(lookupNumber + "\t");
    str.append("0\t");
//		str.append(qRecordKey + "\t");
    str.append("x\t");
    str.append(qName);
    str.append("\t" + (recvTime - lookupRecvdTime));
    str.append("\t0");
//		str.append("\t" + ConfigFileInfo.getPingLatency(ConfigFileInfo.getClosestNameServer()));
    str.append("\t0");
//		str.append("\t" + ConfigFileInfo.getClosestNameServer());
    str.append("\t0");
//		str.append("\t" + numTransmission);
    str.append("\t0");
    str.append("\t0");// + nameserverID);
    str.append("\t" + LocalNameServer.nodeID);
    str.append("\t" + lookupRecvdTime);
//		str.append("\t" + queryStatus);
    str.append("\ta");
//		str.append("\t" + nameServerQueried);
    str.append("\t[]");
//		str.append("\t" + nameServerQueriedPingLatency);
    str.append("\t[]");

    //save response time
    String stats = str.toString();
    //		responseTime.add( stats );
    return stats;
  }

  /**
   * 
   * @param receiveTIme
   * @return
   */
  public synchronized void setRecvTime(long receiveTIme) {
    if (this.recvTime == -1) {
      this.recvTime = receiveTIme;
    }
  }

  public synchronized long getResponseTime() {
    if (this.recvTime == -1) {
      return -1L;    //numTransmission > 1 ||
    }
    return (this.recvTime - this.lookupRecvdTime);
  }
}
