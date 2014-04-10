/*
 * Copyright (C) 2013
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.localnameserver;

import edu.umass.cs.gns.nsdesign.packet.DNSPacket;
import edu.umass.cs.gns.util.NameRecordKey;

import java.util.Set;

/**************************************************************
 * This class represents a data structure to store information
 * about queries (name lookup) transmitted by the local name
 * server. It contains a few fields only for logging statistics
 * related to the requests.
 * 
 *************************************************************/
public class DNSRequestInfo {


  /** Unique ID for each query **/
  private int id;
  /** Host/domain name in the query **/
  private String qName;
  /** System time when query was transmitted from the local name server **/
  private long lookupRecvdTime;
  /** System time when a response for this query was received at a local name server.**/
  private long recvTime = -1;

  private DNSPacket incomingPacket;
  public int numInvalidActiveError;

  private int nameserverID;

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
          DNSPacket incomingPacket, int numRestarts) {
    this.id = id;
    this.qName = name;
    this.lookupRecvdTime = time;

    this.incomingPacket = incomingPacket;
    this.numInvalidActiveError = numRestarts;
    this.nameserverID = nameserverID;
  }

  public static String getFailureLogMessage(int lookupNumber, NameRecordKey recordKey, String name,
                                            int transmissionCount, long receivedTime, int numRestarts,
                                            int coordinatorID, Set<Integer> nameserversQueried) {
    String failureCode = "Failed-LookupNoActiveResponse";
    if (nameserversQueried == null || nameserversQueried.isEmpty()) {
      failureCode = "Failed-LookupNoPrimaryResponse";
    }

    return (failureCode + "\t"
            + lookupNumber + "\t"
            + recordKey + "\t"
            + name + "\t"
            + transmissionCount + "\t"
            + (System.currentTimeMillis() - receivedTime) + "\t"
            + receivedTime + "\t"
            + numRestarts + "\t"
            + coordinatorID + "\t"
            + nameserversQueried);
  }


  public synchronized void setNameserverID(int nameserverID1) {
    nameserverID = nameserverID1;

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
    str.append("ID:" + getId());
    str.append(" Name:" + getqName());
    str.append(" Key:NA");// + qRecordKey.getName());str
    str.append(" Time:" + lookupRecvdTime);
    str.append(" Transmission:NA");
    return str.toString();
  }

  public synchronized String getLookupStats() {
    //Response Information: Time(ms) ActiveNS Ping(ms) Name NumTransmission LNS Timestamp(systime)
    StringBuilder str = new StringBuilder();
    str.append("0\t");
    str.append(incomingPacket.getKey().getName() + "\t");
    str.append(getqName());
    str.append("\t" + (getRecvTime() - lookupRecvdTime));
    str.append("\t0");
    str.append("\t0");
    str.append("\t0");
    str.append("\t0");
    str.append("\t" + nameserverID);
    str.append("\t" + LocalNameServer.getNodeID());
    str.append("\t" + lookupRecvdTime);
    str.append("\t" + numInvalidActiveError);
    str.append("\t[]");
    str.append("\t[]");

    //save response time
    String stats = str.toString();
    return stats;
  }

  /**
   * 
   * @param receiveTIme
   * @return
   */
  public synchronized void setRecvTime(long receiveTIme) {
    if (this.getRecvTime() == -1) {
      this.recvTime = receiveTIme;
    }
  }

  public synchronized long getResponseTime() {
    if (this.getRecvTime() == -1) {
      return -1L;
    }
    return (this.getRecvTime() - this.lookupRecvdTime);
  }

  /**
   * @return the id
   */
  public int getId() {
    return id;
  }

  /**
   * @return the qName
   */
  public String getqName() {
    return qName;
  }

  /**
   * @return the recvTime
   */
  public long getRecvTime() {
    return recvTime;
  }

  /**
   * @return the incomingPacket
   */
  public DNSPacket getIncomingPacket() {
    return incomingPacket;
  }
}
