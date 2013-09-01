package edu.umass.cs.gns.localnameserver;

import edu.umass.cs.gns.nameserver.NameRecordKey;
import edu.umass.cs.gns.packet.DNSPacket;

import java.net.InetAddress;

/**************************************************************
 * This class represents a data structure to store information
 * about queries (name lookup) transmitted by the local name
 * server
 * 
 * @author Hardeep Uppal
 *************************************************************/
public class QueryInfo {

//  public final static String CACHE = "cache";
//  public final static String SINGLE_TRANSMISSION = "single-transmission";
//  public final static String MULTIPLE_TRANSMISSION = "multiple-transmission";
  public final static String CONTACT_PRIMARY = "contact-primary";
  public final static String INVALID_ACTIVE_NS = "invalid-active-ns";
  /** Unique ID for each query **/
  public int id;
  /** The key of the value key pair. For GNRS this will be EdgeRecord, CoreRecord or GroupRecord. **/
//  public NameRecordKey qRecordKey;
  /** Host/domain name in the query **/
  public String qName;
  /** System time when query was transmitted from the local name server **/
  private long lookupRecvdTime;
  
  /** System time when a response for this query was received at a local name server.**/
  private long recvTime = -1;
  
  /** Number of transmission before a response was received **/
//  private short numTransmission;
  /** ID of the name server that responded to this query **/
//  private int nameserverID;
//  private String nameServerQueried;
//  private String nameServerQueriedPingLatency;
  /** Query Status: for evaluation purposes **/
//  private String queryStatus;
//  private boolean hasDataToProceed;
  /** Used by the wait/notify calls **/
//  private final Object monitor = new Object();
//  private int lookupNumber;

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
  public QueryInfo(int id, String name, NameRecordKey recordKey, long time, 
		  int nameserverID, String queryStatus, int lookupNumber,
		  DNSPacket incomingPacket, InetAddress senderAddress, int senderPort) {
    this.id = id;
//    this.qRecordKey = recordKey;
    this.qName = name;
    this.lookupRecvdTime = time;
//    this.numTransmission = 1;
//    this.nameserverID = nameserverID;
//    this.nameServerQueried = Integer.toString(nameserverID);
//    this.nameServerQueriedPingLatency = Double.toString(ConfigFileInfo.getPingLatency(nameserverID));
//    this.queryStatus = queryStatus;
//    this.hasDataToProceed = false;
//    this.lookupNumber = lookupNumber;
    
    this.incomingPacket = incomingPacket;
    this.senderAddress = senderAddress;
    this.senderPort = senderPort;
  }

  
  /**************************************************************
   * Append the query status to <i>status</i>
   * @param status Query Status
   *************************************************************/
//  public synchronized void appendQueryStatus(String status) {
//  	if (queryStatus == null )
//  		if (this.queryStatus == null) this.queryStatus = status;
//      else this.queryStatus +=  '|' + status;
//  }
//
//
//  public synchronized String getQueryStatus() {
//  	return queryStatus;
//  }

  /**************************************************************
   * Signal sender that ListenerResponse has data and the sender
   * can proceed.
   * @return <i>true</i> if sender should proceed. <i>false</i>
   * otherwise. 
   *************************************************************/
//  public synchronized boolean hasDataToProceed() {
//    return this.hasDataToProceed;
//  }
  
//  public boolean waitUntilHasDataToProceedOrTimeout(long waitingTime) {
//    long t0 = System.currentTimeMillis();
//    long t1;
//    try {
//      synchronized (monitor) {
//        t1 = System.currentTimeMillis();
//        while (this.hasDataToProceed != true && t1 - t0 < waitingTime) {
//          GNRS.getLogger().fine("QueryInfo: Entering wait queue for queryId:" + id + " at time " + t1);
//          monitor.wait(waitingTime);
//          t1 = System.currentTimeMillis();
//        }
//      }
//      // value is now true   
//    } catch (InterruptedException x) {
//    }
//    return this.hasDataToProceed;
//  }

  /**************************************************************
   * Set hasData flag
   * @param hasData
   *************************************************************/
//  public synchronized void setHasDataToProceed(boolean hasData) {
//    this.hasDataToProceed = hasData;
//  }
//  public void setHasDataToProceed(boolean hasData) {
////    synchronized (monitor) {
//      this.hasDataToProceed = hasData;
//      GNS.getLogger().fine("QueryInfo: Setting value to " + hasData + " for queryId:" + id);
////      monitor.notify();
////    }
//  }

  /**
   * Adds given name server to queried name servers.
   * 
   * @param nameServerID name server to be queried
   * @param queryStatus query status to be appended
   * @return true if name server not already queried, false otherwise
   */
//  public synchronized boolean addNameServerQueried(int nameServerID, String queryStatus) {
//  	String[] nameServers = nameServerQueried.split("|");
//  	for (String x: nameServers) {
//  		if (Integer.toString(nameServerID).equals(x)) return false;
//  		GNS.getLogger().fine("QueryInfo: False: name server " + nameServerID + " already queried.");
//  	}
//  	nameServerQueried += '|' + Integer.toString(nameServerID);
//		nameServerQueriedPingLatency += '|'
//				+ Double.toString(ConfigFileInfo.getPingLatency(nameServerID));
//    nameserverID = nameServerID;
//    if (this.queryStatus == null) this.queryStatus = queryStatus;
//    else this.queryStatus +=  '|' + queryStatus;
//    GNS.getLogger().fine("QueryInfo: True: name server " + nameServerID + " is to be queried.");
//    numTransmission += 1;
//  	return true;
//
//  }
  
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
//    str.append(" NS_ID:" + nameserverID);
//    str.append(" Status:NA");// + queryStatus);
//    str.append(" NameServerQueried:" + nameServerQueried);
//    str.append(" Ping: " + nameServerQueriedPingLatency);
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

//		if (queryStatus == QueryInfo.CACHE) {
//			str.append("\tNA");
//		} else {
//			str.append("\t" + ConfigFileInfo.getPingLatency(nameserverID));
//		}
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
//		if (activeNameServerSet != null) {
//			str.append("\t" + activeNameServerSet.size());
//			str.append("\t" + activeNameServerSet.toString());
//		} else {
//			str.append("\t0");
//			str.append("\t[]");
//		}
//
//		if (primaryNameServerSet != null) {
//			str.append("\t" + primaryNameServerSet.toString());
//		} else {
//			str.append("\t" + "[]");
//		}

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
	  if (this.recvTime == -1) this.recvTime = receiveTIme;
  }
  
  public synchronized long getResponseTime() {
  	if (this.recvTime == -1) return -1L;    //numTransmission > 1 ||
  	return (this.recvTime - this.lookupRecvdTime);
  }
  
}
