package edu.umass.cs.gns.localnameserver;

import edu.umass.cs.gns.nameserver.NameRecordKey;
import edu.umass.cs.gns.packet.QueryRequestPacket;
import java.net.InetAddress;
import java.util.HashSet;
import java.util.Set;

/**************************************************************
 * This class represents a data structure to store information
 * about queries (name lookup) transmitted by the local name
 * server
 * 
 *************************************************************/
public class QueryInfo {

  private int id;
  private QueryRequestPacket incomingPacket;
  private InetAddress senderAddress;
  private int senderPort;
  private Set<Integer> serverIds;

  /**************************************************************
   * Constructs a QueryInfo object with the following parameters
   * @param id Query id
   * @param name Host/Domain name
   * @param time System time when query was transmitted
   * @param nameserverID Response name server ID
   * @param queryStatus Query Status
   **************************************************************/
  public QueryInfo(int id, NameRecordKey recordKey, QueryRequestPacket incomingPacket,
          InetAddress senderAddress, int senderPort, Set<Integer> serverIds) {
    this.id = id;
    this.incomingPacket = incomingPacket;
    this.senderAddress = senderAddress;
    this.senderPort = senderPort;
    this.serverIds = serverIds;
  }

  public int getId() {
    return id;
  }

  public QueryRequestPacket getIncomingPacket() {
    return incomingPacket;
  }

  public InetAddress getSenderAddress() {
    return senderAddress;
  }

  public int getSenderPort() {
    return senderPort;
  }

  public Set<Integer> getServerIds() {
    return serverIds;
  }
  
}
