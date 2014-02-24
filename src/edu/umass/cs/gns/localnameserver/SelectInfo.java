package edu.umass.cs.gns.localnameserver;

import edu.umass.cs.gns.nameserver.NameRecordKey;
import edu.umass.cs.gns.packet.SelectRequestPacket;
import java.net.InetAddress;

/**************************************************************
 * This class represents a data structure to store information
 * about queries (SELECT like lookup) transmitted by the local name
 * server.
 *************************************************************/
public class SelectInfo {

  private int id;
  private InetAddress senderAddress;
  private int senderPort;

  /**************************************************************
   * Constructs a SelectInfo object with the following parameters
   * @param id Query id
   * @param name Host/Domain name
   * @param time System time when query was transmitted
   * @param nameserverID Response name server ID
   * @param queryStatus Query Status
   **************************************************************/
  public SelectInfo(int id, InetAddress senderAddress, int senderPort) {
    this.id = id;
    this.senderAddress = senderAddress;
    this.senderPort = senderPort;
  }

  public int getId() {
    return id;
  }

  public InetAddress getSenderAddress() {
    return senderAddress;
  }

  public int getSenderPort() {
    return senderPort;
  }
  
}
