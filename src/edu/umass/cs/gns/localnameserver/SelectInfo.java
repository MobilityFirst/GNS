package edu.umass.cs.gns.localnameserver;

import edu.umass.cs.gns.nameserver.NameRecordKey;
import edu.umass.cs.gns.packet.SelectRequestPacket;
import java.net.InetAddress;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.json.JSONObject;

/**************************************************************
 * This class represents a data structure to store information
 * about queries (SELECT like lookup) transmitted by the local name
 * server.
 *************************************************************/
public class SelectInfo {

  private int id;
  private SelectRequestPacket incomingPacket;
  private InetAddress senderAddress;
  private int senderPort;
  private Set<Integer> serverIds;
  private ConcurrentHashMap<String, JSONObject> responses;

  /**************************************************************
   * Constructs a SelectInfo object with the following parameters
   * @param id Query id
   * @param name Host/Domain name
   * @param time System time when query was transmitted
   * @param nameserverID Response name server ID
   * @param queryStatus Query Status
   **************************************************************/
  public SelectInfo(int id, NameRecordKey recordKey, SelectRequestPacket incomingPacket,
          InetAddress senderAddress, int senderPort, Set<Integer> serverIds) {
    this.id = id;
    this.incomingPacket = incomingPacket;
    this.senderAddress = senderAddress;
    this.senderPort = senderPort;
    this.serverIds = Collections.newSetFromMap(new ConcurrentHashMap<Integer, Boolean>());
    this.serverIds.addAll(serverIds);
    this.responses =  new ConcurrentHashMap<String, JSONObject>(10, 0.75f, 3);
  }

  public int getId() {
    return id;
  }

  public SelectRequestPacket getIncomingPacket() {
    return incomingPacket;
  }

  public InetAddress getSenderAddress() {
    return senderAddress;
  }

  public int getSenderPort() {
    return senderPort;
  }

  public void removeServerID(int id) {
    serverIds.remove(id);
  }
  
  public Set<Integer> serversYetToRespond() {
    return serverIds;
  }
  
  public boolean allServersResponded() {
    return serverIds.isEmpty();
  }
  
  public boolean addNewResponse(String name, JSONObject json) {
    if (!responses.containsKey(name)) {
      responses.put(name, json);
      return true;
    } else {
      return false;
    }
  }

  public Set<JSONObject> getResponses() {
    return new HashSet<JSONObject>(responses.values());
  }
}
