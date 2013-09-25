package edu.umass.cs.gns.localnameserver;

import edu.umass.cs.gns.nameserver.NameRecordKey;
import edu.umass.cs.gns.packet.QueryRequestPacket;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.json.JSONObject;

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
  private ConcurrentHashMap<Integer, Integer> serverIds;
  private ConcurrentHashMap<String, JSONObject> responses;

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
    this.serverIds = new ConcurrentHashMap<Integer, Integer>(10, 0.75f, 3);
    this.responses =  new ConcurrentHashMap<String, JSONObject>(10, 0.75f, 3);
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

  public void removeServerID(int id) {
    serverIds.remove(id);
  }
  
  public Set<Integer> serversYetToRespond() {
    return serverIds.keySet();
  }
  
  public boolean allServersResponded() {
    return serverIds.isEmpty();
  }
  
  public void addNewResponse(String name, JSONObject json) {
    if (!responses.containsKey(name)) {
      responses.put(name, json);
    }
  }

  public Set<JSONObject> getResponses() {
    return new HashSet<JSONObject>(responses.values());
  }
}
