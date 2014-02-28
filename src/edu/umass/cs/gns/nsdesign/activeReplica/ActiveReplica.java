package edu.umass.cs.gns.nsdesign.activeReplica;

import edu.umass.cs.gns.database.MongoRecords;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nameserver.recordmap.BasicRecordMap;
import edu.umass.cs.gns.nio.GNSNIOTransport;
import edu.umass.cs.gns.nsdesign.GNSNodeConfig;
import edu.umass.cs.gns.packet.*;
import edu.umass.cs.gns.replicaCoordination.ActiveReplicaCoordinator;
import edu.umass.cs.gns.replicaCoordination.ActiveReplicaPaxos;
import edu.umass.cs.gns.util.Util;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.concurrent.ScheduledThreadPoolExecutor;

/*** DONT not use any class in package edu.umass.cs.gns.nsdesign ***/

/**
 * Work in progress. Inactive code.
 *
 * Implements functionality of an active replica of a name.
 * We keep a single instance of this class for all names for whom this name server is an active replica.
 * Created by abhigyan on 2/26/14.
 */
public class ActiveReplica implements ActiveReplicaInterface{

  /** object handles coordination among replicas on a request, if necessary */
  private ActiveReplicaCoordinator activeCoordinator;

  /**ID of this node*/
  private int nodeID;

  /** nio server*/
  private GNSNIOTransport nioServer;

  /** executor service for handling tasks*/
  private ScheduledThreadPoolExecutor scheduledThreadPoolExecutor;

  /** Object provides interface to the database table storing name records */
  private BasicRecordMap nameRecordDB;

  /** Configuration for all nodes in GNS **/
  private GNSNodeConfig gnsNodeConfig;

  /**
   * constructor object
   */
  public ActiveReplica(int nodeID, String configFile, GNSNodeConfig gnsNodeConfig, GNSNIOTransport nioServer,
                       ScheduledThreadPoolExecutor scheduledThreadPoolExecutor) {
    this.nodeID = nodeID;

    this.nioServer = nioServer;

    this.scheduledThreadPoolExecutor = scheduledThreadPoolExecutor;
    String className = "edu.umass.cs.gns.nameserver.recordmap.MongoRecordMap";
    nameRecordDB = (BasicRecordMap) Util.createObject(className,
            // probably should use something more generic here
            MongoRecords.DBNAMERECORD);

    // create the activeCoordinator object.
    activeCoordinator = new ActiveReplicaPaxos(nioServer, new edu.umass.cs.gns.nameserver.GNSNodeConfig(), this);

    this.gnsNodeConfig = gnsNodeConfig;
  }


  /**
   * Entry point for all packets sent to active replica.
   *
   * Currently, we are implementing a single unreplicated active replica and replica controller.
   * So, we do not take any action on some packet types.
   * @param json json object received at name server
   */
  public void handleIncomingPacket(JSONObject json){
    // Types of packets:
    // (1) Lookup (from LNS)
    // (2) Update (from LNS)
    // (3) Add (from ReplicaController)  -- after completing add, sent reply to ReplicaController
    // (4) Remove (from ReplicaController) -- after completing remove, send reply to ReplicaController
    // (5) Group change (from ReplicaController) -- after completing group change, send reply to ReplicaController

    // and finally
    //  (6) ActiveReplicaCoordinator packets (from other ActiveReplicaCoordinator)
    try {
      Packet.PacketType type = Packet.getPacketType(json);
      switch (type) {
        /** Packets sent from LNS **/
        case DNS:     // lookup sent by lns
          if (activeCoordinator == null) {
            Lookup.executeLookupLocal(new DNSPacket(json), this);
          } else {
            activeCoordinator.handleRequest(json);
          }
          break;
        case UPDATE_ADDRESS_LNS: // update sent by lns.
          if (activeCoordinator == null) {
            Update.executeUpdateLocal(new UpdateAddressPacket(json), this);
          } else {
            activeCoordinator.handleRequest(json);
          }
          break;
        case SELECT_REQUEST: // ?? Abhigyan: need to understand how selects are implemented and if they need coordination
          break;
        case SELECT_RESPONSE: // ?? Abhigyan: need to figure how selects are implemented and if they need coordination
          break;
        case NAME_SERVER_LOAD:  // Report the load at name server to LNS.
          break;

        /** Packets sent from replica controller **/
        case ACTIVE_ADD: // sent when new name is added to GNS
          if (activeCoordinator == null) {
            Add.executeAddRecord(new AddRecordPacket(json), this);
          } else {
            activeCoordinator.handleRequest(json);
          }
          break;
        case ACTIVE_REMOVE: // sent when a name is to be removed from GNS
          if (activeCoordinator == null) {
            Remove.executeRemoveLocal(new RemoveRecordPacket(json), this);
          } else {
            activeCoordinator.handleRequest(json);
          }
          break;
        case ACTIVE_GROUPCHANGE: // change the set of active replicas for a name
          break;

        /** Packets from coordination modules at active replica **/
        case ACTIVE_COORDINATION:
          activeCoordinator.handleRequest(json);
          break;
        default:
          GNS.getLogger().warning("No handler for packet type: " + type.toString());
          break;
      }
    } catch (JSONException e) {
      e.printStackTrace();
    }
  }

  /**
   * ActiveReplicaCoordinator calls this method to locally execute a decision.
   * Depending on request type, this method will call a private method to execute request.
   */
  public void executeRequestLocal(JSONObject json) {

  }

  public int getNodeID(){
    return nodeID;
  }

  public BasicRecordMap getNameRecordDB(){
    return nameRecordDB;
  }

  public GNSNodeConfig getGNSNodeConfig(){
    return gnsNodeConfig;
  }

  public GNSNIOTransport getNioServer(){
    return nioServer;
  }

}
