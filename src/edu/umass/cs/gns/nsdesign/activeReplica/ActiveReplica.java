package edu.umass.cs.gns.nsdesign.activeReplica;

import edu.umass.cs.gns.database.MongoRecords;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nameserver.recordmap.BasicRecordMap;
import edu.umass.cs.gns.nio.GNSNIOTransport;
import edu.umass.cs.gns.nsdesign.GNSMessagingTask;
import edu.umass.cs.gns.nsdesign.GNSNodeConfig;
import edu.umass.cs.gns.packet.*;
import edu.umass.cs.gns.replicaCoordination.ActiveReplicaCoordinator;
import edu.umass.cs.gns.util.Util;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import java.util.HashMap;
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
  public ActiveReplica(int nodeID, HashMap<String, String> configParameters, GNSNodeConfig gnsNodeConfig,
                       GNSNIOTransport nioServer, ScheduledThreadPoolExecutor scheduledThreadPoolExecutor) {
    this.nodeID = nodeID;

    this.gnsNodeConfig = gnsNodeConfig;

    this.nioServer = nioServer;

    this.scheduledThreadPoolExecutor = scheduledThreadPoolExecutor;

    String className = "edu.umass.cs.gns.nameserver.recordmap.MongoRecordMap";
    nameRecordDB = (BasicRecordMap) Util.createObject(className,
            // probably should use something more generic here
            MongoRecords.DBNAMERECORD);
//    nameRecordDB.reset();
    activeCoordinator = null;
    // create the activeCoordinator object.
//    activeCoordinator = new ActiveReplicaPaxos(nioServer, new edu.umass.cs.gns.nameserver.GNSNodeConfig(), this);

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

      GNSMessagingTask msgTask = null;
      switch (type) {
        /** Packets sent from LNS **/
        case DNS:     // lookup sent by lns
          if (activeCoordinator == null) {
            msgTask = Lookup.executeLookupLocal(new DNSPacket(json), this);
          } else {
            activeCoordinator.handleRequest(json);
          }
          break;
        case UPDATE_ADDRESS_LNS: // update sent by lns.
          msgTask = Update.handleUpdate(json, this);
          break;
        case SELECT_REQUEST:
          Select.handleSelectRequest(json, this);
          break;
        case SELECT_RESPONSE:
          Select.handleSelectResponse(json, this);
          break;
        case NAME_SERVER_LOAD:  // Report the load at name server to LNS.
          break;

        /** Packets sent from replica controller **/
        case ACTIVE_ADD: // sent when new name is added to GNS
          if (activeCoordinator == null) {
            msgTask = Add.executeAddRecord(new AddRecordPacket(json), this);
          } else {
            activeCoordinator.handleRequest(json);
          }
          break;
        case ACTIVE_REMOVE: // sent when a name is to be removed from GNS
          msgTask = Remove.handleActiveRemovePacket(new OldActiveSetStopPacket(json), this);
          break;
        case ACTIVE_GROUPCHANGE: // change the set of active replicas for a name
          break;

        /** Packets from coordination modules at active replica **/
        case ACTIVE_COORDINATION:
          activeCoordinator.handleRequest(json);
          break;
        // SELECT
        default:
          GNS.getLogger().warning("No handler for packet type: " + type.toString());
          break;
      }
      if (msgTask != null) {
        GNSMessagingTask.send(msgTask, nioServer);
      }
    } catch (JSONException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    } catch (NoSuchAlgorithmException e) {
      e.printStackTrace();
    } catch (InvalidKeySpecException e) {
      e.printStackTrace();
    } catch (InvalidKeyException e) {
      e.printStackTrace();
    } catch (SignatureException e) {
      // todo what to do in these cases?
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

  public BasicRecordMap getDB(){
    return nameRecordDB;
  }

  public GNSNodeConfig getGNSNodeConfig(){
    return gnsNodeConfig;
  }

  public GNSNIOTransport getNioServer(){
    return nioServer;
  }

  public ActiveReplicaCoordinator getActiveCoordinator() {
    return activeCoordinator;
  }
}
