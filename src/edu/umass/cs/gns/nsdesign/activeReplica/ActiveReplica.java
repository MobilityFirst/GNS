package edu.umass.cs.gns.nsdesign.activeReplica;

import edu.umass.cs.gns.nameserver.GNSNodeConfig;
import edu.umass.cs.gns.nio.GNSNIOTransport;
import edu.umass.cs.gns.replicaCoordination.ActiveReplicaCoordinator;
import edu.umass.cs.gns.replicaCoordination.ActiveReplicaPaxos;
import org.json.JSONObject;

import java.util.concurrent.ScheduledThreadPoolExecutor;

/**
 * Work in progress. Inactive code.
 *
 * Implements functionality of an active replica of a name.
 * We keep a single instance of this class for all names for whom this name server is an active replica.
 * Created by abhigyan on 2/26/14.
 */
public class ActiveReplica {

  /** object handles coordination among replicas on a request, if necessary */
  private ActiveReplicaCoordinator activeCoordinator;

  /**ID of this node*/
  private int nodeID;

  /** nio server*/
  private GNSNIOTransport nioServer;

  /** executor service for handling tasks*/
  private ScheduledThreadPoolExecutor scheduledThreadPoolExecutor;

  //

  /**
   * constructor object
   */
  public ActiveReplica(int nodeID, String configFile, GNSNIOTransport nioServer,
                       ScheduledThreadPoolExecutor scheduledThreadPoolExecutor) {
    this.nodeID = nodeID;

    this.nioServer = nioServer;

    this.scheduledThreadPoolExecutor = scheduledThreadPoolExecutor;

    // create the activeCoordinator object.
    activeCoordinator = new ActiveReplicaPaxos(nioServer, new GNSNodeConfig(), this);

  }


  /**
   * Entry point for all packets sent to replica controller.
   * @param json json object received at name server
   */
  public void handleIncomingPacket(JSONObject json) {
    // Types of packets:
    // (1) Lookup (from LNS)
    // (2) Update (from LNS)
    // (3) Add (from ReplicaController)  -- after completing add, sent reply to ReplicaController
    // (4) Remove (from ReplicaController) -- after completing remove, send reply to ReplicaController
    // (5) Group change (from ReplicaController) -- after completing group change, send reply to ReplicaController

    // and finally
    //  (6) ActiveReplicaCoordinator packets (from other ActiveReplicaCoordinator)

  }

  /**
   * ActiveReplicaCoordinator calls this method to locally execute a decision.
   * Depending on request type, this method will call a private method to execute request.
   */
  public void executeRequestLocal(JSONObject json) {

  }


}
