package edu.umass.cs.gns.nsdesign.replicaController;

import edu.umass.cs.gns.nameserver.GNSNodeConfig;
import edu.umass.cs.gns.nio.GNSNIOTransport;
import edu.umass.cs.gns.replicaCoordination.ReplicaControllerCoordinator;
import edu.umass.cs.gns.replicaCoordination.ReplicaControllerPaxos;
import org.json.JSONObject;

import java.util.concurrent.ScheduledThreadPoolExecutor;

/**
 * Work in progress. Inactive code.
 *
 * Class implements all functionality of a replica controller.
 * We keep a single instance of this class for all names for whom this name server is a replica controller.
 * Created by abhigyan on 2/26/14.
 */
public class ReplicaController {

  /** object handles coordination among replicas on a request, if necessary */
  private ReplicaControllerCoordinator rcCoordinator;

  /**ID of this node*/
  private int nodeID;

  /** nio server*/
  private GNSNIOTransport nioServer;

  /** executor service for handling timer tasks*/
  private ScheduledThreadPoolExecutor scheduledThreadPoolExecutor;

  //

  /**
   * constructor object
   */
  public ReplicaController(int nodeID, String configFile, GNSNIOTransport nioServer,
                       ScheduledThreadPoolExecutor scheduledThreadPoolExecutor) {
    this.nodeID = nodeID;

    this.nioServer = nioServer;

    this.scheduledThreadPoolExecutor = scheduledThreadPoolExecutor;

    // create the activeCoordinator object.
    rcCoordinator = new ReplicaControllerPaxos(nioServer, new GNSNodeConfig(), this);

  }


  /**
   * Entry point for all packets sent to replica controller.
   * @param json json object received at name server
   */
  public void handleIncomingPacket(JSONObject json) {
     // types of packets:
     // (1) request actives packet (from LNS)
     // (2) add  (from LNS)
    //  (3) remove (from LNS)
    //  (4) add complete (from ActiveReplica) -- confirmation received from active replica that add is complete
    //  (5) group change complete (from ActiveReplica) -- confirmation received from active replica that group change is complete
    //  (6) remove complete (from ActiveReplica) -- confirmation received from active replica that remove is complete

    // and finally
    //  (6) ReplicaControllerCoordinator packets (from NS)s
  }


  /**
   * ReplicaControllerCoordinator calls this method to locally execute a decision.
   * Depending on request type, it call one of the private methods to execute request.
   */
  public void executeRequestLocal(JSONObject json) {

  }

}
