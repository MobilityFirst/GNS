package edu.umass.cs.gns.nsdesign;

import edu.umass.cs.gns.nio.GNSNIOTransport;
import edu.umass.cs.gns.nio.JSONMessageWorker;
import edu.umass.cs.gns.nsdesign.activeReplica.ActiveReplica;
import edu.umass.cs.gns.nsdesign.activeReplica.ActiveReplicaInterface;
import edu.umass.cs.gns.nsdesign.replicaController.ReplicaController;
import edu.umass.cs.gns.nsdesign.replicaController.ReplicaControllerInterface;

import java.io.IOException;
import java.util.concurrent.ScheduledThreadPoolExecutor;

/*** DONT not use any class in package edu.umass.cs.gns.nsdesign ***/

/**
 * Work in progress. Inactive code.
 * <p>
 * This class represents a name server in GNS. It contains an ActiveReplica and a ReplicaController object
 * to handle functionality related to active replica and replica controller respectively.
 * <p>
 * The code in this class is used only during initializing the system. Thereafter, replica controller and active
 * replica handle all functionality.
 * <p>
 * Created by abhigyan on 2/26/14.
 */
public class NameServer implements NameServerInterface{


  private ActiveReplicaInterface activeReplica;

  private ReplicaControllerInterface replicaController;

  /**
   * Constructor for name server object.
   * @param nodeID ID of this name server
   * @param configFile  Config file with parameters and values
   * @param nodeConfigFile Config file containing ID, IP, port, ping latency of all name servers and local name servers
   */
  public NameServer(int nodeID, String configFile, String nodeConfigFile) throws IOException{

    // create nio server

    GNSNodeConfig gnsNodeConfig = new GNSNodeConfig(nodeConfigFile, nodeID);

    NSPacketDemultiplexer nsDemultiplexer = new NSPacketDemultiplexer(this);

    JSONMessageWorker worker = new JSONMessageWorker(nsDemultiplexer);
    GNSNIOTransport tcpTransport = new GNSNIOTransport(nodeID, gnsNodeConfig, worker);
    new Thread(tcpTransport).start();

    // create executor service
    int numThreads = 5;
    ScheduledThreadPoolExecutor threadPoolExecutor = new ScheduledThreadPoolExecutor(numThreads);

    // be careful to give same 'nodeID' to active replica and replica controller!

    // create active replica (nio, executor)
    activeReplica = new ActiveReplica(nodeID, configFile, gnsNodeConfig, tcpTransport, threadPoolExecutor);

    // create replica controller object (nio, executor)
    replicaController = new ReplicaController(nodeID, configFile, gnsNodeConfig, tcpTransport, threadPoolExecutor);

  }


  @Override
  public ActiveReplicaInterface getActiveReplica() {
    return activeReplica;
  }

  @Override
  public ReplicaControllerInterface getReplicaController() {
    return replicaController;
  }
}
