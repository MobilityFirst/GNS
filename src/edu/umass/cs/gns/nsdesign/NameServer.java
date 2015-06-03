/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.nsdesign;

import edu.umass.cs.gns.util.Shutdownable;
import edu.umass.cs.gns.nodeconfig.GNSNodeConfig;
import edu.umass.cs.gns.database.MongoRecords;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nsdesign.activeReconfiguration.ActiveReplica;
import edu.umass.cs.gns.nsdesign.gnsReconfigurable.GnsReconfigurable;
import edu.umass.cs.gns.nsdesign.gnsReconfigurable.GnsReconfigurableInterface;
import edu.umass.cs.gns.nsdesign.replicaController.NoCoordinationReplicaControllerCoordinator;
import edu.umass.cs.gns.nsdesign.replicaController.ReplicaController;
import edu.umass.cs.gns.nsdesign.replicaController.ReplicaControllerCoordinatorPaxos;
import edu.umass.cs.gns.paxos.PaxosConfig;
import edu.umass.cs.gns.nsdesign.replicaCoordination.ActiveReplicaCoordinator;
import edu.umass.cs.gns.nsdesign.replicaCoordination.ReplicaControllerCoordinator;
import edu.umass.cs.gns.util.GnsMessenger;
import edu.umass.cs.nio.InterfaceJSONNIOTransport;
import edu.umass.cs.nio.JSONDelayEmulator;
import edu.umass.cs.nio.JSONMessageExtractor;
import edu.umass.cs.nio.JSONNIOTransport;

import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 * This class represents a name server in GNS. It contains an GnsReconfigurable and a ReplicaController object
 * to handle functionality related to active replica and replica controller respectively.
 * <p>
 * The code in this class is used only during initializing the system. Thereafter, replica controller and active
 * replica handle all functionality.
 * <p>
 * Created by abhigyan on 2/26/14.
 */
@Deprecated
public class NameServer<NodeIDType> implements Shutdownable {

  private final ScheduledThreadPoolExecutor executorService = new ScheduledThreadPoolExecutor(10); // worker thread pool

  private ActiveReplicaCoordinator appCoordinator; // coordinates app's requests

  private ActiveReplica<NodeIDType, ?> activeReplica; // reconfiguration logic

  private ReplicaControllerCoordinator replicaControllerCoordinator; // replica control logic

  private ReplicaController<NodeIDType> replicaController;

  private NSListenerAdmin nsListenerAdmin;

  /**
   * This one is just kept around so we can shut it down at the end.
   */
  private GnsReconfigurableInterface<NodeIDType> gnsReconfigurable;

  /**
   * This one is just kept around so we can shut it down at the end.
   */
  private InterfaceJSONNIOTransport<NodeIDType> tcpTransport;

  /**
   * This one is just kept around so we can shut it down at the end.
   */
  private MongoRecords<NodeIDType> mongoRecords;

  /**
   * Constructor for name server object. It takes the list of parameters as a <code>HashMap</code> whose keys
   * are parameter names and values are parameter values. Parameter values are <code>String</code> objects.
   *
   * @param nodeID ID of this name server
   * @param configParameters Config file with parameters and values
   * @param gnsNodeConfig <code>GNSNodeConfig</code> containing ID, IP, port, ping latency of all nodes
   * @throws java.io.IOException
   */
  public NameServer(NodeIDType nodeID, HashMap<String, String> configParameters, GNSNodeConfig<NodeIDType> gnsNodeConfig) throws IOException {
    init(nodeID, configParameters, gnsNodeConfig);
  }

  /**
   * This method actually does the work. It starts a listening socket at the name server for incoming messages,
   * and creates <code>GnsReconfigurable</code> and <code>ReplicaController</code> objects.
   *
   * @throws IOException
   *
   * NIOTransport will create an additional listening thread. threadPoolExecutor will create a few more shared
   * pool of threads.
   */
  private void init(final NodeIDType nodeID, HashMap<String, String> configParameters, GNSNodeConfig<NodeIDType> gnsNodeConfig) throws IOException {
    // set to false to cancel non-periodic delayed tasks upon shutdown
    GNS.getLogger().info("Begin name server initialization for " + nodeID.toString());
    this.executorService.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);

    // init transport
    NSPacketDemultiplexer<NodeIDType> nsDemultiplexer = new NSPacketDemultiplexer<NodeIDType>(this, nodeID);
    if (Config.emulatePingLatencies) {
      JSONDelayEmulator.emulateConfigFileDelays(gnsNodeConfig, Config.latencyVariation);
      GNS.getLogger().info(nodeID.toString() + " Emulating delays ... ");
    }
    JSONMessageExtractor messageExtractor = new JSONMessageExtractor(nsDemultiplexer);
    JSONNIOTransport<NodeIDType> gnsnioTransport = new JSONNIOTransport<NodeIDType>(nodeID, gnsNodeConfig, messageExtractor);
    new Thread(gnsnioTransport).start();
    tcpTransport = new GnsMessenger<NodeIDType>(nodeID, gnsnioTransport, executorService);

    // init DB
    mongoRecords = new MongoRecords<NodeIDType>(nodeID, Config.mongoPort);

    gnsReconfigurable = new GnsReconfigurable<NodeIDType>(nodeID, gnsNodeConfig, tcpTransport, mongoRecords);

    GNS.getLogger().info(nodeID.toString() + " GNS initialized");
    // reInitialize active replica with the app
    activeReplica = new ActiveReplica(nodeID, gnsNodeConfig, tcpTransport, executorService, gnsReconfigurable);
    GNS.getLogger().info(nodeID.toString() + " Active replica initialized");

    // we create app coordinator inside constructor for activeReplica because of cyclic dependency between them
    appCoordinator = activeReplica.getCoordinator();
    GNS.getLogger().info(nodeID.toString() + " App (GNS) coordinator initialized");

    replicaController = new ReplicaController<NodeIDType>(nodeID, gnsNodeConfig, tcpTransport,
            executorService, mongoRecords);
    GNS.getLogger().info(nodeID.toString() + " Replica controller initialized");

    if (Config.singleNS) {
      replicaControllerCoordinator = new NoCoordinationReplicaControllerCoordinator<NodeIDType>(nodeID, gnsNodeConfig, replicaController);
    } else {
      PaxosConfig paxosConfig = new PaxosConfig();
      paxosConfig.setDebugMode(Config.debuggingEnabled);
      paxosConfig.setPaxosLogFolder(Config.paxosLogFolder + "/NODE" + nodeID.toString() + "/replicaController");
      paxosConfig.setFailureDetectionPingMillis(Config.failureDetectionPingSec * 1000);
      paxosConfig.setFailureDetectionTimeoutMillis(Config.failureDetectionTimeoutSec * 1000);
      replicaControllerCoordinator = new ReplicaControllerCoordinatorPaxos<NodeIDType>(nodeID, tcpTransport,
              gnsNodeConfig, replicaController, paxosConfig);
    }
    GNS.getLogger().info(nodeID.toString() + " Replica controller coordinator initialized");

    // start the NSListenerAdmin thread
    nsListenerAdmin = new NSListenerAdmin(gnsReconfigurable, appCoordinator, replicaController, replicaControllerCoordinator, gnsNodeConfig);
    nsListenerAdmin.start();

    GNS.getLogger().info(nodeID.toString() + " Admin thread initialized");
  }

  public ActiveReplicaCoordinator getActiveReplicaCoordinator() {
    return appCoordinator;
  }

  public ActiveReplica<?, ?> getActiveReplica() {
    return activeReplica;
  }

  public ReplicaControllerCoordinator getReplicaControllerCoordinator() {
    return replicaControllerCoordinator;
  }

  public ScheduledThreadPoolExecutor getExecutorService() {
    return executorService;
  }
//
//  public GnsReconfigurableInterface getGnsReconfigurable() {
//    return gnsReconfigurable;
//  }

  /**
   * * Shutdown a name server by closing different components in.
   */
  @Override
  public void shutdown() {
    tcpTransport.stop();
    executorService.shutdown();
    try {
      executorService.awaitTermination(10, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    assert executorService.isTerminated();
    gnsReconfigurable.shutdown();
    appCoordinator.shutdown();
    activeReplica.shutdown();
    replicaControllerCoordinator.shutdown();
    replicaController.shutdown();
    nsListenerAdmin.shutdown();
    mongoRecords.close();

  }
}
