/*
 * Copyright (C) 2014
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.nsdesign;

import edu.umass.cs.gns.nsdesign.nodeconfig.GNSNodeConfig;
import edu.umass.cs.gns.database.MongoRecords;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nio.InterfaceJSONNIOTransport;
import edu.umass.cs.gns.nio.JSONDelayEmulator;
import edu.umass.cs.gns.nio.JSONMessageExtractor;
import edu.umass.cs.gns.nio.JSONNIOTransport;
import edu.umass.cs.gns.nsdesign.activeReconfiguration.ActiveReplica;
import edu.umass.cs.gns.nsdesign.gnsReconfigurable.DummyGnsReconfigurable;
import edu.umass.cs.gns.nsdesign.gnsReconfigurable.GnsReconfigurable;
import edu.umass.cs.gns.nsdesign.gnsReconfigurable.GnsReconfigurableInterface;
import edu.umass.cs.gns.nsdesign.nodeconfig.NodeId;
import edu.umass.cs.gns.nsdesign.replicaController.DefaultRcCoordinator;
import edu.umass.cs.gns.nsdesign.replicaController.ReplicaController;
import edu.umass.cs.gns.nsdesign.replicaController.ReplicaControllerCoordinatorPaxos;
import edu.umass.cs.gns.paxos.PaxosConfig;
import edu.umass.cs.gns.replicaCoordination.ActiveReplicaCoordinator;
import edu.umass.cs.gns.replicaCoordination.ReplicaControllerCoordinator;
import edu.umass.cs.gns.util.ConsistentHashing;
import edu.umass.cs.gns.util.GnsMessenger;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Properties;
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
public class NameServer implements Shutdownable {

  private ScheduledThreadPoolExecutor executorService = new ScheduledThreadPoolExecutor(10); // worker thread pool

  private ActiveReplicaCoordinator appCoordinator; // coordinates app's requests

  private ActiveReplica<?> activeReplica; // reconfiguration logic

  private ReplicaControllerCoordinator replicaControllerCoordinator; // replica control logic

  private ReplicaController replicaController;

  private NSListenerAdmin admin;

  private GnsReconfigurableInterface gnsReconfigurable;

  private InterfaceJSONNIOTransport tcpTransport;

  private MongoRecords mongoRecords;

  /**
   * Constructor for name server object. It takes the list of parameters as a config file.
   *
   * @param nodeID ID of this name server
   * @param configFile Config file with parameters and values
   * @param gnsNodeConfig <code>GNSNodeConfig</code> containing ID, IP, port, ping latency of all nodes
   */
  public NameServer(NodeId<String> nodeID, String configFile, GNSNodeConfig gnsNodeConfig) throws IOException {

    // load options given in config file in a java properties object
    Properties prop = new Properties();

    File f = new File(configFile);
    if (!f.exists()) {
      System.err.println("Config file not found:" + configFile);
      System.exit(2);
    }
    InputStream input = new FileInputStream(configFile);
    // load a properties file
    prop.load(input);

    // create a hash map with all options including options in config file
    HashMap<String, String> allValues = new HashMap<String, String>();
    for (String propertyName : prop.stringPropertyNames()) {
      allValues.put(propertyName, prop.getProperty(propertyName));
    }
    init(nodeID, allValues, gnsNodeConfig);
  }

  /**
   * Constructor for name server object. It takes the list of parameters as a <code>HashMap</code> whose keys
   * are parameter names and values are parameter values. Parameter values are <code>String</code> objects.
   *
   * @param nodeID ID of this name server
   * @param configParameters Config file with parameters and values
   * @param gnsNodeConfig <code>GNSNodeConfig</code> containing ID, IP, port, ping latency of all nodes
   */
  public NameServer(NodeId<String> nodeID, HashMap<String, String> configParameters, GNSNodeConfig gnsNodeConfig) throws IOException {
    init(nodeID, configParameters, gnsNodeConfig);
  }

  /**
   * This methods actually does the work. It start a listening socket at the name server for incoming messages,
   * and creates <code>GnsReconfigurable</code> and <code>ReplicaController</code> objects.
   *
   * @throws IOException
   *
   * NIOTransport will create an additional listening thread. threadPoolExecutor will create a few more shared
   * pool of threads.
   */
  private void init(NodeId<String> nodeID, HashMap<String, String> configParameters, GNSNodeConfig gnsNodeConfig) throws IOException {
    // set to false to cancel non-periodic delayed tasks upon shutdown
    this.executorService.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);

    // init transport
    NSPacketDemultiplexer nsDemultiplexer = new NSPacketDemultiplexer(this, nodeID);
    if (Config.emulatePingLatencies) {
      JSONDelayEmulator.emulateConfigFileDelays(gnsNodeConfig, Config.latencyVariation);
      GNS.getLogger().info("Emulating delays ... ");
    }
    JSONMessageExtractor worker = new JSONMessageExtractor(nsDemultiplexer);
    JSONNIOTransport gnsnioTransport = new JSONNIOTransport(nodeID, gnsNodeConfig, worker);
    new Thread(gnsnioTransport).start();
    tcpTransport = new GnsMessenger(nodeID, gnsnioTransport, executorService);
    // be careful to give same 'nodeID' to everyone

    // init DB
    mongoRecords = new MongoRecords(nodeID, Config.mongoPort);

    // reInitialize GNS
    if (Config.dummyGNS) {
      gnsReconfigurable = new DummyGnsReconfigurable(nodeID, gnsNodeConfig, tcpTransport);
    } else { // real GNS
      gnsReconfigurable = new GnsReconfigurable(nodeID, gnsNodeConfig, tcpTransport, mongoRecords);
    }
    GNS.getLogger().info("GNS initialized");
    // reInitialize active replica with the app
    activeReplica = new ActiveReplica(nodeID, gnsNodeConfig, tcpTransport, executorService, gnsReconfigurable);
    GNS.getLogger().info("Active replica initialized");

    // we create app coordinator inside constructor for activeReplica because of cyclic dependency between them
    appCoordinator = activeReplica.getCoordinator();
    GNS.getLogger().info("App (GNS) coordinator initialized");

    replicaController = new ReplicaController(nodeID, gnsNodeConfig, tcpTransport,
            executorService, mongoRecords);
    GNS.getLogger().info("Replica controller initialized");

    if (Config.singleNS) {
      replicaControllerCoordinator = new DefaultRcCoordinator(nodeID, replicaController);
    } else {
      PaxosConfig paxosConfig = new PaxosConfig();
      paxosConfig.setDebugMode(Config.debuggingEnabled);
      paxosConfig.setPaxosLogFolder(Config.paxosLogFolder + "/replicaController");
      paxosConfig.setFailureDetectionPingMillis(Config.failureDetectionPingSec * 1000);
      paxosConfig.setFailureDetectionTimeoutMillis(Config.failureDetectionTimeoutSec * 1000);
      replicaControllerCoordinator = new ReplicaControllerCoordinatorPaxos(nodeID, tcpTransport,
              new NSNodeConfig(gnsNodeConfig), replicaController, paxosConfig);
    }
    GNS.getLogger().info("Replica controller coordinator initialized");

    // start the NSListenerAdmin thread
    admin = new NSListenerAdmin(gnsReconfigurable, appCoordinator, replicaController, replicaControllerCoordinator, gnsNodeConfig);
    admin.start();

    GNS.getLogger().info("Admin thread initialized");
  }

  public ActiveReplicaCoordinator getActiveReplicaCoordinator() {
    return appCoordinator;
  }

  public ActiveReplica<?> getActiveReplica() {
    return activeReplica;
  }

  public ReplicaControllerCoordinator getReplicaControllerCoordinator() {
    return replicaControllerCoordinator;
  }

  public ScheduledThreadPoolExecutor getExecutorService() {
    return executorService;
  }

  public GnsReconfigurableInterface getGnsReconfigurable() {
    return gnsReconfigurable;
  }

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
    admin.shutdown();
    mongoRecords.close();

  }
}
