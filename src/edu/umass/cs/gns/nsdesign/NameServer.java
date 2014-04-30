package edu.umass.cs.gns.nsdesign;

import edu.umass.cs.gns.database.MongoRecords;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nio.*;
import edu.umass.cs.gns.nsdesign.activeReconfiguration.ActiveReplica;
import edu.umass.cs.gns.nsdesign.gnsReconfigurable.GnsReconfigurable;
import edu.umass.cs.gns.nsdesign.replicaController.DefaultRcCoordinator;
import edu.umass.cs.gns.nsdesign.replicaController.ReplicaController;
import edu.umass.cs.gns.nsdesign.replicaController.ReplicaControllerCoordinatorPaxos;
import edu.umass.cs.gns.paxos.PaxosConfig;
import edu.umass.cs.gns.replicaCoordination.ActiveReplicaCoordinator;
import edu.umass.cs.gns.replicaCoordination.ReplicaControllerCoordinator;
import edu.umass.cs.gns.util.ConsistentHashing;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.ScheduledThreadPoolExecutor;


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
public class NameServer{

  private ScheduledThreadPoolExecutor executorService = new ScheduledThreadPoolExecutor(5); // worker thread pool

  private ActiveReplicaCoordinator  appCoordinator; // coordinates app's requests
 
  private ActiveReplica activeReplica; // reconfiguration logic
 
  private ReplicaControllerCoordinator replicaControllerCoordinator; // replica control logic
  
  private GnsReconfigurable gnsReconfigurable;

  /**
   * Constructor for name server object. It takes the list of parameters as a config file.
   * @param nodeID ID of this name server
   * @param configFile  Config file with parameters and values
   * @param gnsNodeConfig <code>GNSNodeConfig</code> containing ID, IP, port, ping latency of all nodes
   */
  public NameServer(int nodeID, String configFile, GNSNodeConfig gnsNodeConfig) throws IOException{
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
    for (String propertyName: prop.stringPropertyNames()) {
      allValues.put(propertyName, prop.getProperty(propertyName));
    }
    init(nodeID, allValues, gnsNodeConfig);
  }

  /**
   * Constructor for name server object. It takes the list of parameters as a <code>HashMap</code> whose keys
   * are parameter names and values are parameter values. Parameter values are <code>String</code> objects.
   * @param nodeID ID of this name server
   * @param configParameters  Config file with parameters and values
   * @param gnsNodeConfig <code>GNSNodeConfig</code> containing ID, IP, port, ping latency of all nodes
   */
  public NameServer(int nodeID, HashMap<String, String> configParameters, GNSNodeConfig gnsNodeConfig) throws IOException {

    init(nodeID, configParameters, gnsNodeConfig);

  }

  /**
   * This methods actually does the work. It start a listening socket at the name server for incoming messages,
   * and creates <code>GnsReconfigurable</code> and <code>ReplicaController</code> objects.
   * @throws IOException
   * 
   * NIOTransport will create an additional listening thread. threadPoolExecutor will create a few more shared
   * pool of threads.
   */
  private void init(int nodeID, HashMap<String, String> configParameters, GNSNodeConfig gnsNodeConfig) throws IOException{
    // create nio server

//    GNS.numPrimaryReplicas = numReplicaControllers; // setting it there in case someone is reading that field.
    ConsistentHashing.initialize(GNS.numPrimaryReplicas, gnsNodeConfig.getNameServerIDs());

    // init transport
    NSPacketDemultiplexer nsDemultiplexer = new NSPacketDemultiplexer(this);
    if (Config.emulatePingLatencies) GNSDelayEmulator.emulateConfigFileDelays(gnsNodeConfig, Config.latencyVariation);
    GNSNIOTransportInterface tcpTransport;
    if (Config.useGNSNIOTransport) {
      JSONMessageExtractor worker = new JSONMessageExtractor(nsDemultiplexer);
      tcpTransport = new GNSNIOTransport(nodeID, gnsNodeConfig, worker);
    } else {
      ByteStreamToJSONObjects byteToJson = new ByteStreamToJSONObjects(nsDemultiplexer);
      tcpTransport = new NioServer(nodeID, byteToJson, gnsNodeConfig);
    }
    new Thread(tcpTransport).start();

    // init worker thread pool
    int numThreads = 5;
    ScheduledThreadPoolExecutor threadPoolExecutor = new ScheduledThreadPoolExecutor(numThreads);

    // be careful to give same 'nodeID' to everyone

    // init DB
    MongoRecords mongoRecords = new MongoRecords(nodeID, Config.mongoPort);

    // initialize GNS
    GnsReconfigurable gnsReconfigurable = new GnsReconfigurable(nodeID, configParameters, gnsNodeConfig, tcpTransport,
            threadPoolExecutor, mongoRecords);
    GNS.getLogger().info("GNS initialized. ");
    // initialize active replica with the app
    activeReplica  = new ActiveReplica(nodeID, configParameters, gnsNodeConfig, tcpTransport, threadPoolExecutor,
            gnsReconfigurable);
    GNS.getLogger().info("Active replica initialized");

    // we create app coordinator inside constructor for activeReplica because of cyclic dependency between them
    appCoordinator  = activeReplica.getCoordinator();
    GNS.getLogger().info("App (GNS) coordinator initialized");

    ReplicaController rc = new ReplicaController(nodeID, configParameters, gnsNodeConfig, tcpTransport,
            threadPoolExecutor, mongoRecords);
    GNS.getLogger().info("Replica controller initialized");

    if (Config.singleNS) {
      replicaControllerCoordinator = new DefaultRcCoordinator(nodeID, rc);
    } else {
      PaxosConfig paxosConfig = new PaxosConfig();
      paxosConfig.setPaxosLogFolder(Config.paxosLogFolder + "/replicaController");
      paxosConfig.setFailureDetectionPingMillis(Config.failureDetectionPingSec * 1000);
      paxosConfig.setFailureDetectionTimeoutMillis(Config.failureDetectionTimeoutSec * 1000);
      replicaControllerCoordinator = new ReplicaControllerCoordinatorPaxos(nodeID, tcpTransport,
              new NSNodeConfig(gnsNodeConfig), rc, paxosConfig);
    }
    GNS.getLogger().info("Replica controller coordinator initialized");

    // start the NSListenerAdmin thread
    new NSListenerAdmin(gnsReconfigurable, appCoordinator, rc, replicaControllerCoordinator, gnsNodeConfig).start();
    GNS.getLogger().info("Admin thread initialized");
  }

  public ActiveReplicaCoordinator getActiveReplicaCoordinator() {
    return appCoordinator;
  }

  public ActiveReplica getActiveReplica() {
    return activeReplica;
  }

  public ReplicaControllerCoordinator getReplicaControllerCoordinator() {
    return replicaControllerCoordinator;
  }


  public ScheduledThreadPoolExecutor getExecutorService() {
    return executorService;
  }

  public GnsReconfigurable getGnsReconfigurable() {
    return gnsReconfigurable;
  }
  
}
