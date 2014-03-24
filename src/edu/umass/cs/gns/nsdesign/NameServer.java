package edu.umass.cs.gns.nsdesign;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nio.GNSNIOTransport;
import edu.umass.cs.gns.nio.JSONMessageWorker;
import edu.umass.cs.gns.nsdesign.activeReplica.ActiveReplica;
import edu.umass.cs.gns.nsdesign.activeReplica.ActiveReplicaInterface;
import edu.umass.cs.gns.nsdesign.replicaController.ReplicaController;
import edu.umass.cs.gns.nsdesign.replicaController.ReplicaControllerInterface;
import edu.umass.cs.gns.util.ConsistentHashing;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Properties;
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

  private int numReplicaControllers = 1;

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
    if (f.exists() == false) {
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
  public NameServer(int nodeID, HashMap<String, String> configParameters, GNSNodeConfig gnsNodeConfig) throws IOException{

    init(nodeID, configParameters, gnsNodeConfig);

  }

  /**
   * This methods actually does the work. It start a listening socket at the name server for incoming messages,
   * and creates <code>ActiveReplica</code> and <code>ReplicaController</code> objects.
   * @throws IOException
   */
  private void init(int nodeID, HashMap<String, String> configParameters, GNSNodeConfig gnsNodeConfig) throws IOException{
    // create nio server

    if (configParameters.containsKey(NSParameterNames.PRIMARY_REPLICAS))
      numReplicaControllers = Integer.parseInt(configParameters.get(NSParameterNames.PRIMARY_REPLICAS));
    GNS.numPrimaryReplicas = numReplicaControllers; // setting it there in case someone is reading that field.
    ConsistentHashing.initialize(numReplicaControllers, gnsNodeConfig.getNumberOfNameServers());

    NSPacketDemultiplexer nsDemultiplexer = new NSPacketDemultiplexer(this);

    JSONMessageWorker worker = new JSONMessageWorker(nsDemultiplexer);
    GNSNIOTransport tcpTransport = new GNSNIOTransport(nodeID, gnsNodeConfig, worker);
    new Thread(tcpTransport).start();

    int numThreads = 5;
    ScheduledThreadPoolExecutor threadPoolExecutor = new ScheduledThreadPoolExecutor(numThreads);

    // be careful to give same 'nodeID' to active replica and replica controller!

    activeReplica = new ActiveReplica(nodeID, configParameters, gnsNodeConfig, tcpTransport, threadPoolExecutor);

    replicaController = new ReplicaController(nodeID, configParameters, gnsNodeConfig, tcpTransport, threadPoolExecutor);
  }

  @Override
  public ActiveReplicaInterface getActiveReplica() {
    return activeReplica;
  }

  @Override
  public ReplicaControllerInterface getReplicaController() {
    return replicaController;
  }

  public void resetDB() {
    activeReplica.resetDB();
    replicaController.resetDB();
  }
}
