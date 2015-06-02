package edu.umass.cs.gns.newApp;

import edu.umass.cs.gns.database.MongoRecords;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nodeconfig.GNSInterfaceNodeConfig;
import edu.umass.cs.gns.nodeconfig.GNSNodeConfig;
import static edu.umass.cs.gns.newApp.AppReconfigurableNodeOptions.*;
import java.io.IOException;
import edu.umass.cs.gns.reconfiguration.AbstractReplicaCoordinator;
import edu.umass.cs.gns.reconfiguration.ReconfigurableNode;
import edu.umass.cs.gns.util.ParametersAndOptions;
import static edu.umass.cs.gns.util.ParametersAndOptions.HELP;
import static edu.umass.cs.gns.util.ParametersAndOptions.printOptions;
import java.util.Map;
import java.util.Set;

/**
 * @author Westy
 */
public class AppReconfigurableNode extends ReconfigurableNode<String> {

  private MongoRecords<String> mongoRecords = null;
  
  public AppReconfigurableNode(String nodeID, GNSInterfaceNodeConfig<String> nc)
          throws IOException {
    super(nodeID, nc);
  }

  @Override
  protected AbstractReplicaCoordinator<String> createAppCoordinator() {
    // this is called by super so we need to get this field initialized now
    if (this.mongoRecords == null) {
      this.mongoRecords = new MongoRecords<>(this.myID, AppReconfigurableNodeOptions.mongoPort);
    }
    NewApp app = new NewApp(this.myID, (GNSInterfaceNodeConfig) this.nodeConfig, this.messenger, mongoRecords);

    NewAppCoordinator appCoordinator = new NewAppCoordinator(app, this.nodeConfig, this.messenger);

    // start the NSListenerAdmin thread
    new AppAdmin(app, (GNSNodeConfig) nodeConfig).start();

    GNS.getLogger().info(myID.toString() + " Admin thread initialized");

    return appCoordinator;
  }

  private static void startNodePair(String nodeID, String nodeConfigFilename) throws IOException {
    GNSNodeConfig nodeConfig = new GNSNodeConfig(nodeConfigFilename, nodeID);
    new AppReconfigurableNode((String)nodeConfig.getReplicaNodeIdForTopLevelNode(nodeID), nodeConfig);
    new AppReconfigurableNode((String)nodeConfig.getReconfiguratorNodeIdForTopLevelNode(nodeID), nodeConfig);
  }

  private static void startTestNodes(String filename) throws IOException {
    GNSNodeConfig nodeConfig = new GNSNodeConfig(filename, true);
    try {
      for (String activeID : (Set<String>) nodeConfig.getActiveReplicas()) {
        System.out.println("#####################################################");
        System.out.println("############# Setting up active replica " + activeID);
        new AppReconfigurableNode(activeID, nodeConfig);
      }
      for (String rcID : (Set<String>) nodeConfig.getReconfigurators()) {
        System.out.println("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$");
        System.out.println("$$$$$$$$$$$$$$$$ Setting up reconfigurator " + rcID);
        new AppReconfigurableNode(rcID, nodeConfig);
      }

    } catch (IOException ioe) {
      ioe.printStackTrace();
    }
  }

  public static void main(String[] args) throws IOException {
    Map<String, String> options
            = ParametersAndOptions.getParametersAsHashMap(AppReconfigurableNode.class.getCanonicalName(),
                    AppReconfigurableNodeOptions.getAllOptions(), args);
    if (options.containsKey(HELP)) {
      ParametersAndOptions.printUsage(AppReconfigurableNode.class.getCanonicalName(),
              AppReconfigurableNodeOptions.getAllOptions());
      System.exit(0);
    }
    printOptions(options);
    AppReconfigurableNodeOptions.initializeFromOptions(options);
    if (options.get(TEST) != null && options.get(NS_FILE) != null) { // for testing
      startTestNodes(options.get(NS_FILE));
    } else if (options.get(ID) != null && options.get(NS_FILE) != null) {
      startNodePair(options.get(ID), options.get(NS_FILE));
    } else {
      ParametersAndOptions.printUsage(AppReconfigurableNode.class.getCanonicalName(),
              AppReconfigurableNodeOptions.getAllOptions());
      System.exit(0);
    }
  }
}
