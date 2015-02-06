package edu.umass.cs.gns.newApp;

import edu.umass.cs.gns.database.MongoRecords;
import edu.umass.cs.gns.nsdesign.Config;
import edu.umass.cs.gns.nsdesign.nodeconfig.GNSNodeConfig;
import java.io.IOException;
import edu.umass.cs.gns.reconfiguration.AbstractReplicaCoordinator;
import edu.umass.cs.gns.reconfiguration.InterfaceReconfigurableNodeConfig;
import edu.umass.cs.gns.reconfiguration.ReconfigurableNode;
import java.util.Set;

/**
 * @author Westy
 * @param <NodeIDType>
 */
public class AppReconfigurableNode<NodeIDType> extends ReconfigurableNode<NodeIDType> {
  
  private final MongoRecords<NodeIDType> mongoRecords;

  public AppReconfigurableNode(NodeIDType nodeID, InterfaceReconfigurableNodeConfig<NodeIDType> nc)
          throws IOException {
    super(nodeID, nc);
    this.mongoRecords = new MongoRecords<>(nodeID, Config.mongoPort);
  }

  @Override
  protected AbstractReplicaCoordinator<NodeIDType> createAppCoordinator() {
    App app = new App(this.myID, this.nodeConfig, this.messenger, mongoRecords);
    //NoopAppCoordinator appCoordinator = new NoopAppCoordinator(app);
    AppCoordinator appCoordinator = new AppCoordinator(app, this.nodeConfig, this.messenger);
    return appCoordinator;
  }

  // local setup
  public static void main(String[] args) throws IOException {
    // change this to use GNSNodeConfig
    String filename = Config.WESTY_GNS_DIR_PATH + "/conf/name-server-info";
    GNSNodeConfig nodeConfig = new GNSNodeConfig(filename, true);
    try {
      System.out.println("Setting up actives at " + nodeConfig.getActiveReplicas());
      for (String activeID : (Set<String>) nodeConfig.getActiveReplicas()) {
        new AppReconfigurableNode(activeID, nodeConfig);
      }
      System.out.println("Setting up RCs at " + nodeConfig.getReconfigurators());
      for (String rcID : (Set<String>) nodeConfig.getReconfigurators()) {
        new AppReconfigurableNode(rcID, nodeConfig);
      }

    } catch (IOException ioe) {
      ioe.printStackTrace();
    }
  }
}
