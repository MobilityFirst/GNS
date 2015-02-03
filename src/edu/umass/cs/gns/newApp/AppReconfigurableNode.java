package edu.umass.cs.gns.newApp;

import edu.umass.cs.gns.database.MongoRecords;
import edu.umass.cs.gns.nsdesign.Config;
import java.io.IOException;
import edu.umass.cs.gns.reconfiguration.AbstractReplicaCoordinator;
import edu.umass.cs.gns.reconfiguration.InterfaceReconfigurableNodeConfig;
import edu.umass.cs.gns.reconfiguration.ReconfigurableNode;
import edu.umass.cs.gns.reconfiguration.examples.ReconfigurableSampleNodeConfig;
import edu.umass.cs.gns.reconfiguration.examples.TestConfig;

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
  public static void main(String[] args) {
    ReconfigurableSampleNodeConfig nc = new ReconfigurableSampleNodeConfig();
    nc.localSetup(TestConfig.getNodes());
    try {
      System.out.println("Setting up actives at " + nc.getActiveReplicas());
      for (int activeID : nc.getActiveReplicas()) {
        new AppReconfigurableNode(activeID, nc);
      }
      System.out.println("Setting up RCs at " + nc.getReconfigurators());
      for (int rcID : nc.getReconfigurators()) {
        new AppReconfigurableNode(rcID, nc);
      }

    } catch (IOException ioe) {
      ioe.printStackTrace();
    }
  }
}
