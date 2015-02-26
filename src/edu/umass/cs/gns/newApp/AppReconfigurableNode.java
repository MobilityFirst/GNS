package edu.umass.cs.gns.newApp;

import edu.umass.cs.gns.database.MongoRecords;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nsdesign.Config;
import edu.umass.cs.gns.nsdesign.nodeconfig.GNSNodeConfig;
import java.io.IOException;
import edu.umass.cs.gns.reconfiguration.AbstractReplicaCoordinator;
import edu.umass.cs.gns.reconfiguration.InterfaceReconfigurableNodeConfig;
import edu.umass.cs.gns.reconfiguration.ReconfigurableNode;
import edu.umass.cs.gns.reconfiguration.ReconfigurationConfig;
import java.util.Set;

/**
 * @author Westy
 * @param <NodeIDType>
 */
public class AppReconfigurableNode<NodeIDType> extends ReconfigurableNode<NodeIDType> {

  private MongoRecords<NodeIDType> mongoRecords = null;

  public AppReconfigurableNode(NodeIDType nodeID, InterfaceReconfigurableNodeConfig<NodeIDType> nc)
          throws IOException {
    super(nodeID, nc);

  }

  @Override
  protected AbstractReplicaCoordinator<NodeIDType> createAppCoordinator() {
    // this is called by super so we need to get this field initialized now
    if (this.mongoRecords == null) {
      this.mongoRecords = new MongoRecords<>(this.myID, Config.mongoPort);
    }
    App app = new App(this.myID, this.nodeConfig, this.messenger, mongoRecords);

    AppCoordinator appCoordinator = new AppCoordinator(app, this.nodeConfig, this.messenger);

    // start the NSListenerAdmin thread
    new AppAdmin(app, (GNSNodeConfig) nodeConfig).start();

    GNS.getLogger().info(myID.toString() + " Admin thread initialized");

    return appCoordinator;
  }

  // local setup
  public static void main(String[] args) throws IOException {
    // Use the NullDemandProfile until we figure out the issues with the default one.
    // FIXME: Still doesn't work yet.
    ReconfigurationConfig.setDemandProfile(NullDemandProfile.class);
    String filename = Config.WESTY_GNS_DIR_PATH + "/conf/name-server-info";
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
}
