/*
 *
 *  Copyright (c) 2015 University of Massachusetts
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you
 *  may not use this file except in compliance with the License. You
 *  may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 *  implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 *
 *  Initial developer(s): Abhigyan Sharma, Westy
 *
 */
package edu.umass.cs.gnsserver.gnsApp;

import edu.umass.cs.gnsserver.main.GNS;
import edu.umass.cs.gnsserver.nodeconfig.GNSInterfaceNodeConfig;
import edu.umass.cs.gnsserver.nodeconfig.GNSNodeConfig;
import static edu.umass.cs.gnsserver.gnsApp.AppReconfigurableNodeOptions.*;
import java.io.IOException;
import edu.umass.cs.gnsserver.utils.ParametersAndOptions;
import edu.umass.cs.reconfiguration.AbstractReplicaCoordinator;
import edu.umass.cs.reconfiguration.ReconfigurableNode;

import static edu.umass.cs.gnsserver.utils.ParametersAndOptions.printOptions;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Instantiates the replicas and reconfigurators. Also parses
 * all the command line options.
 *
 * @author Westy
 */
public class AppReconfigurableNode extends ReconfigurableNode<String> {
  
  private final static List<AppReconfigurableNode> allNodes = new ArrayList<>();

  //private MongoRecords<String> mongoRecords = null;
  /**
   * Create an AppReconfigurableNode instance.
   *
   * @param nodeID
   * @param nc
   * @throws IOException
   */
  public AppReconfigurableNode(String nodeID, GNSInterfaceNodeConfig<String> nc)
          throws IOException {
    super(nodeID, nc);
  }

  /**
   * Create and returns an app coordinator.
   *
   * @return a coordinator or null if one can't be created
   */
  @Override
  protected AbstractReplicaCoordinator<String> createAppCoordinator() {
    GnsApp app = null;
    try {
      app = new GnsApp(this.myID, (GNSNodeConfig<String>) this.nodeConfig,
              this.messenger);
    } catch (IOException e) {
      GNS.getLogger().info("Unable to create app: " + e);
      // not sure what to do here other than just return null
      return null;
    }

    GnsAppCoordinator<String> appCoordinator = new GnsAppCoordinator<String>(app, this.nodeConfig, this.messenger);
    return appCoordinator;

  }

  private static void startNodePair(String nodeID, String nodeConfigFilename) throws IOException {
    GNSNodeConfig<String> nodeConfig = new GNSNodeConfig<String>(nodeConfigFilename, nodeID);
    System.out.println("********* Starting active replica. *********");
    allNodes.add(new AppReconfigurableNode((String) nodeConfig.getReplicaNodeIdForTopLevelNode(nodeID), nodeConfig));
    System.out.println("********* Starting reconfigurator. *********");
    allNodes.add(new AppReconfigurableNode((String) nodeConfig.getReconfiguratorNodeIdForTopLevelNode(nodeID), nodeConfig));
    System.out.println("********* Nodes have started. Server is ready. *********");
  }

  private static void startStandalone(String nodeConfigFilename) throws IOException {
    GNSNodeConfig<String> nodeConfig = new GNSNodeConfig<String>(nodeConfigFilename, true);
    String nodeID = (String) nodeConfig.getActiveReplicas().iterator().next();
    GNS.getLogger().info("Starting standalone node " + nodeID);
    allNodes.add(new AppReconfigurableNode(nodeID, nodeConfig));
  }

  private static void startTestNodes(String nodeConfigFilename) throws IOException {
    GNSNodeConfig<String> nodeConfig = new GNSNodeConfig<String>(nodeConfigFilename, true);
    try {
      for (String activeID : (Set<String>) nodeConfig.getActiveReplicas()) {
        System.out.println("########### Multi-node test #############");
        System.out.println("###################################");
        System.out.println("############# Setting up active replica " + activeID);
        allNodes.add(new AppReconfigurableNode(activeID, nodeConfig));
      }
      for (String rcID : (Set<String>) nodeConfig.getReconfigurators()) {
        System.out.println("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$");
        System.out.println("$$$$$$$$$$$$$$$$ Setting up reconfigurator " + rcID);
        allNodes.add(new AppReconfigurableNode(rcID, nodeConfig));
      }
      System.out.println("********* Nodes have started. Server is ready. *********");

    } catch (IOException ioe) {
      ioe.printStackTrace();
    }
  }

  /**
   * Parses all the command line arguments and invoke methods
   * to create replicas and reconfigurators.
   *
   * @param args
   * @throws IOException
   */
  public static void main(String[] args) throws IOException {
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        for (AppReconfigurableNode node : allNodes) {
          System.out.println("Shutting down " + node.myID);
          node.close();
        }
        System.out.println("********* All nodes have been shutdown. *********");
      }
    });
    Map<String, String> options
            = ParametersAndOptions.getParametersAsHashMap(AppReconfigurableNode.class.getCanonicalName(),
                    AppReconfigurableNodeOptions.getAllOptions(), args);
    printOptions(options);
    AppReconfigurableNodeOptions.initializeFromOptions(options);
    if (options.containsKey(STANDALONE) && options.get(NS_FILE) != null) {
      startStandalone(options.get(NS_FILE));
    } else if (options.get(TEST) != null && options.get(NS_FILE) != null) { // for testing
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
