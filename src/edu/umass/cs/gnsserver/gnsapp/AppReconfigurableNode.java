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
 *  Initial developer(s): Westy
 *
 */
package edu.umass.cs.gnsserver.gnsapp;

import edu.umass.cs.gnsserver.main.GNSConfig;
import edu.umass.cs.gnsserver.nodeconfig.GNSInterfaceNodeConfig;
import edu.umass.cs.gnsserver.nodeconfig.GNSNodeConfig;
import static edu.umass.cs.gnsserver.gnsapp.AppReconfigurableNodeOptions.*;

import java.io.IOException;

import edu.umass.cs.gnsserver.utils.ParametersAndOptions;
import edu.umass.cs.reconfiguration.AbstractReplicaCoordinator;
import edu.umass.cs.reconfiguration.ReconfigurableNode;
import static edu.umass.cs.gnsserver.utils.ParametersAndOptions.printOptions;
import edu.umass.cs.reconfiguration.ReconfigurationConfig;
import edu.umass.cs.utils.Config;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;

/**
 * Instantiates the replicas and reconfigurators. Also parses
 * all the command line options.
 *
 * @author Westy
 */
public class AppReconfigurableNode extends ReconfigurableNode<String> {

  private final static List<AppReconfigurableNode> ALL_NODES = new ArrayList<>();

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
  @Deprecated
  protected AbstractReplicaCoordinator<String> createAppCoordinator() {
    GNSApp app = null;
    try {
      app = new GNSApp(this.myID, (GNSNodeConfig<String>) this.nodeConfig,
              this.messenger);
    } catch (IOException e) {
      GNSConfig.getLogger().info("Unable to create app: " + e);
      // not sure what to do here other than just return null
      return null;
    }

    GNSAppCoordinator<String> appCoordinator = new GNSAppCoordinator<String>(app, this.nodeConfig, this.messenger);
    return appCoordinator;

  }

  private static void startNodePair(String nodeID, String nodeConfigFilename) throws IOException {
    GNSNodeConfig<String> nodeConfig = new GNSNodeConfig<>(nodeConfigFilename, nodeID);
    System.out.println("********* Starting active replica. *********");
    ALL_NODES.add(new AppReconfigurableNode(nodeConfig.getReplicaNodeIdForTopLevelNode(nodeID), nodeConfig));
    System.out.println("********* Starting reconfigurator. *********");
    ALL_NODES.add(new AppReconfigurableNode(nodeConfig.getReconfiguratorNodeIdForTopLevelNode(nodeID), nodeConfig));
    printRCConfig();
    System.out.println("********* Nodes have started. Server is ready. *********");
  }

  private static void startStandalone(String nodeConfigFilename) throws IOException {
    GNSNodeConfig<String> nodeConfig = new GNSNodeConfig<>(nodeConfigFilename, true);
    String nodeID = nodeConfig.getActiveReplicas().iterator().next();
    GNSConfig.getLogger().log(Level.INFO, "Starting standalone node {0}", nodeID);
    ALL_NODES.add(new AppReconfigurableNode(nodeID, nodeConfig));
  }

  private static void startTestNodes(String nodeConfigFilename) throws IOException {
    GNSNodeConfig<String> nodeConfig = new GNSNodeConfig<>(nodeConfigFilename, true);
    try {
      for (String activeID : nodeConfig.getActiveReplicas()) {
        System.out.println("########### Multi-node test #############");
        System.out.println("###################################");
        System.out.println("############# Setting up active replica " + activeID);
        ALL_NODES.add(new AppReconfigurableNode(activeID, nodeConfig));
      }
      for (String rcID : nodeConfig.getReconfigurators()) {
        System.out.println("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$");
        System.out.println("$$$$$$$$$$$$$$$$ Setting up reconfigurator " + rcID);
        ALL_NODES.add(new AppReconfigurableNode(rcID, nodeConfig));
      }
      printRCConfig();
      System.out.println("********* Nodes have started. Server is ready. *********");

    } catch (IOException ioe) {
      ioe.printStackTrace();
    }
  }

  protected static Map<String, String> initOptions(String[] args)
          throws IOException {
    Map<String, String> options = ParametersAndOptions
            .getParametersAsHashMap(
                    AppReconfigurableNode.class.getCanonicalName(),
                    AppReconfigurableNodeOptions.getAllOptions(), args);
    AppReconfigurableNodeOptions.initializeFromOptions(options);
    printOptions(options);
    return options;
  }

  public static void printRCConfig() {
    StringBuilder result = new StringBuilder();
    for (ReconfigurationConfig.RC rc : ReconfigurationConfig.RC.values()) {
      result.append(rc.toString());
      result.append(" => ");
      result.append(Config.getGlobal(rc));
      result.append("\n");
    }
    System.out.print(result.toString());
  }

  /**
   * Parses all the command line arguments and invoke methods
   * to create replicas and reconfigurators.
   *
   * @param args
   * @throws IOException
   */
  public static void main(String[] args) throws IOException {
    System.out
            .println("*********************************************************\n"
                    + "This mode of starting the GNS is not recommended. Start ReconfigurableNode"
                    + " instead with the node ID(s) being started listed at the end of command-line options,"
                    + " and APPLICATION=edu.umass.cs.gnsserver.GnsApp in gigapaxos.properties."
                    + "*********************************************************\n");
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        for (AppReconfigurableNode node : ALL_NODES) {
          System.out.println("Shutting down " + node.myID);
          node.close();
        }
        System.out.println("********* All nodes have been shutdown. *********");
      }
    });
    Map<String, String> options = initOptions(args);

    if (options.containsKey(STANDALONE) && options.get(NS_FILE) != null) {
      startStandalone(options.get(NS_FILE));
      // run multiple nodes on a single machine
    } else if (options.get(TEST) != null && options.get(NS_FILE) != null) {
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
