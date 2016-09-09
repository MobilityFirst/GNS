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
package edu.umass.cs.gnsserver.gnsapp.deprecated;

import edu.umass.cs.gnsserver.nodeconfig.GNSInterfaceNodeConfig;

import java.io.IOException;

import edu.umass.cs.gnsserver.utils.ParametersAndOptions;
import edu.umass.cs.reconfiguration.AbstractReplicaCoordinator;
import edu.umass.cs.reconfiguration.ReconfigurableNode;
import static edu.umass.cs.gnsserver.utils.ParametersAndOptions.printOptions;
import edu.umass.cs.reconfiguration.ReconfigurationConfig;
import edu.umass.cs.utils.Config;
import java.util.Map;

/**
 * Instantiates the replicas and reconfigurators. Also parses
 * all the command line options.
 *
 * @author Westy
 */
@Deprecated
public class AppReconfigurableNode extends ReconfigurableNode<String> {

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

  public static Map<String, String> initOptions(String[] args)
          throws IOException {
    Map<String, String> options = ParametersAndOptions
            .getParametersAsHashMap(
                    AppReconfigurableNode.class.getCanonicalName(),
                    AppOptionsOld.getAllOptions(), args);
    AppOptionsOld.initializeFromOptions(options);
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
    System.out.println("*********************************************************\n"
            + "This mode of starting the GNS is no longer supported. Start ReconfigurableNode"
            + " instead with the node ID(s) being started listed at the end of command-line options,"
            + " and APPLICATION=edu.umass.cs.gnsserver.GnsApp in gigapaxos.properties."
            + "*********************************************************\n");
    System.exit(-1);
  }

  @Override
  protected AbstractReplicaCoordinator<String> createAppCoordinator() {
    throw new RuntimeException("This method should not have been called");
  }
}
