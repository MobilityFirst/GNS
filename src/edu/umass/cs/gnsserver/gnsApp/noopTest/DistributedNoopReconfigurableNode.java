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
package edu.umass.cs.gnsserver.gnsApp.noopTest;

import java.io.IOException;

import edu.umass.cs.reconfiguration.AbstractReplicaCoordinator;
import edu.umass.cs.reconfiguration.ReconfigurableNode;
import edu.umass.cs.reconfiguration.interfaces.ReconfigurableNodeConfig;

/**
 * @author Westy adapted from code originally by V. Arun
 */
public class DistributedNoopReconfigurableNode extends ReconfigurableNode<String> {

  /**
   * @param id
   * @param nc
   * @throws IOException
   */
  public DistributedNoopReconfigurableNode(String id,
          ReconfigurableNodeConfig<String> nc) throws IOException {
    super(id, nc);
    System.out.println("Starting node " + id + " at " + nc.getNodeAddress(id) + ":" + nc.getNodePort(id));
  }

  /**
   * Creates the app coordinator.
   * 
   * @return the coordinator
   */
  @Override
  protected AbstractReplicaCoordinator<String> createAppCoordinator() {
    DistributedNoopApp app = new DistributedNoopApp(this.myID);
    DistributedNoopAppCoordinator appCoordinator = new DistributedNoopAppCoordinator(app,
            DistributedNoopAppCoordinator.CoordType.PAXOS, this.nodeConfig,
            this.messenger);
    return appCoordinator;
  }

  /**
   * The main routine. Does local setup.
   * 
   * @param args
   */
  public static void main(String[] args) {
    String nodeId = null;
    String hostsFile = null;
    if (args.length == 2) {
      nodeId = args[0];
      hostsFile = args[1];
    } else {
      System.out.println("USage: java -cp GNS.jar edu.umass.cs.gnsserver.gnsApp.noopTest.NoopReconfigurableNode nodeId hostsFile");
      System.exit(1);
    }
    try {
      TestReconfigurableNodeConfig nc = new TestReconfigurableNodeConfig(hostsFile);
      System.out.println("######################## Setting up active at " + nodeId);
      new DistributedNoopReconfigurableNode(nodeId, nc);
      System.out.println("######################## Setting up RC at " + nc.activeToRC(nodeId));
      new DistributedNoopReconfigurableNode(nc.activeToRC(nodeId), nc);

    } catch (IOException e) {
      System.out.println("Problem setting up nodes: " + e);
      e.printStackTrace();
    }
  }
}
