/*
 * Copyright (C) 2015
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.gnsApp.noopTest;

import java.io.IOException;

import edu.umass.cs.reconfiguration.AbstractReplicaCoordinator;
import edu.umass.cs.reconfiguration.ReconfigurableNode;
import edu.umass.cs.reconfiguration.interfaces.InterfaceReconfigurableNodeConfig;

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
          InterfaceReconfigurableNodeConfig<String> nc) throws IOException {
    super(id, nc);
    System.out.println("Starting node " + id + " at " + nc.getNodeAddress(id) + ":" + nc.getNodePort(id));
  }

  /**
   * Creates the app coordinator.
   * 
   * @return
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
      System.out.println("USage: java -cp GNS.jar edu.umass.cs.gns.gnsApp.noopTest.NoopReconfigurableNode nodeId hostsFile");
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
