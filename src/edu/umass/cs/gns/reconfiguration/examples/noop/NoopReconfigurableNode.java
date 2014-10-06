package edu.umass.cs.gns.reconfiguration.examples.noop;

import java.io.IOException;

import edu.umass.cs.gns.reconfiguration.AbstractReplicaCoordinator;
import edu.umass.cs.gns.reconfiguration.InterfaceReconfigurableNodeConfig;
import edu.umass.cs.gns.reconfiguration.ReconfigurableNode;
import edu.umass.cs.gns.reconfiguration.examples.ReconfigurableSampleNodeConfig;
import edu.umass.cs.gns.reconfiguration.examples.TestConfig;

/**
@author V. Arun
 */
public class NoopReconfigurableNode extends ReconfigurableNode<Integer> {

	public NoopReconfigurableNode(Integer id, InterfaceReconfigurableNodeConfig<Integer> nc)
			throws IOException {
		super(id, nc);
	}

	@Override
	protected AbstractReplicaCoordinator<Integer> createAppCoordinator() {
		NoopApp app = new NoopApp(this.myID); 
		NoopAppCoordinator appCoordinator = new NoopAppCoordinator(app);
		return appCoordinator;
	}

	public static void main(String[] args) {
		NoopReconfigurableNode[] nodes = new NoopReconfigurableNode[TestConfig.numNodes];
		ReconfigurableSampleNodeConfig nc = new ReconfigurableSampleNodeConfig();
		nc.localSetup(TestConfig.getNodes());
		try {
			for(int i=0; i<TestConfig.numNodes; i++) {
				nodes[i] = new NoopReconfigurableNode(i+TestConfig.startNodeID, nc);
			}
		} catch(IOException ioe) {
			ioe.printStackTrace();
		}
	}
}
