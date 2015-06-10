package edu.umass.cs.reconfiguration.examples.noop;

import java.io.IOException;

import edu.umass.cs.reconfiguration.AbstractReplicaCoordinator;
import edu.umass.cs.reconfiguration.InterfaceReconfigurableNodeConfig;
import edu.umass.cs.reconfiguration.ReconfigurableNode;
import edu.umass.cs.reconfiguration.examples.ReconfigurableSampleNodeConfig;
import edu.umass.cs.reconfiguration.examples.TestConfig;

/**
 * @author V. Arun
 */
public class NoopReconfigurableNode extends ReconfigurableNode<Integer> {

	/**
	 * @param id
	 * @param nc
	 * @throws IOException
	 */
	public NoopReconfigurableNode(Integer id,
			InterfaceReconfigurableNodeConfig<Integer> nc) throws IOException {
		super(id, nc);
	}

	@Override
	protected AbstractReplicaCoordinator<Integer> createAppCoordinator() {
		NoopApp app = new NoopApp(this.myID);
		NoopAppCoordinator appCoordinator = new NoopAppCoordinator(app,
				NoopAppCoordinator.CoordType.PAXOS, this.nodeConfig,
				this.messenger);
		return appCoordinator;
	}

	// local setup
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		ReconfigurableSampleNodeConfig nc = new ReconfigurableSampleNodeConfig();
		nc.localSetup(TestConfig.getNodes());
		try {
			System.out.println("Setting up actives at "
					+ nc.getActiveReplicas());
			for (int activeID : nc.getActiveReplicas()) {
				new NoopReconfigurableNode(activeID, nc);
			}
			System.out.println("Setting up RCs at " + nc.getReconfigurators());
			for (int rcID : nc.getReconfigurators()) {
				new NoopReconfigurableNode(rcID, nc);
			}

		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
	}
}
