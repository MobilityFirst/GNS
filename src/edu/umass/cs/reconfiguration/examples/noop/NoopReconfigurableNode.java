package edu.umass.cs.reconfiguration.examples.noop;

import java.io.IOException;

import edu.umass.cs.reconfiguration.AbstractReplicaCoordinator;
import edu.umass.cs.reconfiguration.InterfaceReconfigurableNodeConfig;
import edu.umass.cs.reconfiguration.ReconfigurableNode;
import edu.umass.cs.reconfiguration.examples.TestConfig;

/**
 * @author V. Arun
 */
public class NoopReconfigurableNode extends ReconfigurableNode<String> {

	/**
	 * @param id
	 * @param nc
	 * @param cleanSlate
	 * @throws IOException
	 */
	public NoopReconfigurableNode(String id,
			InterfaceReconfigurableNodeConfig<String> nc, boolean cleanSlate)
			throws IOException {
		super(id, nc, cleanSlate);
	}

	/**
	 * @param id
	 * @param nc
	 * @throws IOException
	 */
	public NoopReconfigurableNode(String id,
			InterfaceReconfigurableNodeConfig<String> nc) throws IOException {
		super(id, nc);
	}

	@Override
	protected AbstractReplicaCoordinator<String> createAppCoordinator() {
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
		// ConsoleHandler handler = new ConsoleHandler();
		// handler.setLevel(Level.FINE);
		// PaxosManager.getLogger().setLevel(Level.FINE);
		// PaxosManager.getLogger().addHandler(handler);
		// PaxosManager.getLogger().setUseParentHandlers(false);

		InterfaceReconfigurableNodeConfig<String> nc = TestConfig
				.getTestNodeConfig();
		try {
			System.out.println("Setting up actives at "
					+ nc.getActiveReplicas());
			for (String activeID : nc.getActiveReplicas()) {
				new NoopReconfigurableNode(activeID, nc);
			}
			System.out.println("Setting up RCs at " + nc.getReconfigurators());
			boolean first = true;
			for (String rcID : nc.getReconfigurators()) {
				// start first node with clean slate
				new NoopReconfigurableNode(rcID, nc,
						first && !(first = false) ? TestConfig.TEST_CLEAN_SLATE
								: false);
			}

		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
	}
}
