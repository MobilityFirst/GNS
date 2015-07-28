/*
 * Copyright (c) 2015 University of Massachusetts
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 * 
 * Initial developer(s): V. Arun
 */
package edu.umass.cs.reconfiguration.examples.noop;

import java.io.IOException;

import edu.umass.cs.reconfiguration.AbstractReplicaCoordinator;
import edu.umass.cs.reconfiguration.ReconfigurableNode;
import edu.umass.cs.reconfiguration.examples.TestConfig;
import edu.umass.cs.reconfiguration.interfaces.InterfaceReconfigurableNodeConfig;

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
				new NoopReconfigurableNode(rcID,
				// must use different node config for each node
						TestConfig.getTestNodeConfig(),// nc,
						first && !(first = false) ? TestConfig.TEST_CLEAN_SLATE
								: false);
			}

		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
	}
}
