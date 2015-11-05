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

import edu.umass.cs.gigapaxos.PaxosConfig;
import edu.umass.cs.reconfiguration.AbstractReplicaCoordinator;
import edu.umass.cs.reconfiguration.ReconfigurableNode;
import edu.umass.cs.reconfiguration.ReconfigurationConfig;
import edu.umass.cs.reconfiguration.interfaces.ReconfigurableNodeConfig;
import edu.umass.cs.reconfiguration.reconfigurationutils.DefaultNodeConfig;

/**
 * @author V. Arun
 * 
 *         A class like this is optional and is needed only if the application
 *         developer wants to use non-default means to instantiate an app
 *         replica. For most applications, the default reflection-based replica
 *         instantiation mechanism should suffice obviating this class.
 */
public class NoopReconfigurableNode extends ReconfigurableNode<String> {

	/**
	 * @param id
	 * @param nc
	 * @param cleanSlate
	 * @throws IOException
	 */
	public NoopReconfigurableNode(String id,
			ReconfigurableNodeConfig<String> nc, boolean cleanSlate)
			throws IOException {
		super(id, nc, cleanSlate);
	}

	/**
	 * @param id
	 * @param nc
	 * @throws IOException
	 */
	public NoopReconfigurableNode(String id,
			ReconfigurableNodeConfig<String> nc) throws IOException {
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

	private static final boolean TEST_CLEAN_SLATE = false;

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

		ReconfigurableNodeConfig<String> nc = new DefaultNodeConfig<String>(
				PaxosConfig.getActives(),
				ReconfigurationConfig.getReconfigurators());
		try {
			System.out.print("Setting up actives at " + nc.getActiveReplicas()
					+ "...");
			for (String activeID : nc.getActiveReplicas()) {
				new NoopReconfigurableNode(activeID, nc);
			}
			System.out.println("done");
			System.out.print("Setting up RCs at " + nc.getReconfigurators()
					+ "...");
			boolean first = true;
			for (String rcID : nc.getReconfigurators()) {
				// start first node with clean slate
				new NoopReconfigurableNode(rcID,
				// must use different node config for each node
						(new DefaultNodeConfig<String>(
								PaxosConfig.getActives(), ReconfigurationConfig
										.getReconfigurators())),
						first && !(first = false) ? TEST_CLEAN_SLATE : false);
			}
			System.out.println("done");
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
	}
}
