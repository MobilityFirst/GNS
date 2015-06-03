package edu.umass.cs.gns.reconfiguration.reconfigurationutils;

import edu.umass.cs.gns.gigapaxos.InterfaceReplicable;
import edu.umass.cs.gns.gigapaxos.InterfaceRequest;
import edu.umass.cs.gns.gigapaxos.deprecated.Replicable;
import edu.umass.cs.gns.nio.IntegerPacketType;
import edu.umass.cs.gns.reconfiguration.Reconfigurator;
import edu.umass.cs.gns.reconfiguration.RepliconfigurableReconfiguratorDB;
import edu.umass.cs.gns.reconfiguration.RequestParseException;
import edu.umass.cs.gns.reconfiguration.examples.noop.NoopAppCoordinator;

import java.util.Set;

public class BackwardsCompatibility {

	public static Replicable InterfaceReplicableToReplicable(
			final InterfaceReplicable app) {
		assert (app instanceof RepliconfigurableReconfiguratorDB
				|| app instanceof NoopAppCoordinator || app instanceof edu.umass.cs.gns.newApp.NewAppCoordinator // -Westy
		) : app.getClass();
		Replicable replicable = new Replicable() {

			@Override
			public boolean handleDecision(String name, String value,
					boolean doNotReplyToClient) {
				try {
					app.handleRequest(app.getRequest(value), doNotReplyToClient);
				} catch (RequestParseException e) {
					Reconfigurator.log.warning(app
							+ " unable to parse request " + value);
					e.printStackTrace();
				}
				return true; // should always return true
			}

			@Override
			public String getState(String name) {
				return app.getState(name);
			}

			@Override
			public boolean updateState(String name, String state) {
				return app.updateState(name, state);
			}

			@Override
			public boolean handleRequest(InterfaceRequest request,
					boolean doNotReplyToClient) {
				throw new UnsupportedOperationException("Not supported yet."); // To
																				// change
																				// body
																				// of
																				// generated
																				// methods,
																				// choose
																				// Tools
																				// |
																				// Templates.
			}

			@Override
			public boolean handleRequest(InterfaceRequest request) {
				throw new UnsupportedOperationException("Not supported yet."); // To
																				// change
																				// body
																				// of
																				// generated
																				// methods,
																				// choose
																				// Tools
																				// |
																				// Templates.
			}

			@Override
			public InterfaceRequest getRequest(String stringified)
					throws RequestParseException {
				throw new UnsupportedOperationException("Not supported yet."); // To
																				// change
																				// body
																				// of
																				// generated
																				// methods,
																				// choose
																				// Tools
																				// |
																				// Templates.
			}

			@Override
			public Set<IntegerPacketType> getRequestTypes() {
				throw new UnsupportedOperationException("Not supported yet."); // To
																				// change
																				// body
																				// of
																				// generated
																				// methods,
																				// choose
																				// Tools
																				// |
																				// Templates.
			}
		};
		return replicable;
	}
}
