package edu.umass.cs.reconfiguration.reconfigurationutils;

import edu.umass.cs.gigapaxos.InterfaceReplicable;
import edu.umass.cs.gigapaxos.InterfaceRequest;
import edu.umass.cs.gigapaxos.deprecated.Replicable;
import edu.umass.cs.nio.IntegerPacketType;
import edu.umass.cs.reconfiguration.Reconfigurator;

import java.util.Set;

@Deprecated
public class BackwardsCompatibility {

	protected static Replicable InterfaceReplicableToReplicable(
			final InterfaceReplicable app) {
		assert(false);
		if("".isEmpty()) throw new RuntimeException("This class should no longer be used");
		
		Replicable replicable = new Replicable() {

			@Override
			public boolean handleDecision(String name, String value,
					boolean doNotReplyToClient) {
				try {
					app.handleRequest(app.getRequest(value), doNotReplyToClient);
				} catch (RequestParseException e) {
					Reconfigurator.getLogger().warning(app
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
