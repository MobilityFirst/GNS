package edu.umass.cs.gigapaxos.examples.noop;

import java.util.Set;

import edu.umass.cs.gigapaxos.InterfaceReplicable;
import edu.umass.cs.gigapaxos.InterfaceRequest;
import edu.umass.cs.gigapaxos.examples.PaxosAppRequest;
import edu.umass.cs.gigapaxos.paxospackets.RequestPacket;
import edu.umass.cs.nio.IntegerPacketType;
import edu.umass.cs.reconfiguration.examples.noop.NoopApp;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;

/**
 * @author arun
 *
 */
public class NoopPaxosApp implements InterfaceReplicable {

	@Override
	public boolean handleRequest(InterfaceRequest request) {
		// execute request here

		// set response if request instanceof InterfaceClientRequest
		if (request instanceof RequestPacket)
			((RequestPacket) request).setResponse("appropriate_response_value");
		if (request instanceof PaxosAppRequest)
			((PaxosAppRequest) request)
					.setResponse("appropriate_response_value");
		return true;
	}

	@Override
	public boolean handleRequest(InterfaceRequest request,
			boolean doNotReplyToClient) {
		// execute request without replying back to client

		// identical to above unless app manages its own messaging
		return this.handleRequest(request);
	}

	@Override
	public String getState(String name) {
		// should return checkpoint state here
		return null;
	}

	@Override
	public boolean updateState(String name, String state) {
		// should update checkpoint state here for name
		return true;
	}

	/**
	 * Needed only if app uses request types other than RequestPacket. Refer
	 * {@link NoopApp} for a more detailed example.
	 */
	@Override
	public InterfaceRequest getRequest(String stringified)
			throws RequestParseException {
		// TODO Auto-generated method stub
		return null;
	}

	/**
	 * Needed only if app uses request types other than RequestPacket. Refer
	 * {@link NoopApp} for a more detailed example.
	 */
	@Override
	public Set<IntegerPacketType> getRequestTypes() {
		// TODO Auto-generated method stub
		return null;
	}
}
