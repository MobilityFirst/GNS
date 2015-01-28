package edu.umass.cs.gns.reconfiguration;

import java.io.IOException;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.umass.cs.gns.gigapaxos.PaxosManager;
import edu.umass.cs.gns.nio.IntegerPacketType;
import edu.umass.cs.gns.nio.InterfaceJSONNIOTransport;
import edu.umass.cs.gns.nio.JSONMessenger;
import edu.umass.cs.gns.util.MyLogger;
import edu.umass.cs.gns.util.Stringifiable;

public class PaxosReplicaCoordinator<NodeIDType> extends
		AbstractReplicaCoordinator<NodeIDType>  {

	private final PaxosManager<NodeIDType> paxosManager;
	public static final Logger log = Logger.getLogger(Reconfigurator.class
			.getName());

	public PaxosReplicaCoordinator(InterfaceReplicable app, NodeIDType myID,
			Stringifiable<NodeIDType> unstringer,
			InterfaceJSONNIOTransport<NodeIDType> niot) {
		super(app, (JSONMessenger<NodeIDType>) niot);
		this.paxosManager = new PaxosManager<NodeIDType>(myID, unstringer,
				niot, this);
	}

	@Override
	public Set<IntegerPacketType> getRequestTypes() {
		return this.app.getRequestTypes();
	}

	// FIXME: request.getServiceName() must be the paxos group ID
	@Override
	public boolean coordinateRequest(InterfaceRequest request)
			throws IOException, RequestParseException {
		log.log(Level.INFO, MyLogger.FORMAT[4], new Object[] { this,
				"paxos coordinating", request.getRequestType(), ": ", request });
		// this.sendAllLazy(request);
		this.paxosManager.propose(request.getServiceName(), request.toString());
		return true;
	}
	
	// in case paxosGroupID is not the same as the name in the request
	public boolean coordinateRequest(String paxosGroupID, InterfaceRequest request) throws RequestParseException {
		log.log(Level.INFO, MyLogger.FORMAT[4], new Object[] { this,
				"paxos coordinating", request.getRequestType(), ": ", request });
		this.paxosManager.propose(paxosGroupID, request.toString());
		return true;
	}

	public boolean createReplicaGroup(String groupName, int epoch,
			String state, Set<NodeIDType> nodes) {
		this.paxosManager.createPaxosInstance(groupName, (short) epoch,
				nodes, this);
		return true;
	}

	public String toString() {
		return this.getClass().getSimpleName() + getMyID();
	}

	@Override
	public Set<NodeIDType> getReplicaGroup(String serviceName) {
		return this.paxosManager.getPaxosNodeIDs(serviceName);
	}

	/* FIXME: Needed only for reconfiguring reconfigurators, which
	 * is not yet implemented. We also need PaxosManager support 
	 * for deleting a paxos group.
	 */
	@Override
	public void deleteReplicaGroup(String serviceName) {
		throw new RuntimeException("Method not yet implemented");
	}
}
