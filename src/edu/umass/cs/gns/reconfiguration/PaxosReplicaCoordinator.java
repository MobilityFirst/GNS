package edu.umass.cs.gns.reconfiguration;

import java.io.IOException;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.umass.cs.gns.gigapaxos.InterfaceReplicable;
import edu.umass.cs.gns.gigapaxos.InterfaceRequest;
import edu.umass.cs.gns.gigapaxos.PaxosManager;
import edu.umass.cs.gns.nio.IntegerPacketType;
import edu.umass.cs.gns.nio.InterfaceJSONNIOTransport;
import edu.umass.cs.gns.nio.JSONMessenger;
import edu.umass.cs.gns.nio.Stringifiable;
import edu.umass.cs.utils.MyLogger;

public class PaxosReplicaCoordinator<NodeIDType> extends
		AbstractReplicaCoordinator<NodeIDType> {

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

	public PaxosReplicaCoordinator(InterfaceReplicable app, NodeIDType myID,
			Stringifiable<NodeIDType> unstringer,
			InterfaceJSONNIOTransport<NodeIDType> niot, int outOfOrderLimit) {
		this(app, myID, unstringer, niot);
		this.paxosManager.setOutOfOrderLimit(outOfOrderLimit);
	}

	@Override
	public Set<IntegerPacketType> getRequestTypes() {
		return this.app.getRequestTypes();
	}

	@Override
	public boolean coordinateRequest(InterfaceRequest request)
			throws IOException, RequestParseException {
		return this.coordinateRequest(request.getServiceName(), request);
	}

	private String propose(String paxosID, InterfaceRequest request) {
		String proposee = null;
		if (request instanceof InterfaceReconfigurableRequest
				&& ((InterfaceReconfigurableRequest) request).isStop())
			proposee = this.paxosManager.proposeStop(paxosID, request
					.toString(),
					(short) ((InterfaceReconfigurableRequest) request)
							.getEpochNumber());
		else
			proposee = this.paxosManager.propose(paxosID, request.toString());
		return proposee;
	}

	// in case paxosGroupID is not the same as the name in the request
	protected boolean coordinateRequest(String paxosGroupID,
			InterfaceRequest request) throws RequestParseException {
		String proposee = this.propose(paxosGroupID, request);
		log.log(Level.INFO,
				MyLogger.FORMAT[6],
				new Object[] {
						this,
						(proposee != null ? "paxos-coordinated"
								: "failed to paxos-coordinate"),
						request.getRequestType(), " to ", proposee, ":",
						request });
		return proposee != null;
	}

	/*
	 * This method always returns true as it will always succeed in either
	 * creating the group with the requested epoch number or higher. In either
	 * case, the caller should consider the operation a success.
	 */
	@Override
	public boolean createReplicaGroup(String groupName, int epoch,
			String state, Set<NodeIDType> nodes) {
		log.info(this + " about to create paxos instance " + groupName + ":"
				+ epoch + (state != null ? " with initial state " + state : ""));
		// will block for a default timeout if a lower unstopped epoch exits
		boolean created = this.paxosManager.createPaxosInstanceForcibly(
				groupName, (short) epoch, nodes, this, state,
				PaxosManager.CAN_CREATE_TIMEOUT);
		if (!created)
			log.info(this + " paxos instance " + groupName + ":" + epoch
					+ " or higher already exists");

		boolean createdOrExistsOrHigher = (created || this.paxosManager
				.existsOrHigher(groupName, (short) epoch));
		;
		assert (createdOrExistsOrHigher) : this + " failed to create "
				+ groupName + ":" + epoch + " with state " + state;
		return createdOrExistsOrHigher;
	}

	public String toString() {
		return this.getClass().getSimpleName() + getMyID();
	}

	@Override
	public Set<NodeIDType> getReplicaGroup(String serviceName) {
		if (this.paxosManager.isStopped(serviceName))
			return null;
		return this.paxosManager.getPaxosNodeIDs(serviceName);
	}

	/*
	 * FIXME: The method definition in AbstractReplicaCorodinator must accept an
	 * epoch number so that this method indeed overrides that method.
	 */
	// @Override
	public void deleteReplicaGroup(String serviceName, int epoch) {
		this.paxosManager.deletePaxosInstance(serviceName, (short) epoch);
	}

	protected void forceCheckpoint(String paxosID) {
		this.paxosManager.forceCheckpoint(paxosID);
	}

	@Override
	public Integer getEpoch(String name) {
		return this.paxosManager.getVersion(name);
	}

	@Override
	public String getFinalState(String name, int epoch) {
		String state = this.paxosManager.getFinalState(name, (short) epoch);
		log.info(this.getMyID()
				+ " received request for epoch final state "
				+ name
				+ ":"
				+ epoch
				+ "; returning: "
				+ state
				+ (state == null ? " paxos instance stopped is "
						+ this.paxosManager.isStopped(name)
						+ " and epoch final state is "
						+ this.paxosManager.getFinalState(name, (short) epoch)
						: ""));
		return state;
	}

	@Override
	public void putInitialState(String name, int epoch, String state) {
		throw new RuntimeException("This method should never have been called");
	}

	@Override
	public boolean deleteFinalState(String name, int epoch) {
		/*
		 * Will also delete one previous version. Sometimes, a node can miss a
		 * drop epoch that arrived even before it created that epoch, in which
		 * case, it would end up trying hard and succeeding at creating the
		 * epoch that just got dropped by using the previous epoch final state
		 * if it is available locally. So it is best to delete that final state
		 * as well so that the late, zombie epoch creation eventually fails.
		 * 
		 * Note: Usually deleting lower epochs in addition to the specified
		 * epoch is harmless. There is at most one lower epoch final state at a
		 * node anyway.
		 */
		return this.paxosManager.deleteFinalState(name, (short) epoch, 1);
	}

	@Override
	public InterfaceReconfigurableRequest getStopRequest(String name, int epoch) {
		InterfaceReconfigurableRequest stop = super.getStopRequest(name, epoch);
		if (stop!=null && !(stop instanceof InterfaceReplicableRequest))
			throw new RuntimeException(
					"Stop requests for Paxos apps must implement InterfaceReplicableRequest "
							+ "and their needsCoordination() method must return true by default "
							+ "(unless overridden by setNeedsCoordination(false))");
		return stop;
	}
	
	public boolean existsOrHigher(String name, int epoch) {
		return this.paxosManager.existsOrHigher(name, (short)epoch);
	}
}
