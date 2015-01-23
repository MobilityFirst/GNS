package edu.umass.cs.gns.reconfiguration.deprecated;

import java.io.IOException;
import java.util.Set;

import org.json.JSONObject;

import edu.umass.cs.gns.gigapaxos.PaxosManager;
import edu.umass.cs.gns.nio.IntegerPacketType;
import edu.umass.cs.gns.nio.InterfacePacketDemultiplexer;
import edu.umass.cs.gns.reconfiguration.AbstractReplicaCoordinator;
import edu.umass.cs.gns.reconfiguration.InterfaceRepliconfigurable;
import edu.umass.cs.gns.reconfiguration.InterfaceRequest;
import edu.umass.cs.gns.reconfiguration.RequestParseException;

/*
 * This class implements methods required for replica coordination as specified in
 * AbstractReplicaCoordinator. Internally, it uses paxos for replica coordination.
 * 
 * This class will likely go away.
 */
@Deprecated
public class ReconfiguratorPaxos<NodeIDType> extends
		AbstractReplicaCoordinator<NodeIDType> implements
		InterfaceRepliconfigurable, InterfacePacketDemultiplexer {
	
	private final PaxosManager<NodeIDType> paxosManager;

	public ReconfiguratorPaxos(InterfaceRepliconfigurable app) {
		super(app);
		this.paxosManager = null; // FIXME:
		throw new RuntimeException("Method not implemented yet");
	}

	@Override
	public Set<IntegerPacketType> getRequestTypes() {
		return app.getRequestTypes();
	}

	// FIXME: call paxosManager.propose(.)
	@Override
	public boolean coordinateRequest(InterfaceRequest request)
			throws IOException, RequestParseException {
		throw new RuntimeException("Method not implemented yet");
	}

	// FIXME: call paxosManager.createPaxosInstance(.)
	@Override
	public boolean createReplicaGroup(String serviceName, int epoch,
			String state, Set<NodeIDType> nodes) {
		throw new RuntimeException("Method not implemented yet");
	}

	// FIXME: call paxosManager.remove(.)
	@Override
	public void deleteReplicaGroup(String serviceName) {
		throw new RuntimeException("Method not implemented yet");
	}

	// FIXME: call paxosManager.getPaxosNodeIDs(.)
	@Override
	public Set<NodeIDType> getReplicaGroup(String serviceName) {
		throw new RuntimeException("Method not implemented yet");
	}

	@Override
	public boolean handleJSONObject(JSONObject jsonObject) {
		// Convert to InterfaceRequest and call handleIncoming.
		InterfaceRequest request=null;
		try {
			request = app.getRequest(jsonObject.toString());
		} catch (RequestParseException e) {
			e.printStackTrace();
		}
		return request!=null ? handleIncoming(request) : false;
	}
}
