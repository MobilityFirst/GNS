package edu.umass.cs.gns.reconfiguration;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import edu.umass.cs.gns.nio.IntegerPacketType;

/**
@author V. Arun
 */
public class UnreplicatedAppCoordinator<RequestType extends IntegerPacketType, NodeIDType> extends
		AbstractReplicaCoordinator<NodeIDType> {

	public UnreplicatedAppCoordinator(InterfaceReplicable app) {
		super(app);
	}

	@Override
	public boolean coordinateRequest(InterfaceRequest request)
			throws IOException, RequestParseException {
		return true;
	}

	@Override
	public boolean createReplicaGroup(String serviceName, int epoch, String state, Set<NodeIDType> nodes) {
		return true;
	}

	@Override
	public void deleteReplicaGroup(String serviceName) {
	}

	@Override
	public Set<NodeIDType> getReplicaGroup(String serviceName) {
		return null;
	}

	@Override
	public Set<IntegerPacketType> getRequestTypes() {
		// TODO Auto-generated method stub
		return new HashSet<IntegerPacketType>();
	}
	
	@Override
	public InterfaceStopRequest getStopRequest(String name, int epoch) {
		if(this.app instanceof InterfaceReconfigurable) return ((InterfaceReconfigurable)this.app).getStopRequest(name, epoch);
		throw new RuntimeException("Can not get stop request for a non-reconfigurable app");
	}
}
