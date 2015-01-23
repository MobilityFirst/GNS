package edu.umass.cs.gns.reconfiguration.deprecated;

import java.util.Set;

import edu.umass.cs.gns.nio.IntegerPacketType;
import edu.umass.cs.gns.nio.JSONMessenger;
import edu.umass.cs.gns.reconfiguration.InterfaceReconfigurableNodeConfig;
import edu.umass.cs.gns.reconfiguration.InterfaceReconfigurableRequest;
import edu.umass.cs.gns.reconfiguration.InterfaceRepliconfigurable;
import edu.umass.cs.gns.reconfiguration.InterfaceRequest;
import edu.umass.cs.gns.reconfiguration.Reconfigurator;
import edu.umass.cs.gns.reconfiguration.RequestParseException;

/* This class makes Reconfigurator replicable and reconfigurable. 
 * Will likely go away.
 */
@Deprecated
public class ReconfiguratorRepliconfigurable<NodeIDType> extends
		Reconfigurator<NodeIDType> implements InterfaceRepliconfigurable {

	public ReconfiguratorRepliconfigurable(
			InterfaceReconfigurableNodeConfig<NodeIDType> nc,
			JSONMessenger<NodeIDType> m) {
		super(nc, m);
	}

	// FIXME: 
	@Override
	public boolean handleRequest(InterfaceRequest request,
			boolean doNotReplyToClient) {
		String serviceName = request.getServiceName();
		String rcGroupName = serviceName; // FIXME: hash serviceName
		/* invoke specific methods like handleDemandReport etc. 
		 * via ReconfiguratorProtocolTask.
		 */
		
		return false;
	}

	@Override
	public String getState(String name) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean updateState(String name, String state) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean handleRequest(InterfaceRequest request) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public InterfaceRequest getRequest(String stringified)
			throws RequestParseException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Set<IntegerPacketType> getRequestTypes() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public InterfaceReconfigurableRequest getStopRequest(String name, int epoch) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getFinalState(String name, int epoch) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void putInitialState(String name, int epoch, String state) {
		// TODO Auto-generated method stub

	}

	@Override
	public boolean deleteFinalState(String name, int epoch) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Integer getEpoch(String name) {
		// TODO Auto-generated method stub
		return null;
	}

}
