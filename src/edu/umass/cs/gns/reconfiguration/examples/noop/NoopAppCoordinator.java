package edu.umass.cs.gns.reconfiguration.examples.noop;

import java.io.IOException;
import java.util.HashMap;
import java.util.Set;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gns.nio.GenericMessagingTask;
import edu.umass.cs.gns.nio.IntegerPacketType;
import edu.umass.cs.gns.reconfiguration.AbstractReplicaCoordinator;
import edu.umass.cs.gns.reconfiguration.InterfaceReconfigurable;
import edu.umass.cs.gns.reconfiguration.InterfaceReplicable;
import edu.umass.cs.gns.reconfiguration.InterfaceRequest;
import edu.umass.cs.gns.reconfiguration.InterfaceReconfigurableRequest;
import edu.umass.cs.gns.reconfiguration.RequestParseException;

/**
 * @author V. Arun
 */
public class NoopAppCoordinator extends AbstractReplicaCoordinator<Integer> {

	private class CoordData {
		final String name;
		final int epoch;
		final Set<Integer> replicas;

		CoordData(String name, int epoch, Set<Integer> replicas) {
			this.name = name;
			this.epoch = epoch;
			this.replicas = replicas;
		}
	}

	private final HashMap<String, CoordData> groups = new HashMap<String, CoordData>();

	NoopAppCoordinator(InterfaceReplicable app) {
		super(app);
		this.registerCoordination(NoopAppRequest.PacketType.DEFAULT_APP_REQUEST);
	}

	@Override
	public boolean coordinateRequest(InterfaceRequest request)
			throws IOException, RequestParseException {
		try {
			((NoopAppRequest)request).setNeedsCoordination(false);
			this.sendAllLazy((NoopAppRequest) request);
		} catch (JSONException e) {
			e.printStackTrace();
		}
		return true;
	}

	@Override
	public boolean createReplicaGroup(String serviceName, int epoch,
			String state, Set<Integer> nodes) {
		CoordData data = new CoordData(serviceName, epoch, nodes);
		this.groups.put(serviceName, data);
		this.app.putInitialState(serviceName, epoch, state);
		return true;
	}

	@Override
	public void deleteReplicaGroup(String serviceName) {
		this.groups.remove(serviceName);
	}

	@Override
	public Set<Integer> getReplicaGroup(String serviceName) {
		CoordData data = this.groups.get(serviceName);
		if (data != null)
			return data.replicas;
		else
			return null;
	}

	@Override
	public Set<IntegerPacketType> getRequestTypes() {
		return this.app.getRequestTypes();
	}

	@Override
	public InterfaceReconfigurableRequest getStopRequest(String name, int epoch) {
		if (this.app instanceof InterfaceReconfigurable)
			return ((InterfaceReconfigurable) this.app).getStopRequest(name,
					epoch);
		throw new RuntimeException(
				"Can not get stop request for a non-reconfigurable app");
	}
	
	public boolean existsGroup(String name, int epoch) {
		CoordData data = this.groups.get(name);
		assert(data==null || data.name.equals(name));
		return (data!=null && data.epoch==epoch);
	}

	protected void sendAllLazy(NoopAppRequest request) throws IOException,
			RequestParseException, JSONException {
		if(this.getReplicaGroup(request.getServiceName())==null) return;
		GenericMessagingTask<Integer, JSONObject> mtask = new GenericMessagingTask<Integer, JSONObject>(
				this.getReplicaGroup(request.getServiceName()).toArray(),
				request.toJSONObject());
		if (this.messenger != null)
			this.messenger.send(mtask);
	}
}
