package edu.umass.cs.reconfiguration.reconfigurationutils;

import java.util.Set;

import edu.umass.cs.gigapaxos.InterfaceApplication;
import edu.umass.cs.gigapaxos.InterfaceReplicable;
import edu.umass.cs.gigapaxos.InterfaceRequest;
import edu.umass.cs.nio.IntegerPacketType;
import edu.umass.cs.reconfiguration.InterfaceReconfigurable;
import edu.umass.cs.reconfiguration.InterfaceReconfigurableRequest;
import edu.umass.cs.reconfiguration.InterfaceRepliconfigurable;

/**
@author V. Arun
 */
public class TrivialRepliconfigurable implements InterfaceRepliconfigurable {
	
	public final InterfaceApplication app;
	
	public TrivialRepliconfigurable(InterfaceApplication app) {
		this.app = app;
	}

	@Override
	public boolean handleRequest(InterfaceRequest request) {
		return this.app.handleRequest(request);
	}

	@Override
	public InterfaceRequest getRequest(String stringified)
			throws RequestParseException {
		return this.app.getRequest(stringified);
	}

	@Override
	public Set<IntegerPacketType> getRequestTypes() {
		return this.app.getRequestTypes();
	}


	@Override
	public boolean handleRequest(InterfaceRequest request,
			boolean doNotReplyToClient) {
		return (this.app instanceof InterfaceReplicable ? 
				((InterfaceReplicable)this.app).handleRequest(request, doNotReplyToClient): 
					this.app.handleRequest(request));
	}

	@Override
	public InterfaceReconfigurableRequest getStopRequest(String name, int epoch) {
		if(this.app instanceof InterfaceReconfigurable) return ((InterfaceReconfigurable)this.app).getStopRequest(name, epoch);
		throw new RuntimeException("Can not get stop request for a non-reconfigurable app");
	}

	@Override
	public String getFinalState(String name, int epoch) {
		if(this.app instanceof InterfaceReconfigurable) return ((InterfaceReconfigurable)this.app).getFinalState(name, epoch);
		throw new RuntimeException("Can not get stop request for a non-reconfigurable app");
	}

	@Override
	public void putInitialState(String name, int epoch, String state) {
		if(this.app instanceof InterfaceReconfigurable) {
			((InterfaceReconfigurable)this.app).putInitialState(name, epoch, state);
			return;
		}
		throw new RuntimeException("Can not get stop request for a non-reconfigurable app");
	}

	@Override
	public boolean deleteFinalState(String name, int epoch) {
		if(this.app instanceof InterfaceReconfigurable) return ((InterfaceReconfigurable)this.app).deleteFinalState(name, epoch);
		throw new RuntimeException("Can not get stop request for a non-reconfigurable app");
	}

	@Override
	public Integer getEpoch(String name) {
		if(this.app instanceof InterfaceReconfigurable) return ((InterfaceReconfigurable)this.app).getEpoch(name);
		throw new RuntimeException("Can not get stop request for a non-reconfigurable app");
	}

	@Override
	public String getState(String name) {
		if(this.app instanceof InterfaceReplicable) return 
				((InterfaceReplicable)this.app).getState(name); 
		throw new RuntimeException("Can not get stop request for a non-replicable app");
	}

	@Override
	public boolean updateState(String name, String state) {
		if(this.app instanceof InterfaceReplicable) 
				return ((InterfaceReplicable)this.app).updateState(name, state);
		throw new RuntimeException("Can not get stop request for a non-replicable app");
	}
}
