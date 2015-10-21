/*
 * Copyright (c) 2015 University of Massachusetts
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 * 
 * Initial developer(s): V. Arun
 */
package edu.umass.cs.reconfiguration.reconfigurationutils;

import java.util.Set;

import edu.umass.cs.gigapaxos.InterfaceApplication;
import edu.umass.cs.gigapaxos.InterfaceReplicable;
import edu.umass.cs.gigapaxos.InterfaceRequest;
import edu.umass.cs.nio.IntegerPacketType;
import edu.umass.cs.reconfiguration.interfaces.InterfaceReconfigurable;
import edu.umass.cs.reconfiguration.interfaces.InterfaceReconfigurableRequest;
import edu.umass.cs.reconfiguration.interfaces.InterfaceRepliconfigurable;

/**
@author V. Arun
 */
public class TrivialRepliconfigurable implements InterfaceRepliconfigurable {
	
	/**
	 * The underlying app.
	 */
	public final InterfaceApplication app;
	
	/**
	 * @param app
	 */
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
