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

import edu.umass.cs.gigapaxos.interfaces.Application;
import edu.umass.cs.gigapaxos.interfaces.Replicable;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.reconfiguration.interfaces.Reconfigurable;
import edu.umass.cs.reconfiguration.interfaces.ReconfigurableRequest;
import edu.umass.cs.reconfiguration.interfaces.Repliconfigurable;

/**
@author V. Arun
 */
public class TrivialRepliconfigurable implements Repliconfigurable {
	
	/**
	 * The underlying app.
	 */
	public final Application app;
	
	/**
	 * @param app
	 */
	public TrivialRepliconfigurable(Application app) {
		this.app = app;
	}

	@Override
	public boolean execute(Request request) {
		return this.app.execute(request);
	}

	@Override
	public Request getRequest(String stringified)
			throws RequestParseException {
		return this.app.getRequest(stringified);
	}

	@Override
	public Set<IntegerPacketType> getRequestTypes() {
		return this.app.getRequestTypes();
	}


	@Override
	public boolean execute(Request request,
			boolean doNotReplyToClient) {
		return (this.app instanceof Replicable ? 
				((Replicable)this.app).execute(request, doNotReplyToClient): 
					this.app.execute(request));
	}

	@Override
	public ReconfigurableRequest getStopRequest(String name, int epoch) {
		if(this.app instanceof Reconfigurable) return ((Reconfigurable)this.app).getStopRequest(name, epoch);
		throw new RuntimeException("Can not get stop request for a non-reconfigurable app");
	}

	@Override
	public String getFinalState(String name, int epoch) {
		if(this.app instanceof Reconfigurable) return ((Reconfigurable)this.app).getFinalState(name, epoch);
		throw new RuntimeException("Can not get stop request for a non-reconfigurable app");
	}

	@Override
	public void putInitialState(String name, int epoch, String state) {
		if(this.app instanceof Reconfigurable) {
			((Reconfigurable)this.app).putInitialState(name, epoch, state);
			return;
		}
		throw new RuntimeException("Can not get stop request for a non-reconfigurable app");
	}

	@Override
	public boolean deleteFinalState(String name, int epoch) {
		if(this.app instanceof Reconfigurable) return ((Reconfigurable)this.app).deleteFinalState(name, epoch);
		throw new RuntimeException("Can not get stop request for a non-reconfigurable app");
	}

	@Override
	public Integer getEpoch(String name) {
		if(this.app instanceof Reconfigurable) return ((Reconfigurable)this.app).getEpoch(name);
		throw new RuntimeException("Can not get stop request for a non-reconfigurable app");
	}

	@Override
	public String checkpoint(String name) {
		if(this.app instanceof Replicable) return 
				((Replicable)this.app).checkpoint(name); 
		throw new RuntimeException("Can not get stop request for a non-replicable app");
	}

	@Override
	public boolean restore(String name, String state) {
		if(this.app instanceof Replicable) 
				return ((Replicable)this.app).restore(name, state);
		throw new RuntimeException("Can not get stop request for a non-replicable app");
	}
}
