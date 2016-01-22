/*
 *
 *  Copyright (c) 2015 University of Massachusetts
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you
 *  may not use this file except in compliance with the License. You
 *  may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 *  implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 *
 *  Initial developer(s): Misha Badov, Westy
 *
 */
package edu.umass.cs.gnsserver.activecode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import edu.umass.cs.gnsserver.gnsApp.GnsApplicationInterface;

/**
 * This class represents a pool of active code clients. Each client is associated with a particular thread.
 * This is necessary because of limitations of Java's ThreadPoolExecutor.
 * @author mbadov
 *
 */
public class ClientPool {
	Map<Long, ActiveCodeClient> clients;
	GnsApplicationInterface<?> app;
	ArrayList<ActiveCodeClient> spareWorkers;
	
	/**
	 * Initialize a ClientPool
	 * @param app
	 */
	public ClientPool(GnsApplicationInterface<?> app) {
		clients = new HashMap<>();
		this.app = app;
		spareWorkers = new ArrayList<ActiveCodeClient>();
		for (int i=0; i<10; i++){
			spareWorkers.add(new ActiveCodeClient(app, -1));
		}
	}
	
	protected void addClient(Thread t) {
		clients.put(t.getId(), new ActiveCodeClient(app, -1));
	}
	
	protected void addClient(Thread t, int port){
		clients.put(t.getId(), new ActiveCodeClient(app, port));
	}
	
	protected ActiveCodeClient getClient(long pid) {
		return clients.get(pid);
	}
	
	protected void shutdown() {
		for(ActiveCodeClient client : clients.values()) {
		    client.shutdownServer();
		}
	}
}
