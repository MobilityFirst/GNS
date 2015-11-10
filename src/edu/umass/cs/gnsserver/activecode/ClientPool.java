/*
 * Copyright (C) 2015
 * University of Massachusetts
 * All Rights Reserved 
 *
 */
package edu.umass.cs.gnsserver.activecode;

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
	GnsApplicationInterface app;
	
	public ClientPool(GnsApplicationInterface app) {
		clients = new HashMap<>();
		this.app = app;
	}
	
	public void addClient(Thread t) {
		clients.put(t.getId(), new ActiveCodeClient(app, true));
	}
	
	public ActiveCodeClient getClient(Thread t) {
		return clients.get(t.getId());
	}

	public void shutdown() {
		for(ActiveCodeClient client : clients.values()) {
		    client.shutdownServer();
		}
	}
}
