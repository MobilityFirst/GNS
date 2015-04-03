package edu.umass.cs.gns.activecode;

import java.util.HashMap;
import java.util.Map;

import edu.umass.cs.gns.nsdesign.GnsApplicationInterface;
import edu.umass.cs.gns.nsdesign.gnsReconfigurable.GnsReconfigurable;

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
		clients = new HashMap<Long, ActiveCodeClient>();
		this.app = app;
	}
	
	public void addClient(Thread t) {
		clients.put(t.getId(), new ActiveCodeClient(app, true, true));
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
