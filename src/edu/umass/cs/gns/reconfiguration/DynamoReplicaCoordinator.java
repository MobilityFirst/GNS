package edu.umass.cs.gns.reconfiguration;

import java.io.IOException;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.json.JSONException;

import edu.umass.cs.gns.nio.IntegerPacketType;
import edu.umass.cs.gns.nio.InterfaceJSONNIOTransport;
import edu.umass.cs.gns.nio.JSONMessenger;
import edu.umass.cs.gns.reconfiguration.reconfigurationutils.ConsistentNodeConfig;
import edu.umass.cs.gns.util.MyLogger;

public class DynamoReplicaCoordinator<NodeIDType> extends
		AbstractReplicaCoordinator<NodeIDType> {

	private final ConsistentNodeConfig<NodeIDType> consistentNodeConfig;
	public static final Logger log = Logger.getLogger(Reconfigurator.class
			.getName());

	public DynamoReplicaCoordinator(InterfaceReplicable app, NodeIDType myID,
			ConsistentNodeConfig<NodeIDType> nodeConfig,
			InterfaceJSONNIOTransport<NodeIDType> niot) {
		super(app, (JSONMessenger<NodeIDType>) niot);
		this.consistentNodeConfig = nodeConfig;
	}

	@Override
	public Set<IntegerPacketType> getRequestTypes() {
		return this.app.getRequestTypes();
	}

	// FIXME: implement durability. Currently lazily propagates.
	@Override
	public boolean coordinateRequest(InterfaceRequest request)
			throws IOException, RequestParseException {
		try {
			log.log(Level.INFO, MyLogger.FORMAT[4], new Object[] { this,
					"lazily coordinating", request.getRequestType(), ": ",
					request });
			this.sendAllLazy(request);
		} catch (JSONException e) {
			e.printStackTrace();
		}
		return true;
	}

	@Override
	public Set<NodeIDType> getReplicaGroup(String serviceName) {
		return this.consistentNodeConfig.getReplicatedServers(serviceName);
	}

	public String toString() {
		return this.getClass().getSimpleName() + getMyID();
	}
	
	/*
	 * Consistent hashing means that groups don't have to be explicitly created.
	 */
	@Override
	public boolean createReplicaGroup(String serviceName, int epoch,
			String state, Set<NodeIDType> nodes) {
		throw new RuntimeException(
				"This method should not be invoked as groups are implicitly defined with consistent hashing.");
	}

	@Override
	public void deleteReplicaGroup(String serviceName) {
		throw new RuntimeException("Method not yet implemented");
	}
}
