package edu.umass.cs.reconfiguration;

import java.io.IOException;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.json.JSONException;

import edu.umass.cs.gigapaxos.InterfaceReplicable;
import edu.umass.cs.gigapaxos.InterfaceRequest;
import edu.umass.cs.nio.IntegerPacketType;
import edu.umass.cs.nio.JSONMessenger;
import edu.umass.cs.reconfiguration.reconfigurationutils.ConsistentNodeConfig;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import edu.umass.cs.utils.ML;

/**
 * @author arun
 *
 * @param <NodeIDType>
 * 
 * FIXME: Incomplete.
 */
public class DynamoReplicaCoordinator<NodeIDType> extends
		AbstractReplicaCoordinator<NodeIDType> {

	private final ConsistentNodeConfig<NodeIDType> consistentNodeConfig;
	private static final Logger log = (Reconfigurator.getLogger());

	/**
	 * @param app
	 * @param myID
	 * @param nodeConfig
	 * @param messenger
	 */
	public DynamoReplicaCoordinator(InterfaceReplicable app, NodeIDType myID,
			ConsistentNodeConfig<NodeIDType> nodeConfig,
			JSONMessenger<NodeIDType> messenger) {
		super(app, messenger);
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
			log.log(Level.INFO, ML.F[4], new Object[] { this,
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
	public boolean deleteReplicaGroup(String serviceName, int epoch) {
		throw new RuntimeException("Method not yet implemented");
	}

}
