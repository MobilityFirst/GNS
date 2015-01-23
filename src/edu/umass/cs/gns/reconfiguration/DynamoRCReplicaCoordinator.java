package edu.umass.cs.gns.reconfiguration;

import java.io.IOException;
import java.util.Set;
import java.util.logging.Logger;

import edu.umass.cs.gns.nio.InterfaceJSONNIOTransport;
import edu.umass.cs.gns.reconfiguration.reconfigurationutils.ConsistentReconfigurableNodeConfig;

public class DynamoRCReplicaCoordinator<NodeIDType> extends
		DynamoReplicaCoordinator<NodeIDType> {

	private final ConsistentReconfigurableNodeConfig<NodeIDType> consistentNodeConfig;
	public static final Logger log = Logger.getLogger(Reconfigurator.class
			.getName());

	public DynamoRCReplicaCoordinator(InterfaceReplicable app, NodeIDType myID,
			ConsistentReconfigurableNodeConfig<NodeIDType> nodeConfig,
			InterfaceJSONNIOTransport<NodeIDType> niot) {
		super(app, myID, nodeConfig, niot);
		this.consistentNodeConfig = nodeConfig;
	}
	
	public boolean coordinateRequest(String rcGroupName, InterfaceRequest request) {
		try {
			return super.coordinateRequest(request);
		} catch (IOException | RequestParseException e) {
			e.printStackTrace();
		}
		return false;
	}

	// The only method for which this class exists compared to its parent
	@Override
	public Set<NodeIDType> getReplicaGroup(String name) {
		return this.consistentNodeConfig.getReplicatedReconfigurators(name);
	}
}
