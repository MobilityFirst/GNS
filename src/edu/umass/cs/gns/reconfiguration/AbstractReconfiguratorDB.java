package edu.umass.cs.gns.reconfiguration;

import java.util.Set;

import org.json.JSONException;

import edu.umass.cs.gns.nio.IntegerPacketType;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.DemandReport;
import edu.umass.cs.gns.reconfiguration.reconfigurationutils.ConsistentHashing;
import edu.umass.cs.gns.reconfiguration.reconfigurationutils.ReconfigurationRecord;

/**
@author V. Arun
 */
/* Need to add fault tolerance support via paxos here.
 */
public abstract class AbstractReconfiguratorDB<NodeIDType> implements InterfaceReplicable {
	protected final NodeIDType myID;
	protected final InterfaceReconfigurableNodeConfig<NodeIDType> nodeConfig;
	protected final ConsistentHashing<NodeIDType> CH_RC; // need to refresh when nodeConfig changes
	protected final ConsistentHashing<NodeIDType> CH_AR; // need to refresh when nodeConfig changes

	public ReconfigurationRecord<NodeIDType> getReconfigurationRecord(String name, int epoch) {
		ReconfigurationRecord<NodeIDType> record = this.getReconfigurationRecord(name);
		return record.getEpoch()==epoch ? record : null;
	}
	public abstract ReconfigurationRecord<NodeIDType> getReconfigurationRecord(String name);
	public abstract boolean updateStats(DemandReport<NodeIDType> report);
	public abstract boolean setState(String name, int epoch, ReconfigurationRecord.RCStates state);
	public abstract boolean setStateIfReady(String name, int epoch, ReconfigurationRecord.RCStates state);
	
	public boolean setStateSuper(String name, int epoch, ReconfigurationRecord.RCStates state) {
		ReconfigurationRecord<NodeIDType> record = this.getReconfigurationRecord(name);
		if(record==null || (state==ReconfigurationRecord.RCStates.WAIT_ACK_START && !record.incrEpoch(epoch-1)))  return false;
		record.setState(state);
		return true;
	}
	
	public AbstractReconfiguratorDB(NodeIDType myID, InterfaceReconfigurableNodeConfig<NodeIDType> nc) {
		this.myID = myID;
		this.nodeConfig = nc;
		this.CH_RC = new ConsistentHashing<NodeIDType>(nc.getReconfigurators().toArray());
		this.CH_AR = new ConsistentHashing<NodeIDType>(nc.getActiveReplicas().toArray());
	}

	protected ReconfigurationRecord<NodeIDType> createRecord(String name) {
		ReconfigurationRecord<NodeIDType> record = null;
		try {
			record = new ReconfigurationRecord<NodeIDType>(name, 0, getInitialActives(name));
		} catch(JSONException je) {
			je.printStackTrace();
		}
		return record;
	}
	
	protected boolean amIResponsible(String name) {
		Set<NodeIDType> nodes = CH_RC.getReplicatedServers(name);
		return (nodes.contains(myID) ? true : false); 
	}
	protected boolean amIFirst(String name) {
		Set<NodeIDType> nodes = CH_RC.getReplicatedServers(name);
		return (nodes.contains(myID) && (nodes.iterator().next()==myID) ? true : false); 
	}
	protected Set<NodeIDType> getDefaultActiveReplicas(String name) {
		return this.CH_RC.getReplicatedServers(name);
	}
	
	/* If the set of active replicas and reconfigurators is the same,
	 * this default policy will choose the reconfigurator nodes
	 * also as the initial set of active replicas.
	 */
	private Set<NodeIDType> getInitialActives(String name) {
		return CH_AR.getReplicatedServers(name);
	}

	/***************** Paxos related methods below ***********/
	
	@Override
	public boolean handleRequest(InterfaceRequest request,
			boolean doNotReplyToClient) {
		// TODO Auto-generated method stub
		return false;
	}
	@Override
	public boolean handleRequest(InterfaceRequest request) {
		// TODO Auto-generated method stub
		return false;
	}
	@Override
	public InterfaceRequest getRequest(String stringified)
			throws RequestParseException {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public Set<IntegerPacketType> getRequestTypes() {
		// TODO Auto-generated method stub
		return null;
	}

	public String getState(String name, int epoch) {
		// TODO Auto-generated method stub
		return null;
	}
	public boolean updateState(String name, String state) {
		// TODO Auto-generated method stub
		return false;
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
	}
}
