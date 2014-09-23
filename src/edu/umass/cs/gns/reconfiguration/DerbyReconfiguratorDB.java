package edu.umass.cs.gns.reconfiguration;

import java.util.HashMap;
import java.util.Set;


import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.DemandReport;
import edu.umass.cs.gns.reconfiguration.reconfigurationutils.ReconfigurationRecord;
import edu.umass.cs.gns.reconfiguration.reconfigurationutils.ReconfigurationRecord.RCStates;

/**
@author V. Arun
 */

/* FIXME: Currently, this logger simply maintains a memory map instead 
 * of a persistent DB.
 */
public class DerbyReconfiguratorDB<NodeIDType> extends AbstractReconfiguratorDB<NodeIDType> {
	

	private final HashMap<String, ReconfigurationRecord<NodeIDType>> rrMap = new HashMap<String, ReconfigurationRecord<NodeIDType>>();
	
	public DerbyReconfiguratorDB(NodeIDType myID, InterfaceReconfigurableNodeConfig<NodeIDType> nc) {
		super(myID, nc);
	}

	@Override
	public ReconfigurationRecord<NodeIDType> getReconfigurationRecord(String name) {
		return this.rrMap.get(name);
	}

	public Set<NodeIDType> getActiveReplicas(String name, int epoch) {
		ReconfigurationRecord<NodeIDType> record = this.rrMap.get(name);
		assert(record.getEpoch()==epoch);
		return record.getActiveReplicas(name, epoch);
	}

	@Override
	public boolean updateStats(DemandReport<NodeIDType> report) {
		ReconfigurationRecord<NodeIDType> record = this.rrMap.get(report.getServiceName());
		if(record==null && amIResponsible(report.getServiceName())) {
			record = createRecord(report.getServiceName());
		}
		boolean trigger = record.updateStats(report);
		this.rrMap.put(report.getServiceName(), record);
		return trigger;
	}

	@Override
	public boolean setState(String name, int epoch, ReconfigurationRecord.RCStates state) {
		ReconfigurationRecord<NodeIDType> record = this.getReconfigurationRecord(name);
		if(record==null) return false;
		System.out.println("-------------> RC"+myID+" setting state for "+ name+":"+epoch+" to " + state);
		record.setState(state);
		this.rrMap.put(name, record);
		return true;
	}

	@Override
	public boolean setStateIfReady(String name, int epoch, RCStates state) {
		ReconfigurationRecord<NodeIDType> record = this.getReconfigurationRecord(name, epoch);
		if(record==null) return false;
		if(record.getState()==RCStates.READY) {
			setState(name, epoch, state);
			return true;
		}
		return false;
	}
}
