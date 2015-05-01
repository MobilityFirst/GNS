package edu.umass.cs.gns.reconfiguration;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Set;

import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.DemandReport;
import edu.umass.cs.gns.reconfiguration.reconfigurationutils.ReconfigurationRecord;

public interface InterfaceReconfiguratorDB<NodeIDType> {
	/********* Methods for individual names below *****************/

	public ReconfigurationRecord<NodeIDType> getReconfigurationRecord(
			String name);

	public ReconfigurationRecord<NodeIDType> createReconfigurationRecord(
			ReconfigurationRecord<NodeIDType> record);
	public boolean deleteReconfigurationRecord(
			String name);

	// update demand stats 
	public boolean updateDemandStats(DemandReport<NodeIDType> report);

	// get demand stats
	public String getDemandStats(String name);

	// set epoch and state as specified
	public boolean setState(String name, int epoch,
			ReconfigurationRecord.RCStates state);

	// set epoch, state, newActives, primary only if current state is READY
	public boolean setStateInitReconfiguration(String name, int epoch,
			ReconfigurationRecord.RCStates state, Set<NodeIDType> newActives, NodeIDType primary);
	
	// names for which reconfiguration is incomplete, needed for recovery
	public String[] getPendingReconfigurations();
	
	// get current RC group names from the DB
	public Set<String> getRCGroupNames();
	
	public boolean addReconfigurator(NodeIDType node, InetSocketAddress sockAddr, int version);

	public boolean deleteReconfigurators(int version);

	public Map<NodeIDType, InetSocketAddress> getRCNodeConfig();
	
	// close DB
	public void close();
}
