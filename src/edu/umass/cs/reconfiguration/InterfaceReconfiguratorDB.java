package edu.umass.cs.reconfiguration;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Set;

import edu.umass.cs.reconfiguration.json.reconfigurationpackets.DemandReport;
import edu.umass.cs.reconfiguration.reconfigurationutils.ReconfigurationRecord;

public interface InterfaceReconfiguratorDB<NodeIDType> {
	/********* Methods for individual names below *****************/

	public ReconfigurationRecord<NodeIDType> getReconfigurationRecord(
			String name);

	public ReconfigurationRecord<NodeIDType> createReconfigurationRecord(
			ReconfigurationRecord<NodeIDType> record);

	public boolean deleteReconfigurationRecord(String name, int epoch);

	// update demand stats
	public boolean updateDemandStats(DemandReport<NodeIDType> report);

	// get demand stats
	public String getDemandStats(String name);

	// set epoch and state as specified
	public boolean setState(String name, int epoch,
			ReconfigurationRecord.RCStates state);

	// set epoch, state, newActives, primary only if current state is READY
	public boolean setStateInitReconfiguration(String name, int epoch,
			ReconfigurationRecord.RCStates state, Set<NodeIDType> newActives,
			NodeIDType primary);

	// names for which reconfiguration is incomplete, needed for recovery
	public String[] getPendingReconfigurations();

	// get current RC group names from the DB
	public Map<String, Set<NodeIDType>> getRCGroups();
	public Set<String> getRCGroupNames();

	public boolean addReconfigurator(NodeIDType node,
			InetSocketAddress sockAddr, int version);

	public boolean garbageCollectOldReconfigurators(int version);

	public Map<NodeIDType, InetSocketAddress> getRCNodeConfig(boolean maxOnly);

	// merge state, i.e., append instead of replacing state, exactly once
	public boolean mergeState(String name, int epoch, String mergee,
			String state);

	// clear all merged state in RC record
	public void clearMerged(String name, int epoch);

	public void setRCEpochs(ReconfigurationRecord<NodeIDType> ncRecord);
	// close DB
	public void close();
}
