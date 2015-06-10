package edu.umass.cs.reconfiguration;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Set;

import edu.umass.cs.reconfiguration.reconfigurationpackets.DemandReport;
import edu.umass.cs.reconfiguration.reconfigurationutils.ReconfigurationRecord;

/**
 * @author arun
 *
 * @param <NodeIDType>
 */
public interface InterfaceReconfiguratorDB<NodeIDType> {
	/* ******** Methods for individual names below **************** */

	/**
	 * @param name
	 * @return ReconfigurationRecord for {@code name}.
	 */
	public ReconfigurationRecord<NodeIDType> getReconfigurationRecord(
			String name);

	/**
	 * @param record
	 * @return ReconfigurationRecord created.
	 */
	public ReconfigurationRecord<NodeIDType> createReconfigurationRecord(
			ReconfigurationRecord<NodeIDType> record);

	/**
	 * @param name
	 * @param epoch
	 * @return True if record existed and was successfully deleted.
	 */
	public boolean deleteReconfigurationRecord(String name, int epoch);

	/**
	 * Update demand statistics.
	 * 
	 * @param report
	 * @return True if updated successfully.
	 */
	public boolean updateDemandStats(DemandReport<NodeIDType> report);

	/**
	 * @param name
	 * @return Demand statistics as JSON string.
	 */
	public String getDemandStats(String name);

	/**
	 * Set epoch and state as specified.
	 * 
	 * @param name
	 * @param epoch
	 * @param state
	 * @return True if successfully set; false if the state transition is not
	 *         legitimate.
	 */
	public boolean setState(String name, int epoch,
			ReconfigurationRecord.RCStates state);

	/**
	 * Set epoch, state, newActives, primary only if current state is READY
	 * 
	 * @param name
	 * @param epoch
	 * @param state
	 * @param newActives
	 * @param primary
	 * @return True if successfully set; false if the state transition is not
	 *         legitimate.
	 */
	public boolean setStateInitReconfiguration(String name, int epoch,
			ReconfigurationRecord.RCStates state, Set<NodeIDType> newActives,
			NodeIDType primary);

	/**
	 * Names for which reconfiguration is incomplete, needed for recovery
	 * 
	 * @return Array of names.
	 */
	public String[] getPendingReconfigurations();

	/**
	 * Get current RC group names from the DB
	 * 
	 * @return Map of RC group names and their group members.
	 */
	public Map<String, Set<NodeIDType>> getRCGroups();

	/**
	 * @return Set of all RC group names maintained locally.
	 */
	public Set<String> getRCGroupNames();

	/**
	 * @param node
	 * @param sockAddr
	 * @param version
	 * @return True if successfully added.
	 */
	public boolean addReconfigurator(NodeIDType node,
			InetSocketAddress sockAddr, int version);

	/**
	 * @param version
	 * @return Delete deleted reconfigurators.
	 */
	public boolean garbageCollectOldReconfigurators(int version);

	/**
	 * @param maxOnly
	 *            True means only the latest version information is returned.
	 * @return Node config information for reconfigurators as a map.
	 */
	public Map<NodeIDType, InetSocketAddress> getRCNodeConfig(boolean maxOnly);

	/**
	 * Merge state, i.e., append instead of replacing state, exactly once. The
	 * tricky part is to ensure exactly-once semantics, which we ensure by
	 * maintaining a "merged" set in ReconfigurationRecord.
	 * 
	 * @param name
	 * @param epoch
	 * @param mergee
	 * @param state
	 * @return True if merged successfully.
	 */
	public boolean mergeState(String name, int epoch, String mergee,
			String state);

	/**
	 * Clear all merged state in RC record. Invoked when a stop request is
	 * executed just before transitioning to the next epoch.
	 * 
	 * @param name
	 * @param epoch
	 */
	public void clearMerged(String name, int epoch);

	/**
	 * Sets RC epochs so that we know the epoch numbers for each reconfigurator
	 * group.
	 * 
	 * @param ncRecord
	 */
	public void setRCEpochs(ReconfigurationRecord<NodeIDType> ncRecord);

	/**
	 * Close gracefully.
	 */
	public void close();
}
