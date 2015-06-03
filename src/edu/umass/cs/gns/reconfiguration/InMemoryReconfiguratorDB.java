package edu.umass.cs.gns.reconfiguration;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.DemandReport;
import edu.umass.cs.gns.reconfiguration.reconfigurationutils.ConsistentReconfigurableNodeConfig;
import edu.umass.cs.gns.reconfiguration.reconfigurationutils.ReconfigurationRecord;
import edu.umass.cs.gns.reconfiguration.reconfigurationutils.ReconfigurationRecord.RCStates;
import edu.umass.cs.utils.MyLogger;

/**
 * @author V. Arun
 */

/*
 * FIXME: Currently, this logger simply maintains a memory map instead of a
 * persistent DB.
 */
public class InMemoryReconfiguratorDB<NodeIDType> extends
		AbstractReconfiguratorDB<NodeIDType> implements
		InterfaceReconfiguratorDB<NodeIDType> {

	private final HashMap<String, ReconfigurationRecord<NodeIDType>> rrMap = new HashMap<String, ReconfigurationRecord<NodeIDType>>();

	public static final Logger log = Logger.getLogger(Reconfigurator.class
			.getName());

	public InMemoryReconfiguratorDB(NodeIDType myID,
			ConsistentReconfigurableNodeConfig<NodeIDType> nc) {
		super(myID, nc);
	}

	@Override
	public synchronized ReconfigurationRecord<NodeIDType> getReconfigurationRecord(
			String name) {
		return this.rrMap.get(name);
	}

	@Override
	public synchronized boolean updateDemandStats(
			DemandReport<NodeIDType> report) {
		String name = report.getServiceName();
		ReconfigurationRecord<NodeIDType> record = this.rrMap.get(name);
		if (record == null)
			return false; // record = createRecord(name);
		boolean trigger = record.updateStats(report);
		this.rrMap.put(report.getServiceName(), record);
		return trigger;
	}

	// The one method where all state changes are done.
	@Override
	public synchronized boolean setState(String name, int epoch,
			ReconfigurationRecord.RCStates state) {
		ReconfigurationRecord<NodeIDType> record = this
				.getReconfigurationRecord(name);
		if (record == null)
			return false;

		log.log(Level.INFO,
				MyLogger.FORMAT[8],
				new Object[] { "==============================> DerbyRCDB",
						myID, record.getName(), record.getEpoch(),
						record.getState(), " ->", epoch, state,
						record.getNewActives() });
		record.setState(name, epoch, state);
		if (state.equals(RCStates.READY)) {
			record.setActivesToNewActives();
		}
		this.rrMap.put(name, record); // unnecessary
		return true;
	}

	/*
	 * state can be changed only if the current state is READY and if so, it can
	 * only be changed to WAIT_ACK_STOP. The epoch argument must also match the
	 * current epoch number.
	 */
	@Override
	public synchronized boolean setStateInitReconfiguration(String name,
			int epoch, RCStates state, Set<NodeIDType> newActives,
			NodeIDType primary) {
		ReconfigurationRecord<NodeIDType> record = this
				.getReconfigurationRecord(name);
		assert (record != null && (epoch - record.getEpoch() == 0)) : epoch
				+ "!=" + record.getEpoch() + " at " + myID;
		if (!(record.getState().equals(RCStates.READY)))
			return false;
		assert (state.equals(RCStates.WAIT_ACK_STOP));
		log.log(Level.INFO,
				MyLogger.FORMAT[8],
				new Object[] { "==============================> DerbyRCDB",
						myID, record.getName(), record.getEpoch(),
						record.getState(), " ->", epoch, state,
						record.getNewActives() });
		record.setState(name, epoch, state);
		this.rrMap.put(name, record); // unnecessary
		return true;
	}

	@Override
	public synchronized Integer getEpoch(String name) {
		ReconfigurationRecord<NodeIDType> record = this
				.getReconfigurationRecord(name);
		return record.getEpoch();
	}

	// This also sets newActives
	@Override
	public synchronized ReconfigurationRecord<NodeIDType> createReconfigurationRecord(
			ReconfigurationRecord<NodeIDType> record) {
		assert (this.getReconfigurationRecord(record.getName()) == null);
		this.rrMap.put(record.getName(), record);
		return record;
	}

	/******************** Incomplete paxos methods below **************/
	// FIXME:

	@Override
	public String getState(String rcGroup) {
		String state = "";
		for (ReconfigurationRecord<NodeIDType> record : this.rrMap.values()) {
			if (rcGroup.equals(this.consistentNodeConfig
					.getReplicatedReconfigurators(record.getName()).iterator()
					.next().toString()))
				state += record.toString() + "\n";
		}
		return state;
	}

	@Override
	public boolean updateState(String rcGroup, String state) {
		String[] lines = state.split("\n");
		for (String line : lines) {
			ReconfigurationRecord<NodeIDType> record = null;
			try {
				record = new ReconfigurationRecord<NodeIDType>(new JSONObject(
						line), this.consistentNodeConfig);
			} catch (JSONException e) {
				e.printStackTrace();
			}
			this.rrMap.put(record.getName(), record);
		}
		return true;
	}

	@Override
	public String getDemandStats(String name) {
		return null;
	}

	@Override
	public boolean deleteReconfigurationRecord(
			String name, int epoch) {
		ReconfigurationRecord<NodeIDType> record = this
				.getReconfigurationRecord(name);
		log.log(Level.INFO,
				MyLogger.FORMAT[5],
				new Object[] { "==============================> DerbyRCDB",
						myID, record.getName(), record.getEpoch(),
						record.getState(), " -> DELETE" });
		return this.rrMap.remove(name)!=null;
	}
	
	@Override
	public String[] getPendingReconfigurations() {
		return new String[0];
	}

	@Override
	public void close() {
		// do nothing
	}

	@Override
	public String getFinalState(String name, int epoch) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean addReconfigurator(NodeIDType node,
			InetSocketAddress sockAddr, int version) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean garbageCollectOldReconfigurators(int version) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Map<NodeIDType, InetSocketAddress> getRCNodeConfig(boolean maxOnly) {
		// TODO Auto-generated method stub
		return null;
	}
	

	@Override
	public boolean mergeState(String name, int epoch, String mergee,
			String state) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void clearMerged(String name, int epoch) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Set<String> getRCGroupNames() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, Set<NodeIDType>> getRCGroups() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setRCEpochs(ReconfigurationRecord<NodeIDType> ncRecord) {
		// TODO Auto-generated method stub
		
	}

}
