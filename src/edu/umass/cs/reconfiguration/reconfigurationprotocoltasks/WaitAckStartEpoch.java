/*
 * Copyright (c) 2015 University of Massachusetts
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 * 
 * Initial developer(s): V. Arun
 */
package edu.umass.cs.reconfiguration.reconfigurationprotocoltasks;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.umass.cs.nio.GenericMessagingTask;
import edu.umass.cs.protocoltask.ProtocolEvent;
import edu.umass.cs.protocoltask.ProtocolExecutor;
import edu.umass.cs.protocoltask.ProtocolTask;
import edu.umass.cs.protocoltask.ThresholdProtocolTask;
import edu.umass.cs.reconfiguration.Reconfigurator;
import edu.umass.cs.reconfiguration.RepliconfigurableReconfiguratorDB;
import edu.umass.cs.reconfiguration.reconfigurationpackets.AckStartEpoch;
import edu.umass.cs.reconfiguration.reconfigurationpackets.RCRecordRequest;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReconfigurationPacket;
import edu.umass.cs.reconfiguration.reconfigurationpackets.StartEpoch;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReconfigurationPacket.PacketType;
import edu.umass.cs.reconfiguration.reconfigurationutils.ReconfigurationRecord;
import edu.umass.cs.utils.MyLogger;

/**
 * @author V. Arun
 * @param <NodeIDType>
 */
/*
 * This protocol task is initiated at a reconfigurator to await a majority of
 * acknowledgments for StartEpoch messages from active replicas.
 */
public class WaitAckStartEpoch<NodeIDType>
		extends
		ThresholdProtocolTask<NodeIDType, ReconfigurationPacket.PacketType, String> {

	/*
	 * The restart period here should generally be much longer and should
	 * reflect the time expected to fetch the epoch final state from the
	 * previous epoch replicas. Redundant restarts here won't cause
	 * WaitEpochFinalState to request the state again and again (which would be
	 * bad), but just add the request to the list of notifiees when the epoch
	 * start is complete. Still, there is no value in resending this faster than
	 * needed.
	 */
	private static final long RESTART_PERIOD = 8 * WaitAckStopEpoch.RESTART_PERIOD;
	private static final int MAX_NUM_RESTARTS_AFTER_MAJORITY = 5;

	private final StartEpoch<NodeIDType> startEpoch;
	private final RepliconfigurableReconfiguratorDB<NodeIDType> DB;

	private final Set<NodeIDType> ackers = new HashSet<NodeIDType>();
	private final String key;
	private boolean majority = false;
	private int numRestarts = 0;

	private static final Logger log = Reconfigurator.getLogger();

	/**
	 * @param startEpoch
	 * @param DB
	 */
	public WaitAckStartEpoch(StartEpoch<NodeIDType> startEpoch,
			RepliconfigurableReconfiguratorDB<NodeIDType> DB) {
		super(
				startEpoch.getCurEpochGroup(),
				(!startEpoch.isMerge() ? startEpoch.getCurEpochGroup().size() / 2 + 1
						: 1), !startEpoch.isMerge() ? false : true);
		// need to recreate start epoch just to set initiator to self
		this.startEpoch = new StartEpoch<NodeIDType>(DB.getMyID(), startEpoch);
		this.DB = DB;
		this.key = this.refreshKey();
	}

	@Override
	public GenericMessagingTask<NodeIDType, ?>[] restart() {
		log.log(Level.WARNING, MyLogger.FORMAT[2],
				new Object[] { this.refreshKey(), " re-starting ",
						this.startEpoch.getSummary() });
		// restart if not obviated; but quite after a threshold if majority
		if (!this.amObviated()
				&& (!this.haveMajority() || ++numRestarts < MAX_NUM_RESTARTS_AFTER_MAJORITY))
			return start();
		// else
		ProtocolExecutor.cancel(this);
		return null;
	}

	@Override
	public GenericMessagingTask<NodeIDType, ?>[] start() {
		/*
		 * Send StartEpoch to all new actives and await a majority. But if the
		 * startEpoch is a merge request, we only need to sequentially send
		 * startEpoch requests and it suffices to get an ack from one.
		 * 
		 * Should send startEpoch only to self in case of merge operations.
		 */

		assert (!this.startEpoch.isMerge() || this.startEpoch.curEpochGroup
				.contains(this.DB.getMyID()));
		log.log(Level.INFO,
				MyLogger.FORMAT[2],
				new Object[] { this.refreshKey(), " starting ",
						this.startEpoch.getSummary() });

		GenericMessagingTask<NodeIDType, StartEpoch<NodeIDType>> mtask = !this.startEpoch
				.isMerge() ? new GenericMessagingTask<NodeIDType, StartEpoch<NodeIDType>>(
				this.startEpoch.getCurEpochGroup().toArray(), this.startEpoch)
				: new GenericMessagingTask<NodeIDType, StartEpoch<NodeIDType>>(
						this.DB.getMyID(), this.startEpoch);
		return mtask.toArray();
	}

	protected boolean amObviated() {
		ReconfigurationRecord<NodeIDType> record = this.DB
				.getReconfigurationRecord(this.startEpoch.getServiceName());
		if (record == null
				|| (record.getEpoch() - this.startEpoch.getEpochNumber() > 0))
			return true;
		return false;
	}

	/**
	 * @return The refreshed key.
	 */
	public String refreshKey() {
		return Reconfigurator.getTaskKey(getClass(), startEpoch, this.DB
				.getMyID().toString());
	}

	/**
	 * Packet types handled.
	 */
	public static final ReconfigurationPacket.PacketType[] types = { ReconfigurationPacket.PacketType.ACK_START_EPOCH, };

	@Override
	public Set<PacketType> getEventTypes() {
		return new HashSet<ReconfigurationPacket.PacketType>(
				Arrays.asList(types));
	}

	@Override
	public String getKey() {
		return this.key;
	}

	@SuppressWarnings("unchecked")
	@Override
	public boolean handleEvent(ProtocolEvent<PacketType, String> event) {
		assert (event.getType().equals(types[0]));
		AckStartEpoch<NodeIDType> ackStart = ((AckStartEpoch<NodeIDType>) event);
		log.log(Level.FINE, "{0} received {1} from {2}", new Object[] { this,
				ackStart.getSummary(), ackStart.getSender() });

		/*
		 * It is important to try to start all replicas on a best effort basis
		 * even if we have received a majority, especially for RC group names.
		 * Otherwise, some replicas can get left behind and cause NC changes
		 * to get stuck for long periods of time.
		 */
		this.ackers.add(ackStart.getSender());
		if (this.haveMajority() && this.startEpoch.hasCurEpochGroup()
				&& this.ackers.size() == this.startEpoch.getCurEpochGroup()
						.size()) {
			log.log(Level.INFO, "{0} received ALL ACKS for {1}", new Object[] { this,
					startEpoch.getSummary() });
			ProtocolExecutor.cancel(this); // all done
		}
		return true;
	}

	/*
	 * FIXME: shoud maybe spawn WaitAckDropEoch only after all new actives have
	 * sent acked start epoch as opposed to just a majority. An alternative is
	 * to delay WaitAckDropEoch a bit to allow all new actives to have acked
	 * starting the new epoch.
	 */
	// Send dropEpoch when startEpoch is acked by majority
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public synchronized GenericMessagingTask<NodeIDType, ?>[] handleThresholdEvent(
			ProtocolTask<NodeIDType, ReconfigurationPacket.PacketType, String>[] ptasks) {

		this.setMajority(true);
		log.log(Level.INFO, MyLogger.FORMAT[3], new Object[] {
				this,
				" received MAJORITY ACKs for ",
				startEpoch.getSummary(),
				(this.startEpoch.creator != null ? "; sending ack to client "
						+ this.startEpoch.creator : "") });

		// multicast start epoch confirmation message
		GenericMessagingTask<NodeIDType, ?> epochStartCommit = new GenericMessagingTask(
				this.DB.getMyID(), new RCRecordRequest<NodeIDType>(
						this.DB.getMyID(), this.startEpoch,
						RCRecordRequest.RequestTypes.RECONFIGURATION_COMPLETE));

		/*
		 * Reconfiguration split operations can not just drop previous epoch
		 * final state, otherwise one part of the split may succeed and the
		 * other can be stuck forever.
		 * 
		 * FIXME: We currently just never drop the final state for any RC group
		 * here. We can drop the final state for mergee groups, but we do that
		 * in Reconfigurator when the merge completes executing. For all other
		 * RC groups, this means that RC group reconfigurations will necessarily
		 * be unclean, but that is okay because of the WAIT_DELETE timeout.
		 */
		if (this.startEpoch.hasPrevEpochGroup()) {
			// previous state is never dropped for RC group names
			if (!this.DB.isRCGroupName(this.startEpoch.getServiceName())
			/*
			 * && !this.startEpoch.getServiceName().equals(
			 * AbstractReconfiguratorDB.RecordNames.NODE_CONFIG .toString())
			 */)
				// drop previous epoch final state
				ptasks[0] = new WaitAckDropEpoch<NodeIDType>(this.startEpoch,
						this.DB);
		} else if (this.startEpoch.creator != null) { // creation epoch
			/*
			 * We used to send creation confirmation to client here earlier, but
			 * it is best to do it using
			 * Reconfigurator.sendCreateConfirmationToClient as the client
			 * facing messenger may in general be different.
			 */
		} else
			assert (false);

		/*
		 * Propagate start epoch confirmation to all. But if RC group name, do
		 * so only if I am a reconfigurator present in the new group.
		 */
		return (!this.DB.isRCGroupName(this.startEpoch.getServiceName()) || this.startEpoch
				.getCurEpochGroup().contains(this.DB.getMyID())) ? epochStartCommit
				.toArray() : null;
		// default action will do timed cancel
	}

	public String toString() {
		return this.getKey();
	}

	public long getPeriod() {
		return RESTART_PERIOD;
	}

	private void setMajority(boolean b) {
		this.majority = b;
	}

	private synchronized boolean haveMajority() {
		return this.majority;
	}
}
