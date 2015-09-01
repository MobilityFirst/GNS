/*
 * Copyright (c) 2015 University of Massachusetts
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 * 
 * Initial developer(s): V. Arun
 */
package edu.umass.cs.reconfiguration.reconfigurationprotocoltasks;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.umass.cs.nio.GenericMessagingTask;
import edu.umass.cs.protocoltask.ProtocolEvent;
import edu.umass.cs.protocoltask.ProtocolExecutor;
import edu.umass.cs.protocoltask.ProtocolTask;
import edu.umass.cs.protocoltask.ThresholdProtocolTask;
import edu.umass.cs.reconfiguration.ReconfigurationConfig.RC;
import edu.umass.cs.reconfiguration.Reconfigurator;
import edu.umass.cs.reconfiguration.RepliconfigurableReconfiguratorDB;
import edu.umass.cs.reconfiguration.reconfigurationpackets.AckStopEpoch;
import edu.umass.cs.reconfiguration.reconfigurationpackets.RCRecordRequest;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReconfigurationPacket;
import edu.umass.cs.reconfiguration.reconfigurationpackets.StartEpoch;
import edu.umass.cs.reconfiguration.reconfigurationpackets.StopEpoch;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReconfigurationPacket.PacketType;
import edu.umass.cs.reconfiguration.reconfigurationutils.ReconfigurationRecord;
import edu.umass.cs.reconfiguration.reconfigurationutils.ReconfigurationRecord.RCStates;
import edu.umass.cs.utils.Config;
import edu.umass.cs.utils.DelayProfiler;
import edu.umass.cs.utils.MyLogger;

/**
 * @author V. Arun
 * @param <NodeIDType>
 */
/*
 * This protocol task is initiated at a reconfigurator to await a majority of
 * acknowledgments from active replicas for StopEpoch messages.
 */
public class WaitAckStopEpoch<NodeIDType>
		extends
		ThresholdProtocolTask<NodeIDType, ReconfigurationPacket.PacketType, String> {

	/**
	 * Retransmission timeout. The restart periods for most other
	 * reconfiguration protocol tasks are multiples of this, so it should be
	 * changed with care.
	 */
	public static final long RESTART_PERIOD = Config.getGlobalLong(RC.STOP_TASK_RESTART_PERIOD);//2000;

	private final String key;
	private final StopEpoch<NodeIDType> stopEpoch;
	protected final StartEpoch<NodeIDType> startEpoch; // just convenient to
														// remember this
	protected final RepliconfigurableReconfiguratorDB<NodeIDType> DB;
	private Iterator<NodeIDType> nodeIterator = null;

	private String finalState = null;
	protected int restartCount = 0;
	private final long initTime = System.currentTimeMillis();

	protected static final Logger log = Reconfigurator.getLogger();

	/**
	 * @param startEpoch
	 * @param DB
	 */
	public WaitAckStopEpoch(StartEpoch<NodeIDType> startEpoch,
			RepliconfigurableReconfiguratorDB<NodeIDType> DB) {
		super(startEpoch.getPrevEpochGroup(), 1);
		this.stopEpoch = new StopEpoch<NodeIDType>(DB.getMyID(),
				startEpoch.getPrevGroupName(), startEpoch.getPrevEpochNumber(),
				startEpoch.isMerge());
		this.startEpoch = startEpoch;
		this.nodeIterator = startEpoch.getPrevEpochGroup().iterator();
		this.DB = DB;
		this.key = this.refreshKey();
		this.setPeriod(RESTART_PERIOD);
	}

	@Override
	public GenericMessagingTask<NodeIDType, ?>[] restart() {
		if (this.amObviated()) {
			log.log(Level.INFO,
					"{0} canceling itself as obviated; startEpoch = {1}",
					new Object[] { this, this.startEpoch });
			ProtocolExecutor.cancel(this);
			return null;
		}
		// else
		if (++restartCount % 2 == 0)
			log.log(Level.WARNING, MyLogger.FORMAT[2],
					new Object[] { this.refreshKey(), " resending ",
							this.stopEpoch.getSummary() });
		return start();
	}

	/*
	 * If DB has already moved on beyond the epoch being stopped, then we might
	 * as well commit suicide. A stop epoch task may not get a response if there
	 * is no state for the name at the previous epoch replicas. The only way to
	 * recognize this at a reconfigurator is to check the DB state.
	 */
	protected boolean amObviated() {
		ReconfigurationRecord<NodeIDType> record = this.DB
				.getReconfigurationRecord(this.stopEpoch.getServiceName());
		if (record == null // nothing to delete
				// moved on
				|| (record.getEpoch() - this.stopEpoch.getEpochNumber() > 0)
				// moved beyond WAIT_ACK_STOP
				|| (record.getEpoch() == this.stopEpoch.getEpochNumber() && record
						.getState().equals(RCStates.WAIT_DELETE))) {
			log.log(Level.INFO, "{0} obviated because record = ", new Object[] {
					this.getKey(), record!=null ? record.getSummary() : record});
			return true;
		}
		return false;
	}

	@Override
	public GenericMessagingTask<NodeIDType, ?>[] start() {
		/*
		 * Creation epoch (nothing to stop) or split reconfiguration where the
		 * splittee will be stopped by the reconfiguration operation for that
		 * splittee group. In both cases, we don't actually need to stop
		 * anything, so we spoof an AckStopEpoch to self.
		 * 
		 * We *must not* stop the splittee group because it needs to first
		 * commit a reconfiguration intent and then do the stop itself;
		 * otherwise we may prevent the intent from ever getting committed and
		 * be stuck.
		 */
		if (this.startEpoch.noPrevEpochGroup() || this.startEpoch.isSplit()) {
			return new GenericMessagingTask<NodeIDType, AckStopEpoch<NodeIDType>>(
					this.DB.getMyID(), new AckStopEpoch<NodeIDType>(
							this.startEpoch.getInitiator(),
							new StopEpoch<NodeIDType>(
									this.startEpoch.getInitiator(),
									this.startEpoch.getServiceName(),
									this.startEpoch.getEpochNumber() - 1)))
					.toArray();
		}
		NodeIDType nextNode = getNextNode();
		log.log(Level.INFO, "{0} sending {1} to {2}", new Object[] { this,
				this.stopEpoch.getSummary(), nextNode });
		// else send stopEpoch sequentially to old actives and await a response
		return this.startEpoch.hasPrevEpochGroup() ? new GenericMessagingTask<NodeIDType, StopEpoch<NodeIDType>>(
				nextNode, this.stopEpoch).toArray() : null;
	}

	private NodeIDType getNextNode() {
		if (!this.nodeIterator.hasNext())
			this.nodeIterator = startEpoch.getPrevEpochGroup().iterator();
		return (this.nodeIterator.next());
	}

	/**
	 * Note: Trying to start this task when one is already running will cause
	 * the executor to get stuck.
	 * 
	 * @return The refreshed key.
	 */
	public String refreshKey() {
		return Reconfigurator.getTaskKey(getClass(), stopEpoch, this.DB
				.getMyID().toString());
	}

	/**
	 * Packet types handled.
	 */
	public static final ReconfigurationPacket.PacketType[] types = { ReconfigurationPacket.PacketType.ACK_STOP_EPOCH };

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
		// asserted above
		AckStopEpoch<NodeIDType> ackStopEpoch = (AckStopEpoch<NodeIDType>) event;
		log.log(Level.FINE, "{0} received {1} ", new Object[] { this,
				ackStopEpoch.getSummary() });
		this.startEpoch.setFirstPrevEpochCandidate(ackStopEpoch.getSender());
		// finalState can not be null
		if (this.stopEpoch.shouldGetFinalState())
			return (this.finalState = ackStopEpoch.getFinalState()) != null;
		return true;
	}

	// Send startEpoch when stopEpoch is committed
	@Override
	public GenericMessagingTask<NodeIDType, ?>[] handleThresholdEvent(
			ProtocolTask<NodeIDType, PacketType, String>[] ptasks) {
		if (this.DB.isRCGroupName(this.startEpoch.getServiceName())) {
			DelayProfiler.updateDelay("stopRCEpoch", this.initTime);
		} else
			DelayProfiler.updateDelay("stopServiceEpoch", this.initTime);

		log.log(Level.INFO, "{0} received ACK_{1} ", new Object[] { this,
				this.stopEpoch.getSummary() });

		GenericMessagingTask<NodeIDType, ?>[] mtasks = null;
		// no next epoch group means delete record
		if (this.startEpoch.noCurEpochGroup()) {
			assert (!startEpoch.isMerge());
			ptasks[0] = new WaitAckDropEpoch<NodeIDType>(this.startEpoch,
					this.DB);
			// about to delete RC record, but send confirmation to client anyway
			return this.prepareDeleteIntent();
		} else if (this.startEpoch.isMerge()) {
			RCRecordRequest<NodeIDType> merge = new RCRecordRequest<NodeIDType>(
					this.DB.getMyID(), new StartEpoch<NodeIDType>(
							this.startEpoch, this.finalState),
					RCRecordRequest.RequestTypes.RECONFIGURATION_MERGE);
			mtasks = (new GenericMessagingTask<NodeIDType, Object>(
					this.DB.getMyID(), merge)).toArray();
			log.log(Level.INFO, "{0} sending out {1}", new Object[] { this,
					merge.getSummary() });
		} else
			// else start next epoch group
			ptasks[0] = new WaitAckStartEpoch<NodeIDType>(this.startEpoch,
					this.DB);

		return mtasks; // ptasks[0].start() will actually send the startEpoch
	}

	private GenericMessagingTask<NodeIDType, ?>[] prepareDeleteIntent() {
		RCRecordRequest<NodeIDType> rcRecReq = new RCRecordRequest<NodeIDType>(
				this.DB.getMyID(), this.startEpoch,
				RCRecordRequest.RequestTypes.RECONFIGURATION_COMPLETE);
		return (new GenericMessagingTask<NodeIDType, Object>(this.DB.getMyID(),
				rcRecReq)).toArray();
	}

	public String toString() {
		return this.getKey();
	}

}
