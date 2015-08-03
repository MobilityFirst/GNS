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
package edu.umass.cs.gigapaxos;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;

import edu.umass.cs.gigapaxos.PaxosConfig.PC;
import edu.umass.cs.gigapaxos.paxospackets.RequestPacket;
import edu.umass.cs.gigapaxos.paxosutil.ConsumerTask;
import edu.umass.cs.utils.Config;
import edu.umass.cs.utils.DelayProfiler;
import edu.umass.cs.utils.Util;

/**
 * @author arun
 *
 *         A utility class to consume batched requests.
 */
public class RequestBatcher extends ConsumerTask<RequestPacket> {
	/*
	 * Warning: setting this to a high value seems to cause problems with long
	 * log messages in derby DB. Increase with care.
	 */
	private static final int MAX_BATCH_SIZE = 1000;
	private static final long SLEEP_DURATION = Config
			.getGlobalLong(PC.BATCH_SLEEP_DURATION);

	private final HashMap<String, LinkedBlockingQueue<RequestPacket>> batched;
	private final PaxosManager<?> paxosManager;
	private static double adaptiveSleepDuration = SLEEP_DURATION;

	/**
	 * @param lock
	 *            Used for synchronization by abstract ConsumerTask<TaskType>.
	 * @param paxosManager
	 *            Needed to consume requests by invoking
	 *            {@code paxosManager.handleIncomingPacketInternal}.
	 */
	public RequestBatcher(HashMap<String, LinkedBlockingQueue<RequestPacket>> lock,
			PaxosManager<?> paxosManager) {
		super(lock);
		this.batched = lock;
		this.paxosManager = paxosManager;
		this.setSleepDuration(SLEEP_DURATION);
	}

	/**
	 * @param paxosManager
	 */
	public RequestBatcher(PaxosManager<?> paxosManager) {
		this(new HashMap<String, LinkedBlockingQueue<RequestPacket>>(), paxosManager);
	}

	@Override
	public void process(RequestPacket task) {
		this.paxosManager.proposeBatched(task.setCreateTime(System
				.currentTimeMillis()));
	}

	protected synchronized static void updateSleepDuration(long sample) {
		adaptiveSleepDuration = Util.movingAverage(adaptiveSleepDuration,
				((double) sample) * Config
				.getGlobalDouble(PC.BATCH_OVERHEAD));
		if (Util.oneIn(10))
			DelayProfiler.updateMovAvg("sleepDuration",
					(int) adaptiveSleepDuration);
	}

	// just to name the thread, otherwise super suffices
	public void start() {
		Thread me = (new Thread(this));
		me.setName(RequestBatcher.class.getSimpleName()
				+ this.paxosManager.getMyID());
		me.start();
	}

	@Override
	public void enqueueImpl(RequestPacket task) {
		this.setSleepDuration(SLEEP_DURATION + (long) adaptiveSleepDuration);
		LinkedBlockingQueue<RequestPacket> taskList = this.batched.get(task.getPaxosID());
		if (taskList == null)
			taskList = new LinkedBlockingQueue<RequestPacket>();
		taskList.add(task);
		this.batched.put(task.getPaxosID(), taskList);
	}

	/*
	 * This method extracts a batched request from enqueued requests of batch
	 * size at most MAX_BATCH_SIZE.
	 */
	@Override
	public RequestPacket dequeueImpl() {
		if (this.batched.isEmpty())
			return null;
		// pluck first list (each grouped by paxosID)
		Iterator<Entry<String, LinkedBlockingQueue<RequestPacket>>> mapEntryIter = this.batched
				.entrySet().iterator();
		Entry<String, LinkedBlockingQueue<RequestPacket>> firstEntry = mapEntryIter
				.next();

		// make a batched request out of this extracted (nonempty) list
		Iterator<RequestPacket> reqPktIter = firstEntry.getValue().iterator();
		assert (reqPktIter.hasNext());
		// first pluck the first request from the list
		RequestPacket first = (reqPktIter.next());
		reqPktIter.remove();

		// then pluck the rest into a batch within the first request
		Set<RequestPacket> batch = new HashSet<RequestPacket>();
		int maxBatchSize = Math.min(MAX_BATCH_SIZE - 1, firstEntry.getValue()
				.size());
		/*
		 * totalByteLength must be less than SQLPaxosLogger.MAX_LOG_MESSAGE_SIZE
		 * that specifies the maximum length of a paxos log message. We use the
		 * method lengthEstimate() below that is a loose upper bound on the
		 * actual length. It is better to be conservative instead of computing
		 * the exact length here for two reasons: (1) we need a
		 * toJSON.toString() conversion just for computing the exact length; (2)
		 * the size can grow because NIO or paxos components might sneak
		 * additional fields as a request packet morphs into a logged accept or
		 * decision. It is much cleaner to prevent exceptions in
		 * AbstractPaxosLogger.logBatch(.) by being cautious here than trying to
		 * salvage the batch or dropping the whole batch in the logger.
		 */
		int totalByteLength = first.lengthEstimate();
		for (int i = 0; i < maxBatchSize; i++) {
			assert (reqPktIter.hasNext());
			RequestPacket next = reqPktIter.next();
			// break if not within size limit
			if ((totalByteLength += next.lengthEstimate()) > SQLPaxosLogger.MAX_LOG_MESSAGE_SIZE)
				break;
			// else add to batch and remove
			batch.add(next);
			reqPktIter.remove();
		}

		// latch plucked sub-list above to the first request
		if (!batch.isEmpty())
			first.latchToBatch(batch.toArray(new RequestPacket[0]));

		// remove first list if all plucked
		if (firstEntry.getValue().isEmpty())// !reqPktIter.hasNext())
			mapEntryIter.remove();

		DelayProfiler.updateMovAvg("batchSize", first.batchSize() + 1);
		return first;
	}
}
