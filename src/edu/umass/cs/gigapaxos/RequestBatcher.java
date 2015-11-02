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
import edu.umass.cs.nio.NIOTransport;
import edu.umass.cs.utils.Config;
import edu.umass.cs.utils.DelayProfiler;
import edu.umass.cs.utils.Util;

/**
 * @author arun
 *
 *         A utility class to consume batched requests.
 */
public class RequestBatcher extends ConsumerTask<RequestPacket> {

	private static final int MAX_BATCH_SIZE = Config
			.getGlobalInt(PC.MAX_BATCH_SIZE);
	private static final double MIN_BATCH_SLEEP_DURATION = Config
			.getGlobalDouble(PC.BATCH_SLEEP_DURATION);
	private static final double BATCH_OVERHEAD = Config
			.getGlobalDouble(PC.BATCH_OVERHEAD);

	private final HashMap<String, LinkedBlockingQueue<RequestPacket>> batched;
	private final PaxosManager<?> paxosManager;
	private static double agreementLatency = 0;

	/**
	 * @param lock
	 *            Used for synchronization by abstract ConsumerTask<TaskType>.
	 * @param paxosManager
	 *            Needed to consume requests by invoking
	 *            {@code paxosManager.handleIncomingPacketInternal}.
	 */
	private RequestBatcher(
			HashMap<String, LinkedBlockingQueue<RequestPacket>> lock,
			PaxosManager<?> paxosManager) {
		super(lock);
		this.batched = lock;
		this.paxosManager = paxosManager;
	}

	/**
	 * @param paxosManager
	 */
	public RequestBatcher(PaxosManager<?> paxosManager) {
		this(new HashMap<String, LinkedBlockingQueue<RequestPacket>>(),
				paxosManager);
	}

	@Override
	public void process(RequestPacket task) {
		this.paxosManager.proposeBatched(task);
	}


	protected synchronized static void updateSleepDuration(long entryTime) {
		agreementLatency = Util.movingAverage(((double) (System.currentTimeMillis() - entryTime)),
				agreementLatency);
		if (Util.oneIn(10))
			DelayProfiler.updateDelay("latency", entryTime);
	}

	// just to name the thread, otherwise super suffices
	public void start() {
		Thread me = (new Thread(this));
		me.setName(RequestBatcher.class.getSimpleName()
				+ this.paxosManager.getMyID());
		me.start();
	}
	
	private int avgNumQGroups = 1;

	// max time for which the dequeueing thread will wait 
	private static final long MAX_BATCH_SLEEP_DURATION = 10;
	// min delay for enqueue/dequeue to happen at all
	private static final long MIN_AGREEMENT_LATENCY_FOR_BATCHING = 10;
	// max queued groups after which we stop any batch sleeps
	private static final int MAX_GROUPS_FOR_BATCH_SLEEP = 5;
	// FIXME: currently not actually used in throttleExcessiveLoad
	private static final int MAX_QUEUED_REQUESTS = Config.getGlobalInt(PC.MAX_OUTSTANDING_REQUESTS);
	@Override
	public void enqueueImpl(RequestPacket task) {
		this.setSleepDuration(this.computeSleepDuration());

		// increase outstanding count by requests entering through me
		//this.paxosManager.incrNumOutstanding(task.setEntryReplicaAndReturnCount(this.paxosManager.getMyID()));
		this.paxosManager.incrNumOutstanding(task.addDebugInfo("b", this.paxosManager.getMyID()));
		
		LinkedBlockingQueue<RequestPacket> taskList = this.batched.get(task
				.getPaxosID());
		if (taskList == null)
			taskList = new LinkedBlockingQueue<RequestPacket>();
		taskList.add(task);
		this.batched.put(task.getPaxosID(), taskList);
		this.queueSize += task.batchSize()+1;
		this.avgNumQGroups = (int)Util.movingAverage(this.batched.size(), this.avgNumQGroups);
		this.throttleExcessiveLoad();
	}
	
	private double computeSleepDuration() {
		return (Math.max(this.avgNumQGroups, this.batched.size()) < MAX_GROUPS_FOR_BATCH_SLEEP ? Math
				.min(MAX_BATCH_SLEEP_DURATION, MIN_BATCH_SLEEP_DURATION
						+ agreementLatency * BATCH_OVERHEAD)
				/ (Math.max(this.avgNumQGroups, this.batched.size())) : 0);
	}
	
	protected int getQueueSize() {
		return this.queueSize;
	}
	private int queueSize = 0;

	/** 
	 * This load throttling mechanism will propagate the exception so that
	 * the client facing demultiplexer will catch it and take the necessary
	 * action to throttle the incoming request load.
	 * 
	 * @param paxosID
	 */
	private void throttleExcessiveLoad() {
		// FIXME: unused
		if (this.queueSize > MAX_QUEUED_REQUESTS) {
			//throw new OverloadException("Excessive client request load");
		}
	}
	
	protected static boolean shouldEnqueue() {
		return agreementLatency > MIN_AGREEMENT_LATENCY_FOR_BATCHING;
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
		int totalBatchSize = first.batchSize() + 1;
		while (reqPktIter.hasNext()) {
			RequestPacket next = reqPktIter.next();
			// break if not within size limits
			if (// log message or network payload size limit would be reached
			((totalByteLength += next.lengthEstimate()) > (SQLPaxosLogger
					.isLoggingEnabled() ? Math.min(
					NIOTransport.MAX_PAYLOAD_SIZE,
					SQLPaxosLogger.MAX_LOG_MESSAGE_SIZE)
					: NIOTransport.MAX_PAYLOAD_SIZE))
					// batch size limit would be reached
					|| ((totalBatchSize += next.batchSize() + 1) > MAX_BATCH_SIZE))
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

		if (Util.oneIn(10)) 
			DelayProfiler.updateMovAvg("#queued", queueSize);
		assert (first.batchSize() < MAX_BATCH_SIZE);
		queueSize -= (first.batchSize()+1);
		return first;
	}
}
