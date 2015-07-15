package edu.umass.cs.gigapaxos;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;

import edu.umass.cs.gigapaxos.paxospackets.RequestPacket;
import edu.umass.cs.gigapaxos.paxosutil.ConsumerTask;

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
	private static final int MAX_BATCH_SIZE = 100;

	private final HashMap<String, ArrayList<RequestPacket>> batched;
	private final PaxosManager<?> paxosManager;

	/**
	 * @param lock
	 *            Used for synchronization by abstract ConsumerTask<TaskType>.
	 * @param paxosManager
	 *            Needed to consume requests by invoking
	 *            {@code paxosManager.handleIncomingPacketInternal}.
	 */
	public RequestBatcher(HashMap<String, ArrayList<RequestPacket>> lock,
			PaxosManager<?> paxosManager) {
		super(lock);
		this.batched = lock;
		this.paxosManager = paxosManager;
	}

	/**
	 * @param paxosManager
	 */
	public RequestBatcher(PaxosManager<?> paxosManager) {
		this(new HashMap<String, ArrayList<RequestPacket>>(), paxosManager);
	}

	@Override
	public void enqueueImpl(RequestPacket task) {
		ArrayList<RequestPacket> taskList = this.batched.get(task.getPaxosID());
		if (taskList == null)
			taskList = new ArrayList<RequestPacket>();
		taskList.add(task);
		this.batched.put(task.getPaxosID(), taskList); // unnecessary
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
		Iterator<Entry<String, ArrayList<RequestPacket>>> mapEntryIter = this.batched
				.entrySet().iterator();
		Entry<String, ArrayList<RequestPacket>> firstEntry = mapEntryIter
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
		// totalByteLength must be less than SQLPaxosLogger.MAX_LOG_MESSAGE_SIZE
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

		if (!batch.isEmpty())
			first.latchToBatch(batch.toArray(new RequestPacket[0]));

		// remove first list if all plucked
		if (!reqPktIter.hasNext())
			mapEntryIter.remove();
		return first;
	}

	@Override
	public void process(RequestPacket task) {
		this.paxosManager.proposeBatched(task);
	}

	// just to name the thread, otherwise super suffices
	public void start() {
		Thread me = (new Thread(this));
		me.setName(RequestBatcher.class.getSimpleName()
				+ this.paxosManager.getMyID());
		me.start();
	}
}
