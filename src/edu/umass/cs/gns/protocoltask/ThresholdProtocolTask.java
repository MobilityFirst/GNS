package edu.umass.cs.gns.protocoltask;

import java.util.Set;
import java.util.TreeSet;

import edu.umass.cs.gns.nio.GenericMessagingTask;
import edu.umass.cs.gns.protocoltask.json.ProtocolPacket;
import edu.umass.cs.gns.util.Waitfor;


/**
 * @author V. Arun
 */

/*
 * This abstract class is concretized for Long keys, int node IDs, and JSON-based ProtocolPackets.
 * Its purpose is to receive responses from a threshold number of nodes in the specified set. To
 * enable this, instantiators of this abstract class must implement (1) a "boolean handleEvent(event)"
 * method that says whether or not the response was valid, which helps ThresholdProtocolTask
 * automatically stop retrying with nodes that have already responded; (2) a handleThresholdEvent(.)
 * method that returns a protocol task to be executed when the threshold is reached (returning null
 * automatically cancels the task as it is considered complete).
 */
public abstract class ThresholdProtocolTask<NodeIDType,EventType,KeyType> implements
		SchedulableProtocolTask<NodeIDType, EventType, KeyType> {
	private final Waitfor<NodeIDType> waitfor;
	private final int threshold;
	private boolean thresholdHandlerInvoked=false;

	public ThresholdProtocolTask(Set<NodeIDType> nodes) { // default all
		this.waitfor = new Waitfor<NodeIDType>(nodes);
		this.threshold = nodes.size();
	}

	public ThresholdProtocolTask(Set<NodeIDType> nodes, int threshold) {
		this.waitfor = new Waitfor<NodeIDType>(nodes);
		this.threshold = threshold;
	}

	public abstract boolean handleEvent(
			ProtocolEvent<EventType, KeyType> event); // return value tells if event is a valid response

	// default action is to cancel the protocol task when the threshold is reached
	public GenericMessagingTask<NodeIDType,?>[] handleThresholdEvent(
			ProtocolTask<NodeIDType, EventType, KeyType>[] ptasks) { // action when threshold is reached
		ProtocolExecutor.cancel(this);

		return null;
	}

	@SuppressWarnings("unchecked")
	public GenericMessagingTask<NodeIDType,?>[] handleEvent(
			ProtocolEvent<EventType, KeyType> event,
			ProtocolTask<NodeIDType, EventType, KeyType>[] ptasks) {
		boolean validResponse = this.handleEvent(event);
		assert (event.getMessage() instanceof ProtocolPacket);
		if (validResponse)
			this.waitfor.updateHeardFrom(((ThresholdProtocolEvent<NodeIDType,?,?>)event).getSender());
		GenericMessagingTask<NodeIDType,?>[] mtasks = null;
		if (this.waitfor.getHeardCount() >= this.threshold && testAndInvokeThresholdHandler()) {
			// got valid responses from threshold nodes
			mtasks = this.handleThresholdEvent(ptasks);
			if (GenericMessagingTask.isEmpty(mtasks) && ptasks[0] == null) ProtocolExecutor.cancel(this); // FIXME:
			else ProtocolExecutor.enqueueCancel(this.getKey());
		}
		return mtasks;
	}

	public GenericMessagingTask<NodeIDType,?>[] fix(GenericMessagingTask<NodeIDType,?>[] mtasks) {
		if (mtasks == null || mtasks.length == 0 || mtasks[0] == null ||
				mtasks[0].msgs == null || mtasks[0].msgs.length == 0 ||
				mtasks[0].msgs[0] == null)
			return mtasks;
		for (int i = 0; i < mtasks.length; i++) {
			mtasks[i] = fix(mtasks[i]);
		}
		return mtasks;
	}
	
	public GenericMessagingTask<NodeIDType,?> fix(GenericMessagingTask<NodeIDType,?> mtask) {
		if (mtask == null || mtask.msgs == null || mtask.msgs.length == 0)
			return null;
		return new GenericMessagingTask<NodeIDType,Object>(fix(this.waitfor.getMembersHeardFrom(),
				mtask.recipients), mtask.msgs);
	}

	private Object[] fix(Set<NodeIDType> filter, Object[] original) {
		if (filter == null || filter.size() == 0)
			return original;
		TreeSet<NodeIDType> filtered = new TreeSet<NodeIDType>();
		for (Object obji : original) {
			@SuppressWarnings("unchecked")
			NodeIDType i = (NodeIDType)obji;
			boolean toFilter = false;
			for (NodeIDType objj : filter) {
				if (i.equals(objj))
					toFilter = true;
			}
			if (!toFilter)
				filtered.add(i);
		}
		return filtered.toArray();
	}
	private synchronized boolean testAndInvokeThresholdHandler() {
		if(!this.thresholdHandlerInvoked) return (this.thresholdHandlerInvoked=true);
		return false;
	}
}
