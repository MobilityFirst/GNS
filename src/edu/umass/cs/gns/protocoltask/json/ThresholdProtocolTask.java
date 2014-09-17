package edu.umass.cs.gns.protocoltask.json;

import java.util.Set;
import java.util.TreeSet;

import edu.umass.cs.gns.nio.GenericMessagingTask;
import edu.umass.cs.gns.nio.MessagingTask;
import edu.umass.cs.gns.nsdesign.nodeconfig.NodeId;
import edu.umass.cs.gns.protocoltask.ProtocolEvent;
import edu.umass.cs.gns.protocoltask.ProtocolExecutor;
import edu.umass.cs.gns.protocoltask.ProtocolTask;
import edu.umass.cs.gns.protocoltask.SchedulableProtocolTask;
import edu.umass.cs.gns.protocoltask.ThresholdProtocolEvent;
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
public abstract class ThresholdProtocolTask<NodeIDType, EventType, KeyType> implements
		SchedulableProtocolTask<NodeIDType, EventType, KeyType> {
	private final Waitfor<NodeIDType> waitfor;
	private final int threshold;

	public ThresholdProtocolTask(Set<NodeIDType> nodes) { // default all
		this.waitfor = new Waitfor<NodeIDType>(nodes.toArray());
		this.threshold = nodes.size();
	}

	public ThresholdProtocolTask(Set<NodeId<String>> nodes, int threshold) {
		this.waitfor = new Waitfor<NodeIDType>(nodes.toArray());
		this.threshold = threshold;
	}

	public abstract boolean handleEvent(
			ProtocolEvent<EventType, KeyType> event); // return value tells if event is a valid response

	public abstract GenericMessagingTask<NodeIDType,?>[] handleThresholdEvent(
			ProtocolTask<NodeIDType, EventType, KeyType>[] ptasks); // action when threshold is reached

	@SuppressWarnings("unchecked")
	public GenericMessagingTask<NodeIDType,?>[] handleEvent(
			ProtocolEvent<EventType, KeyType> event,
			ProtocolTask<NodeIDType, EventType, KeyType>[] ptasks) {
		boolean validResponse = this.handleEvent(event);
		assert (event.getMessage() instanceof ProtocolPacket);
		if (validResponse)
			this.waitfor.updateHeardFrom(((ThresholdProtocolEvent<NodeIDType,EventType,KeyType>)event).getSender());
		if (this.waitfor.getHeardCount() >= this.threshold) {
			// got valid responses from threshold nodes
			GenericMessagingTask<NodeIDType,?>[] mtasks = this.handleThresholdEvent(ptasks);
			if (MessagingTask.isEmpty(mtasks) && ptasks[0] == null)
				ProtocolExecutor.cancel(this);
		}
		return null;
	}

	public GenericMessagingTask<NodeId<String>,?>[] fix(GenericMessagingTask<NodeId<String>,?>[] mtasks) {
		//System.out.println("Filtering heardFrom nodes " + Util.arrayToString(this.waitfor.getMembersHeardFrom()));
		if (mtasks == null || mtasks.length == 0 || mtasks[0] == null ||
				mtasks[0].msgs == null || mtasks[0].msgs.length == 0 ||
				mtasks[0].msgs[0] == null)
			return mtasks;
		MessagingTask[] fixed = new MessagingTask[mtasks.length];
		for (int i = 0; i < mtasks.length; i++) {
			fixed[i] = (MessagingTask) fix(mtasks[i]);
		}
		return fixed;
	}

	public GenericMessagingTask<NodeId<String>,?> fix(GenericMessagingTask<NodeId<String>,?> mtask) {
		if (mtask == null || mtask.msgs == null || mtask.msgs.length == 0)
			return null;
		return new GenericMessagingTask<NodeId<String>,Object>(fix(this.waitfor.getMembersHeardFrom(),
				mtask.recipients), mtask.msgs);
	}

	private Object[] fix(Object[] filter, Object[] original) {
		if (filter == null || filter.length == 0)
			return original;
		TreeSet<NodeIDType> filtered = new TreeSet<NodeIDType>();
		for (Object obji : original) {
			@SuppressWarnings("unchecked")
			NodeIDType i = (NodeIDType)obji;
			boolean toFilter = false;
			for (Object objj : filter) {
				@SuppressWarnings("unchecked")
				NodeIDType j = (NodeIDType)objj;
				if (i == j)
					toFilter = true;
			}
			if (!toFilter)
				filtered.add(i);
		}
		return filtered.toArray();
	}
}
