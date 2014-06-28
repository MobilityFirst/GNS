package edu.umass.cs.gns.protocoltask.json;

import java.util.Set;
import java.util.TreeSet;

import edu.umass.cs.gns.nio.MessagingTask;
import edu.umass.cs.gns.nsdesign.packet.Packet;
import edu.umass.cs.gns.protocoltask.ProtocolEvent;
import edu.umass.cs.gns.protocoltask.ProtocolExecutor;
import edu.umass.cs.gns.protocoltask.ProtocolTask;
import edu.umass.cs.gns.protocoltask.SchedulableProtocolTask;
import edu.umass.cs.gns.replicaCoordination.multipaxos.paxosutil.WaitforUtility;
import edu.umass.cs.gns.util.Util;

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
public abstract class ThresholdProtocolTask implements
		SchedulableProtocolTask<Packet.PacketType, Long> {
	private final WaitforUtility waitfor;
	private final int threshold;

	public ThresholdProtocolTask(Set<Integer> nodes) { // default all
		this.waitfor = new WaitforUtility(Util.setToArray(nodes));
		this.threshold = nodes.size();
	}

	public ThresholdProtocolTask(Set<Integer> nodes, int threshold) {
		this.waitfor = new WaitforUtility(Util.setToArray(nodes));
		this.threshold = threshold;
	}

	public abstract boolean handleEvent(
			ProtocolEvent<Packet.PacketType, Long> event); // return value tells if event is a valid response

	public abstract MessagingTask[] handleThresholdEvent(
			ProtocolTask<Packet.PacketType, Long>[] ptasks); // action when threshold is reached

	public MessagingTask[] handleEvent(
			ProtocolEvent<Packet.PacketType, Long> event,
			ProtocolTask<Packet.PacketType, Long>[] ptasks) {
		boolean validResponse = this.handleEvent(event);
		assert (event.getMessage() instanceof ProtocolPacket);
		if (validResponse)
			this.waitfor.updateHeardFrom(((ProtocolPacket) (event.getMessage())).getSender());
		if (this.waitfor.getHeardCount() >= this.threshold) {
			// got valid responses from threshold nodes
			MessagingTask[] mtasks = this.handleThresholdEvent(ptasks);
			if (MessagingTask.isEmpty(mtasks) && ptasks[0] == null)
				ProtocolExecutor.cancel(this);
		}
		return null;
	}

	public MessagingTask[] fix(MessagingTask[] mtasks) {
		System.out.println("Filtering heardFrom nodes " +
				Util.arrayToString(this.waitfor.getMembersHeardFrom()));
		if (mtasks == null || mtasks.length == 0 || mtasks[0] == null ||
				mtasks[0].msgs == null || mtasks[0].msgs.length == 0 ||
				mtasks[0].msgs[0] == null)
			return mtasks;
		MessagingTask[] fixed = new MessagingTask[mtasks.length];
		for (int i = 0; i < mtasks.length; i++) {
			fixed[i] = fix(mtasks[i]);
		}
		return fixed;
	}

	private MessagingTask fix(MessagingTask mtask) {
		if (mtask == null || mtask.msgs == null || mtask.msgs.length == 0)
			return null;
		return new MessagingTask(fix(this.waitfor.getMembersHeardFrom(),
				mtask.recipients), mtask.msgs);
	}

	private int[] fix(int[] filter, int[] original) {
		if (filter == null || filter.length == 0)
			return original;
		TreeSet<Integer> filtered = new TreeSet<Integer>();
		for (int i : original) {
			boolean toFilter = false;
			for (int j : filter)
				if (i == j)
					toFilter = true;
			if (!toFilter)
				filtered.add(i);
		}
		return Util.setToArray(filtered);
	}
}
