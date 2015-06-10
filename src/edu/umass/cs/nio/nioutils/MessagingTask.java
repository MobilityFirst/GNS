package edu.umass.cs.nio.nioutils;

import edu.umass.cs.nio.GenericMessagingTask;
import edu.umass.cs.utils.Util;

/**
 * @author V. Arun
 * 
 *         This messaging task means that each msg needs to be sent to all
 *         recipients. Note: it does *NOT* mean that each msg needs to be sent
 *         to its corresponding recipient; in fact, the sizes of the two arrays,
 *         recipients and msgs, may be different.
 * 
 *         This is a utility class as it is really just a container for two
 *         arrays.
 */
public class MessagingTask extends GenericMessagingTask<Integer, Object> {

	/**
	 * Unicast
	 * 
	 * @param destID
	 * @param pkt
	 */
	public MessagingTask(int destID, Object pkt) {
		super(destID, pkt);
		assert (pkt != null) : "Incorrect usage: MessagingTask should not be instantiated with no messages";
		if (pkt == null)
			throw new RuntimeException(
					"Incorrect usage: MessagingTask should not be instantiated with no messages");
	}

	/**
	 * Multicast
	 * 
	 * @param destIDs
	 * @param pkt
	 */
	public MessagingTask(int[] destIDs, Object pkt) {
		super(Util.intToIntegerArray(destIDs), pkt);
		assert (pkt != null && destIDs != null) : "Incorrect usage: MessagingTask should not be instantiated with null messages or destinations";
		if (!(pkt != null && destIDs != null))
			throw new RuntimeException(
					"Incorrect usage: MessagingTask should not be instantiated with null messages or destinations");
	}

	/**
	 * Unicast multiple packets
	 * 
	 * @param destID
	 * @param pkts
	 */
	public MessagingTask(int destID, Object[] pkts) {
		super(destID, pkts);
		assert (pkts != null && pkts.length > 0 && pkts[0] != null) : "Incorrect usage: MessagingTask should not be instantiated with no messages";
		if (!(pkts != null && pkts.length > 0 && pkts[0] != null))
			throw new RuntimeException(
					"Incorrect usage: MessagingTask should not be instantiated with no messages");

	}

	/**
	 * Multicast multiple packets. This is a special type of constructor
	 * compared to the others above. This is the only constructor that allows
	 * MessagingTask to be instantiated with no messages. We need this as it is
	 * convenient to create an empty MessagingTask in a logging-only
	 * LogMessagingTask object.
	 * 
	 * @param destIDs 
	 * @param pkts 
	 */
	public MessagingTask(int[] destIDs, Object[] pkts) {
		super(Util.intToIntegerArray(destIDs), pkts);
		assert (pkts != null && destIDs != null) : "Incorrect usage: MessagingTask should not be instantiated with null messages or destinations";
		if (!(pkts != null && destIDs != null))
			throw new RuntimeException(
					"Incorrect usage: MessagingTask should not be instantiated with null messages or destinations");
	}

	/**
	 * Converts this messaging task to an array of size one.
	 * 
	 * @return An array of size one containing this messaging task.
	 */
	public MessagingTask[] toArray() {
		MessagingTask[] mtasks = new MessagingTask[1];
		mtasks[0] = this;
		return mtasks;
	}

	/**
	 * @param mtask1
	 * @param mtask2
	 * @return An array of size two containing the two input messaging tasks.
	 */
	public static MessagingTask[] toArray(MessagingTask mtask1,
			MessagingTask mtask2) {
		MessagingTask[] mtasks = new MessagingTask[2];
		mtasks[0] = mtask1;
		mtasks[1] = mtask2;
		return mtasks;
	}
}
