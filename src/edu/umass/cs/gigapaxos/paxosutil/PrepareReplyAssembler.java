package edu.umass.cs.gigapaxos.paxosutil;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import edu.umass.cs.gigapaxos.paxospackets.PValuePacket;
import edu.umass.cs.gigapaxos.paxospackets.PrepareReplyPacket;
import edu.umass.cs.gigapaxos.paxospackets.RequestPacket;
import edu.umass.cs.nio.NIOTransport;
import edu.umass.cs.utils.Util;

/**
 * @author arun
 *
 *         This class is needed because we need to fragment and reassemble large
 *         prepare replies so that each fragment respects NIO's MTU
 *         {@link NIOTransport#MAX_PAYLOAD_SIZE}. We currently limit the size of
 *         batched requests so that it is at most MTU bytes, but that still
 *         means that prepare replies could be much larger as they consist of
 *         all requests from the garbage-collected slot (typically the most
 *         recent checkpoint slot) up until the highest accepted slot. It is
 *         unclear how to precisely limit the size of prepare replies without
 *         significantly hurting performance or liveness. So we don't attempt to
 *         ensure an explicit limit on prepare reply sizes but instead just
 *         fragment and reassemble them if they are too big.
 */
public class PrepareReplyAssembler {

	private static final ConcurrentHashMap<String, ConcurrentHashMap<Integer, PrepareReplyPacket>> preplies = new ConcurrentHashMap<String, ConcurrentHashMap<Integer, PrepareReplyPacket>>();

	/**
	 * @param incoming
	 * @return Reassembled {@link PrepareReplyPacket} if any.
	 */
	public synchronized static PrepareReplyPacket processIncoming(
			PrepareReplyPacket incoming) {

		if (incoming.isComplete())
			return incoming;

		preplies.putIfAbsent(incoming.getPaxosIDVersion(),
				new ConcurrentHashMap<Integer, PrepareReplyPacket>());
		PrepareReplyPacket existing = preplies
				.get(incoming.getPaxosIDVersion()).putIfAbsent(
						incoming.acceptor, incoming);
		// first fragment inserted, wait for more
		if (existing == null)
			return null;

		GC();

		// combine returns true if existing becomes complete
		PrepareReplyPacket retval = (existing.combine(incoming) ? preplies.get(
				incoming.getPaxosIDVersion()).remove(incoming.acceptor) : null);

		// always retain the highest ballot ack'ed by the acceptor
		if (incoming.ballot.compareTo(existing.ballot) > 0)
			preplies.get(incoming.getPaxosIDVersion()).put(incoming.acceptor,
					incoming);

		// remove empty state for paxosID
		if (preplies.get(incoming.getPaxosIDVersion()).isEmpty())
			preplies.remove(incoming.getPaxosIDVersion());

		return retval;
	}

	private static final long MAX_WAITING_TIME = 60000;

	private static void GC() {
		Set<PrepareReplyPacket> removals = new HashSet<PrepareReplyPacket>();
		for (Map<Integer, PrepareReplyPacket> map : preplies.values())
			for (PrepareReplyPacket preply : map.values())
				if (System.currentTimeMillis() - preply.getCreateTime() > MAX_WAITING_TIME)
					removals.add(preply);

		for (PrepareReplyPacket preply : removals) {
			Map<Integer, PrepareReplyPacket> map = preplies.get(preply
					.getPaxosIDVersion());
			if (map != null && map.containsValue(preply))
				map.remove(preply.acceptor);
		}

	}

	/**
	 * @param preply
	 * @return Array of fragment prepare replies.
	 */
	public static PrepareReplyPacket[] fragment(PrepareReplyPacket preply) {
		return fragment(preply, NIOTransport.MAX_PAYLOAD_SIZE);
	}

	private static PrepareReplyPacket[] fragment(PrepareReplyPacket preply,
			int fragmentSize) {
		Set<PrepareReplyPacket> fragments = new HashSet<PrepareReplyPacket>();
		if (preply.getLengthEstimate() <= fragmentSize) {
			fragments.add(preply);
			return fragments.toArray(new PrepareReplyPacket[0]);
		}

		while (!preply.accepted.isEmpty())
			fragments.add(preply.fragment(fragmentSize));
		return fragments.toArray(new PrepareReplyPacket[0]);
	}

	/**
	 * Unit testing code.
	 * 
	 * @param args
	 * @throws InterruptedException
	 */
	public static void main(String[] args) throws InterruptedException {
		Util.assertAssertionsEnabled();
		String paxosID1 = "paxos1", paxosID2 = "paxos2";
		int version1 = 0, version2 = 3;
		int first1 = 0, max1 = 2;
		Ballot ballot1 = new Ballot(43, 578);
		int acceptor1 = 23;
		PrepareReplyPacket preply1 = new PrepareReplyPacket(acceptor1, ballot1,
				new HashMap<Integer, PValuePacket>(), first1 - 1, max1);
		preply1.putPaxosID(paxosID1, version1);
		assert (!preply1.isComplete());
		preply1.accepted.put(0,
				RequestPacket.getRandomPValue(paxosID1, version1, 0, ballot1));
		assert (!preply1.isComplete());
		preply1.accepted.put(1,
				RequestPacket.getRandomPValue(paxosID1, version1, 1, ballot1));
		assert (!preply1.isComplete());
		preply1.accepted.put(2,
				RequestPacket.getRandomPValue(paxosID1, version1, 2, ballot1));
		assert (preply1.isComplete());

		assert (PrepareReplyAssembler.processIncoming(preply1) == preply1);

		int acceptor2 = 25;
		Ballot ballot2 = new Ballot(35, 67);
		int first2 = 20, max2 = 40;
		PrepareReplyPacket preply2 = new PrepareReplyPacket(acceptor2, ballot2,
				new HashMap<Integer, PValuePacket>(), first2 - 1, max2);
		preply2.putPaxosID(paxosID2, version2);
		assert (!preply2.isComplete());
		preply2.accepted.put(first2, RequestPacket.getRandomPValue(paxosID2,
				version2, first2, ballot2));
		assert (PrepareReplyAssembler.processIncoming(preply2) == null);
		System.out.println("preplies after processing preply2: " + preplies);

		PrepareReplyPacket tmp2 = new PrepareReplyPacket(acceptor2, ballot2,
				new HashMap<Integer, PValuePacket>(), first2 - 1, max2);
		for (int i = first2 + 1; i <= max2; i++) {
			tmp2.putPaxosID(paxosID2, version2);
			tmp2.accepted.put(i, RequestPacket.getRandomPValue(paxosID2,
					version2, i, ballot2));
		}
		assert (!tmp2.isComplete());
		// used to manually test GC with a wait time of 1 sec
		Thread.sleep(100);

		assert (PrepareReplyAssembler.processIncoming(tmp2) == preply2);
		System.out.println("preplies after processing tmp2: " + preplies);
		assert (PrepareReplyAssembler.processIncoming(tmp2) == null);

		assert (preply2.isComplete());

		// preply3 is same ballot and acceptor as preply2
		Ballot ballot3 = new Ballot(36, 67);
		PrepareReplyPacket preply3 = new PrepareReplyPacket(acceptor2, ballot3,
				new HashMap<Integer, PValuePacket>(), first2 - 1, max2);
		preply3.putPaxosID(paxosID2, version2);
		preply3.accepted.put(first2, RequestPacket.getRandomPValue(paxosID2,
				version2, first2, ballot2));
		preply3.accepted.put(first2 + 1, RequestPacket.getRandomPValue(
				paxosID2, version2, first2 + 1, ballot2));
		assert (PrepareReplyAssembler.processIncoming(preply3) == null);
		System.out
				.println("preplies slots after processing preply3: "
						+ preplies.get(preply3.getPaxosIDVersion()).get(
								acceptor2).accepted.keySet());
		assert (preplies.get(preply3.getPaxosIDVersion()).get(acceptor2).accepted
				.size() == 2);
		System.out
				.println("preplies slots after processing preply3: "
						+ preplies.get(preply3.getPaxosIDVersion()).get(
								acceptor2).accepted.keySet());
		// tmp2 is lower ballot
		assert (PrepareReplyAssembler.processIncoming(tmp2) != preply3);

		Set<Integer> before = new HashSet<Integer>(tmp2.accepted.keySet());
		PrepareReplyPacket[] fragments = fragment(tmp2, 500);
		System.out.println("Created " + fragments.length + " fragments");
		Set<Integer> after = new HashSet<Integer>();
		for (PrepareReplyPacket fragment : fragments)
			after.addAll(fragment.accepted.keySet());
		assert (after.equals(before));
	}
}
