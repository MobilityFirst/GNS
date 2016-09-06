package edu.umass.cs.gnsserver.interfaces;

import edu.umass.cs.gnsserver.gnsapp.packet.InternalCommandPacket;

/**
 * @author arun
 * 
 *         This interface captures the information carried in internal requests
 *         that may be spawned either by regular multi-step requests or by
 *         active requests.
 * 
 *         <p>
 *         Implementing classes: {@link InternalCommandPacket}.
 *
 */
public interface InternalRequestHeader {

	/**
	 * The default number of TTL hops.
	 */
	public static final int DEFAULT_TTL = 5;

	/**
	 * @return The request ID corresponding to the request that originated the
	 *         active request chain.
	 */
	public long getOriginatingRequestID();

	/**
	 * @return The querier GUID that originated the active request chain.
	 */
	public String getOriginatingGUID();

	/**
	 * @return The number of hops remaining before the active request expires.
	 */
	public int getTTL();

	/**
	 * This information is needed to ensure that any request chain involves a
	 * coordinated request at most once. Ensuring this invariant makes reasoning
	 * about the functional semantics as well as resource usage easier. Not
	 * ensuring this invariant can result in deadlocks if a coordinated request
	 * spawns another coordinated request for the same name. It is also
	 * nontrivial to otherwise ensure that each request in the chain is executed
	 * exactly once as coordinated requests involve replicated execution; not
	 * ensuring this condition risks an exponential blowup in resource usage
	 * with increasing depth. Ensuring this invariant is sufficient (but not
	 * necessary) to avoid these problems, so it is a design choice favoring
	 * simplicity over flexibility.
	 * 
	 * @return True if any request in this chain was coordinated, false otherwise.
	 */
	public boolean hasBeenCoordinatedOnce();
}
