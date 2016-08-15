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
}
