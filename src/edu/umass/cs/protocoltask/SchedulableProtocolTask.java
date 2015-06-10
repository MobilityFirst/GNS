package edu.umass.cs.protocoltask;

import edu.umass.cs.nio.GenericMessagingTask;

/**
 * @author V. Arun
 * @param <NodeIDType>
 * @param <EventType>
 * @param <KeyType>
 * 
 *            Allows for a separate restart method possibly different from the
 *            first start method.
 */
public interface SchedulableProtocolTask<NodeIDType, EventType, KeyType>
		extends ProtocolTask<NodeIDType, EventType, KeyType> {
	/**
	 * @return The messaging task upon each restart.
	 */
	public GenericMessagingTask<NodeIDType, ?>[] restart();

	/**
	 * @return The restart period.
	 */
	public long getPeriod();
}
