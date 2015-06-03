package edu.umass.cs.protocoltask;

import edu.umass.cs.nio.GenericMessagingTask;

/**
 * @author V. Arun
 */

/*
 * Allows for a separate restart method possibly different from the first start method.
 */
public interface SchedulableProtocolTask<NodeIDType, EventType, KeyType>
		extends ProtocolTask<NodeIDType, EventType, KeyType> {
	public GenericMessagingTask<NodeIDType,?>[] restart();
	public long getPeriod();
}
