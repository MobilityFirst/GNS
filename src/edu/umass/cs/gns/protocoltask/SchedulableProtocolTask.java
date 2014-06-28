package edu.umass.cs.gns.protocoltask;

import edu.umass.cs.gns.nio.MessagingTask;

/**
 * @author V. Arun
 */

/*
 * Allows for a separate restart method possibly different from the first start method.
 */
public interface SchedulableProtocolTask<EventType extends Comparable<EventType>, KeyType extends Comparable<KeyType>>
		extends ProtocolTask<EventType, KeyType> {
	public MessagingTask[] restart();

}
