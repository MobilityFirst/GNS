package edu.umass.cs.protocoltask;

import edu.umass.cs.utils.Keyable;

/**
 * @author V. Arun
 * @param <EventType> 
 * @param <KeyType> 
 * 
 * EventType is a high-level, human-readable name of the protocol task.
 * KeyType is generally an identifier that may be dynamically assigned
 * and may not have any meaning, e.g., a DNS request identifier that
 * is later used to match the response against the request. Here, "DNS"
 * is the String EventType while the request identifier is the Integer
 * KeyType.
 */
public interface ProtocolEvent<EventType, KeyType>
		extends Keyable<KeyType> {
	/**
	 * @return Type of event.
	 */
	public EventType getType();

	/**
	 * @return An object whose toString() method must serialize this event to a String.
	 */
	public Object getMessage();

	/**
	 * Sets the protocol key.
	 * 
	 * @param key
	 */
	public void setKey(KeyType key); 
}
