package edu.umass.cs.protocoltask;
/**
@author V. Arun
 * @param <NodeIDType> 
 * @param <EventType> 
 * @param <KeyType> 
 */
public interface ThresholdProtocolEvent<NodeIDType, EventType, KeyType> extends
		ProtocolEvent<EventType, KeyType> {
	/**
	 * @return Sender node ID for this protocol event message.
	 */
	public NodeIDType getSender();
}
