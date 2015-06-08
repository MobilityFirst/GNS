package edu.umass.cs.protocoltask;
/**
@author V. Arun
 * @param <NodeIDType> 
 * @param <EventType> 
 * @param <KeyType> 
 */
public interface ThresholdProtocolEvent<NodeIDType, EventType, KeyType> extends
		ProtocolEvent<EventType, KeyType> {
	public NodeIDType getSender();
}
