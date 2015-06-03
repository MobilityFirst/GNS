package edu.umass.cs.protocoltask;
/**
@author V. Arun
 */
public interface ThresholdProtocolEvent<NodeIDType, EventType, KeyType> extends
		ProtocolEvent<EventType, KeyType> {
	public NodeIDType getSender();
}
