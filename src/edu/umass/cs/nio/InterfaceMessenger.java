package edu.umass.cs.nio;

import java.io.IOException;

import org.json.JSONException;

/**
 * @author arun
 *
 * @param <NodeIDType>
 * @param <MessageType>
 */
public interface InterfaceMessenger<NodeIDType, MessageType> extends InterfaceNIOTransport<NodeIDType, MessageType>{
	  /**
	 * @param mtask
	 * @throws IOException
	 * @throws JSONException
	 */
	public void send(GenericMessagingTask<NodeIDType, ?> mtask) throws IOException, JSONException;
}
