package edu.umass.cs.nio;

import java.io.IOException;

import org.json.JSONException;

public interface InterfaceMessenger<NodeIDType, MessageType> extends InterfaceNIOTransport<NodeIDType, MessageType>{
	  public void send(GenericMessagingTask<NodeIDType, ?> mtask) throws IOException, JSONException;
}
