package edu.umass.cs.gnsserver.activecode.prototype.interfaces;

import java.io.IOException;


public interface Channel {
	

	public void sendMessage(Message msg) throws IOException;
	

	public Message receiveMessage() throws IOException;
	

	public void close();
}
