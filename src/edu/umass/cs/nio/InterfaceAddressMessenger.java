package edu.umass.cs.nio;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * @author arun
 *
 * @param <MessageType>
 */
public interface InterfaceAddressMessenger<MessageType> {
	/**
	 * @param isa
	 * @param msg
	 * @return The number of characters (not bytes) written.
	 * @throws IOException
	 */
	public int sendToAddress(InetSocketAddress isa, MessageType msg)
			throws IOException;
}
