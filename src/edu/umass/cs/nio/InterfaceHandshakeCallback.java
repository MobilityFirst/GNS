package edu.umass.cs.nio;

import java.nio.channels.SelectionKey;

/**
 * @author arun
 *
 */
public interface InterfaceHandshakeCallback {
	/**
	 * @param key
	 */
	public void handshakeComplete(SelectionKey key);
}
