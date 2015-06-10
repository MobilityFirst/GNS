package edu.umass.cs.nio;

import java.nio.channels.SocketChannel;

/**
 * @author V. Arun
 * 
 *         An interface used by NIOTransport to process incoming byte stream
 *         data. 
 */
public interface InterfaceDataProcessingWorker {
	/**
	 * @param socket The socket channel on which the bytes were received.
	 * @param data The bytes received.
	 * @param count The number of bytes received.
	 */
	public abstract void processData(SocketChannel socket, byte[] data,
			int count);
}
