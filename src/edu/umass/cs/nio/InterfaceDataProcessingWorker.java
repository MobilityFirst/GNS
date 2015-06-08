package edu.umass.cs.nio;

import java.nio.channels.SocketChannel;

/**
 * @author V. Arun
 * 
 *         An interface used by NIOTransport to process incoming byte stream
 *         data. 
 */
public interface InterfaceDataProcessingWorker {
	public abstract void processData(SocketChannel socket, byte[] data,
			int count);
}
