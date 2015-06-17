package edu.umass.cs.nio;

import java.nio.ByteBuffer;
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
	 * @param incoming The bytes received.
	 */
	public abstract void processData(SocketChannel socket, ByteBuffer incoming);
	
	
}
