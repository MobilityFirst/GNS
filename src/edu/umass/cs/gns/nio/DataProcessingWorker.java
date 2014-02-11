package edu.umass.cs.gns.nio;

import java.nio.channels.SocketChannel;

/**
@author V. Arun
 */
/* An interface used by NIOTransport to process incoming
 * byte stream data.
 */
public interface DataProcessingWorker {
	public void processData(SocketChannel socket, byte[] data, int count);
}
