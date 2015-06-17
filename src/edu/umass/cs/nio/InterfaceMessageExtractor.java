package edu.umass.cs.nio;

import java.net.InetSocketAddress;

/**
 * @author arun
 *
 * A common interface really just to make it easy to work interchangeably
 * with MessageExtractor and SSLDataProcessingWorker.
 */
public interface InterfaceMessageExtractor extends
		InterfaceDataProcessingWorker {

	/**
	 * @param pd
	 */
	public void addPacketDemultiplexer(AbstractPacketDemultiplexer<?> pd);

	/**
	 * 
	 */
	public void stop();

	/**
	 * @param sockAddr
	 * @param jsonMsg
	 */
	public void processMessage(InetSocketAddress sockAddr, String jsonMsg);

}
