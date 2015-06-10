package edu.umass.cs.nio;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * @author arun
 *
 * @param <NodeIDType>
 * @param <MessageType>
 */
public interface InterfaceNIOTransport<NodeIDType,MessageType> {
	/**
	 * @param id The destination node ID.
	 * @param msg The message to be sent.
	 * @return Number of characters written. The number of characters is calculated 
	 * by converting MessageType to a String. If MessageType is a byte[], then 
	 * ISO-8859-1 encoding is used.
	 * 
	 * @throws IOException
	 */
	public int sendToID(NodeIDType id, MessageType msg) throws IOException;

	/**
	 * @param isa
	 * @param msg
	 * @return Number of characters written. The number of characters is calculated 
	 * by converting MessageType to a String. If MessageType is a byte[], then 
	 * ISO-8859-1 encoding is used.
	 * @throws IOException
	 */
	public int sendToAddress(InetSocketAddress isa, MessageType msg)
			throws IOException;

	/**
	 * @param pd The demultiplexer to be chained at the end of the existing list.
	 */
	public void addPacketDemultiplexer(AbstractPacketDemultiplexer<?> pd);

	/**
	 * @return My node ID.
	 */
	public NodeIDType getMyID();

	/**
	 * Needs to be called to close NIO gracefully.
	 */
	public void stop();
}
