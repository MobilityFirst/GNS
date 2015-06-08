package edu.umass.cs.nio;

import java.io.IOException;
import java.net.InetSocketAddress;

public interface InterfaceNIOTransport<NodeIDType,MessageType> {
	public int sendToID(NodeIDType id, MessageType msg) throws IOException;

	public int sendToAddress(InetSocketAddress isa, MessageType msg)
			throws IOException;

	public void addPacketDemultiplexer(AbstractPacketDemultiplexer<?> pd);

	public NodeIDType getMyID();

	public void stop();
}
