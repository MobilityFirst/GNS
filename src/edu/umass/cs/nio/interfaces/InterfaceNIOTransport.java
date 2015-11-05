/*
 * Copyright (c) 2015 University of Massachusetts
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 * 
 * Initial developer(s): V. Arun
 */
package edu.umass.cs.nio.interfaces;

import java.io.IOException;
import java.net.InetSocketAddress;

import edu.umass.cs.nio.AbstractPacketDemultiplexer;
import edu.umass.cs.nio.SSLDataProcessingWorker;

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
	
	/**
	 * @return Node config used by this NIO transport.
	 */
	public NodeConfig<NodeIDType> getNodeConfig();
	
	/**
	 * @return SSL mode used by this NIO transport.
	 */
	public SSLDataProcessingWorker.SSL_MODES getSSLMode();
	
	/**
	 * @param id
	 * @param msg
	 * @return Number of bytes written.
	 * @throws IOException
	 */
	public int sendToID(NodeIDType id, byte[] msg) throws IOException;

	
	/**
	 * @param isa
	 * @param msg
	 * @return Number of bytes written.
	 * @throws IOException
	 */
	public int sendToAddress(InetSocketAddress isa, byte[] msg)
			throws IOException;

	/**
	 * @param node
	 * @return Whether {@code node} got disconnected.
	 */
	public boolean isDisconnected(NodeIDType node);
}
