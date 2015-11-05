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
package edu.umass.cs.nio;

import org.json.JSONObject;

import edu.umass.cs.nio.interfaces.NodeConfig;

import java.io.IOException;

/**
 * @author V. Arun
 * @param <NodeIDType>
 * 
 *            Consider using JSONMessenger any place you want to use this class.
 *            The former subsumes this class and also has support for
 *            MessagingTask objects.
 * 
 *            This class exists primarily as a wrapper around NIOTransport to
 *            support JSON messages. NIOTransport is for general-purpose NIO
 *            byte stream communication between numbered nodes as specified by
 *            the NodeConfig interface and a data processing worker as specified
 *            by the DataProcessingWorker interface that handles byte arrays.
 *            This class provides the abstraction of JSON messages and a
 *            corresponding PacketDemultiplexer that handles JSON messages
 *            instead of raw bytes.
 * 
 *            Currently, there is no way to directly use NIOTransport other than
 *            with MessageNIOTransport or JSONNIOTransport.
 * 
 */
public class JSONNIOTransport<NodeIDType> extends
		MessageNIOTransport<NodeIDType, JSONObject>  {

	/**
	 * Initiates transporter with id and nodeConfig.
	 * 
	 * @param id
	 *            My node ID.
	 * @param nodeConfig
	 *            A map from all nodes' IDs to their respective socket
	 *            addresses.
	 * @throws IOException
	 */
	public JSONNIOTransport(NodeIDType id,
			NodeConfig<NodeIDType> nodeConfig) throws IOException {
		// Note: Default demultiplexer will not do any useful demultiplexing
		super(id, nodeConfig);
	}

	/**
	 * @param id
	 * @param nodeConfig
	 * @param sslMode
	 * @throws IOException
	 */
	public JSONNIOTransport(NodeIDType id,
			NodeConfig<NodeIDType> nodeConfig,
			SSLDataProcessingWorker.SSL_MODES sslMode) throws IOException {
		// Note: Default multiplexer will not do any useful demultiplexing
		super(id, nodeConfig, sslMode);
	}

	/**
	 * 
	 * @param id
	 *            My node ID.
	 * @param nodeConfig
	 *            A map from all nodes' IDs to their respective socket
	 *            addresses.
	 * @param pd
	 *            The packet demultiplexer to handle received messages.
	 * @param start
	 *            If a server thread must be automatically started upon
	 *            construction. If false, the caller must explicitly invoke (new
	 *            Thread(JSONNIOTransport)).start() to start the server.
	 * @throws IOException
	 */
	public JSONNIOTransport(NodeIDType id,
			NodeConfig<NodeIDType> nodeConfig,
			AbstractPacketDemultiplexer<?> pd, boolean start)
			throws IOException {
		super(id, nodeConfig, pd, start);
	}

	/**
	 * @param id
	 * @param nodeConfig
	 * @param pd
	 * @param sslMode
	 * @throws IOException
	 */
	public JSONNIOTransport(NodeIDType id,
			NodeConfig<NodeIDType> nodeConfig,
			AbstractPacketDemultiplexer<?> pd, SSLDataProcessingWorker.SSL_MODES sslMode)
			throws IOException {
		super(id, nodeConfig, pd, true, sslMode);
	}

}
