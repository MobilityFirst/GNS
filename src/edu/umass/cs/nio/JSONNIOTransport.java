package edu.umass.cs.nio;

import org.json.JSONObject;

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
 *            This class also supports delay emulation and short-circuiting
 *            local sends by directly sending it to the packet demultiplexer.
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
			InterfaceNodeConfig<NodeIDType> nodeConfig) throws IOException {
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
			InterfaceNodeConfig<NodeIDType> nodeConfig,
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
			InterfaceNodeConfig<NodeIDType> nodeConfig,
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
			InterfaceNodeConfig<NodeIDType> nodeConfig,
			AbstractPacketDemultiplexer<?> pd, SSLDataProcessingWorker.SSL_MODES sslMode)
			throws IOException {
		super(id, nodeConfig, pd, true, sslMode);
	}

}
