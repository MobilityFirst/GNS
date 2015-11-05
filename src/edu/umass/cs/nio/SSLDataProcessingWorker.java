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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;

import edu.umass.cs.nio.interfaces.DataProcessingWorker;
import edu.umass.cs.nio.interfaces.HandshakeCallback;
import edu.umass.cs.nio.interfaces.InterfaceMessageExtractor;
import edu.umass.cs.utils.Util;

/**
 * @author arun
 *
 *         This class does SSL wrap/unwrap functions respectively right before
 *         NIO performs an (unencrypted) outgoing network write and right after
 *         NIO receives (encrypted) incoming network reads. It is really not a
 *         "message extractor", just an InterfaceDataProcessingWorker + wrap
 *         functionality, but it implements InterfaceMessageExtractor so that it
 *         is easy for MessageNIOTransport to conduct its functions without
 *         worrying about whether NIOTransport.worker is
 *         InterfaceMessageExtractor or not.
 * 
 *         Nevertheless, this class can be used simply with NIOTransport for
 *         secure byte stream communication as InterfaceMessageExtractor is also
 *         an InterfaceDataProcessingWorker.
 */
public class SSLDataProcessingWorker implements InterfaceMessageExtractor {
	/**
	 * SSL modes supported.
	 */
	public static enum SSL_MODES {
		/**
		 * clear text
		 */
		CLEAR,
		/**
		 * server only authentication (needs a trustStore at clients)
		 */
		SERVER_AUTH,
		/**
		 * server + client authentication
		 */
		MUTUAL_AUTH
	}

	private static final int DEFAULT_PER_CONNECTION_IO_BUFFER_SIZE = 64 * 1024;
	// to handle incoming decrypted data
	private final DataProcessingWorker decryptedWorker;

	private final ExecutorService taskWorkers = Executors.newFixedThreadPool(4);

	private ConcurrentHashMap<SelectableChannel, AbstractNIOSSL> sslMap = new ConcurrentHashMap<SelectableChannel, AbstractNIOSSL>();

	// to signal connection handshake completion to transport
	private HandshakeCallback callbackTransport = null;

	protected final SSLDataProcessingWorker.SSL_MODES sslMode;

	private String myID = null;
	private static final Logger log = NIOTransport.getLogger();

	/**
	 * Wraps an underlying data processing worker by adding an unwrap function
	 * just before delivering it to the underlying worker.
	 * 
	 * @param worker
	 * @throws NoSuchAlgorithmException
	 * @throws SSLException
	 */
	protected SSLDataProcessingWorker(DataProcessingWorker worker)
			throws NoSuchAlgorithmException, SSLException {
		this.decryptedWorker = worker;
		this.sslMode = NIOTransport.DEFAULT_SSL_MODE;
	}

	/**
	 * @param worker
	 * @param sslMode
	 * @throws NoSuchAlgorithmException
	 * @throws SSLException
	 */
	protected SSLDataProcessingWorker(DataProcessingWorker worker,
			SSLDataProcessingWorker.SSL_MODES sslMode)
			throws NoSuchAlgorithmException, SSLException {
		this.decryptedWorker = worker;
		this.sslMode = sslMode;
	}

	protected SSLDataProcessingWorker setHandshakeCallback(
			HandshakeCallback callback) {
		this.callbackTransport = callback;
		return this;
	}

	@Override
	public void processData(SocketChannel channel, ByteBuffer encrypted) {
		AbstractNIOSSL nioSSL = this.sslMap.get(channel);
		assert (nioSSL != null);
		log.log(Level.FINEST,
				"{0} received encrypted data of length {1} bytes on {2} from {3}",
				new Object[] { this, encrypted.remaining(),
						channel.socket().getLocalSocketAddress(),
						channel.socket().getRemoteSocketAddress() });
		// unwrap SSL
		nioSSL.notifyReceived(encrypted);
		assert (encrypted.remaining() == 0); // else buffer overflow exception
	}

	// invoke SSL wrap
	protected int wrap(SocketChannel channel, ByteBuffer unencrypted) {
		AbstractNIOSSL nioSSL = this.sslMap.get(channel);
		assert (nioSSL != null);
		log.log(Level.FINEST,
				"{0} wrapping unencrypted data of length {1} bytes from {2} to {3}",
				new Object[] { this, unencrypted.remaining(),
						channel.socket().getLocalSocketAddress(),
						channel.socket().getRemoteSocketAddress() });
		int originalSize = unencrypted.remaining();
		try {
			nioSSL.nioSend(unencrypted);
		} catch (BufferOverflowException | IllegalStateException e) {
			// do nothing, sender will automatically slow down
		}
		return originalSize - unencrypted.remaining();
	}

	protected boolean isHandshakeComplete(SocketChannel socketChannel) {
		AbstractNIOSSL nioSSL = this.sslMap.get(socketChannel);
		// socketChannel may be unmapped yet or under exception
		return nioSSL != null ? ((NonBlockingSSLImpl) nioSSL)
				.isHandshakeComplete() : false;
	}

	protected boolean register(SelectionKey key, boolean isClient)
			throws IOException {
		assert (!this.sslMap.containsKey(key.channel()));
		SSLEngine engine;
		try {
			engine = SSLContext.getDefault().createSSLEngine();
		} catch (NoSuchAlgorithmException e) {
			throw new IOException(e.getMessage());
		}
		engine.setUseClientMode(isClient);
		if (this.sslMode.equals(SSLDataProcessingWorker.SSL_MODES.MUTUAL_AUTH))
			engine.setNeedClientAuth(true);
		engine.beginHandshake();
		log.log(Level.INFO,
				"{0} registered {1} socket channel {2}",
				new Object[] { this, (isClient ? "client" : "server"),
						key.channel() });
		this.sslMap.put(key.channel(), new NonBlockingSSLImpl(key, engine,
				DEFAULT_PER_CONNECTION_IO_BUFFER_SIZE, this.taskWorkers));
		return true;
	}
	
	protected void poke() {
		for(AbstractNIOSSL nioSSL : this.sslMap.values()) {
			nioSSL.poke();
		}
	}

	// remove entry from sslMap, no need to check containsKey
	private void cleanup(SelectionKey key) {
		SocketChannel socketChannel = (SocketChannel) key.channel();
		NIOTransport.cleanup(key);
		if (socketChannel != null)
			this.sslMap.remove(socketChannel);
	}

	// SSL NIO implementation
	class NonBlockingSSLImpl extends AbstractNIOSSL {
		final SSLEngine engine;
		private boolean handshakeComplete = false;

		NonBlockingSSLImpl(SelectionKey key, SSLEngine engine,
				int ioBufferSize, Executor taskWorkers) {
			super(key, engine, ioBufferSize, taskWorkers);
			this.engine = engine;
		}

		// inbound decrypted data is simply handed over to worker
		@Override
		public void onInboundData(ByteBuffer decrypted) {
			Socket socket = ((SocketChannel) (key.channel())).socket();
			log.log(Level.FINEST,
					"{0} received decrypted data of length {1} bytes on {2} from {3}",
					new Object[] { this, decrypted.remaining(),
							socket.getLocalSocketAddress(),
							socket.getRemoteSocketAddress() });
			// SSLDataProcessingWorker.this.decryptedWorker.processData((SocketChannel)key.channel(),
			// decrypted);
			SSLDataProcessingWorker.this.extractMessages(key, decrypted);
		}

		@Override
		public void onHandshakeFailure(Exception cause) {
			cause.printStackTrace();
			log.log(Level.WARNING,
					"{0} encountered SSL handshake failure; cleaning up channel {1}",
					new Object[] { this, key.channel() });
			// should only be invoked by selection thread
			cleanup(key);
		}

		@Override
		public void onHandshakeSuccess() {
			this.setHandshakeComplete();
			log.log(Level.INFO,
					"{0} conducted successful SSL handshake for channel {1}",
					new Object[] { this, key.channel() });
		}

		@Override
		public void onClosed() {
			log.log(Level.INFO, "{0} cleaning up closed SSL channel {1}",
					new Object[] { this, key.channel() });
			cleanup(key);
		}

		@Override
		public void onOutboundData(ByteBuffer encrypted) {
			SocketChannel channel = ((SocketChannel) key.channel());
			try {
				log.log(Level.FINEST,
						"{0} sending encrypted data of length {1} bytes from {2} to {3}",
						new Object[] { this, encrypted.remaining(),
								channel.socket().getLocalSocketAddress(),
								channel.socket().getRemoteSocketAddress() });
				/*
				 * The assertion is true because we initialized key in the
				 * parent constructor. This method is the only reason we need
				 * the key in the parent, otherwise AbstractNIOSSL is just an
				 * SSL template and has nothing to do with NIO.
				 */
				assert (key != null);
				int totalLength = encrypted.remaining();
				// hack! try few times, but can't really wait here.
				for (int attempts = 0; attempts < 3 && encrypted.hasRemaining(); attempts++)
					channel.write(encrypted);

				// not a showstopper if we don't absolutely complete the write
				if (encrypted.hasRemaining())
					log.log(Level.FINE,
							"{0} failed to bulk-write {1} bytes despite multiple attempts ({2} bytes left unsent)",
							new Object[] { this, totalLength,
									encrypted.remaining(), });

			} catch (IOException | IllegalStateException exc) {
				// need to cleanup as we are screwed
				log.severe(this
						+ " ran into exception or illegal state while writing outbound data; closing channel");
				cleanup(key);
				throw new IllegalStateException(exc);
			}
		}

		public String toString() {
			return this.getClass().getSimpleName() + getMyID();
		}

		private synchronized boolean isHandshakeComplete() {
			return this.handshakeComplete;
		}

		private synchronized void setHandshakeComplete() {
			this.handshakeComplete = true;
			callbackTransport.handshakeComplete(this.key);
		}
	}

	public String toString() {
		return this.getClass().getSimpleName() + getMyID();
	}

	/**
	 * @return My ID, primarily for logging purposes.
	 */
	public String getMyID() {
		return this.myID;
	}

	protected void setMyID(String id) {
		this.myID = id;
	}

	public void stop() {
		this.taskWorkers.shutdownNow();
		if (this.decryptedWorker instanceof InterfaceMessageExtractor)
			((InterfaceMessageExtractor) this.decryptedWorker).stop();
	}

	@Override
	public void addPacketDemultiplexer(AbstractPacketDemultiplexer<?> pd) {
		if (this.decryptedWorker instanceof InterfaceMessageExtractor)
			((InterfaceMessageExtractor) this.decryptedWorker)
					.addPacketDemultiplexer(pd);
	}

	@Override
	public void processLocalMessage(InetSocketAddress sockAddr, String msg) {
		if (this.decryptedWorker instanceof InterfaceMessageExtractor)
			((InterfaceMessageExtractor) this.decryptedWorker).processLocalMessage(
					sockAddr, msg);

	}

	private void extractMessages(SelectionKey key, ByteBuffer incoming) {
		ByteBuffer bbuf = null;
		try {
			while (incoming.hasRemaining()
					&& (bbuf = this.extractMessage(key, incoming)) != null) {
				this.decryptedWorker.processData((SocketChannel) key.channel(),
						bbuf);
			}
		} catch (IOException e) {
			log.severe(this + e.getMessage() + " on channel " + key.channel());
			e.printStackTrace();
			// incoming is emptied out; what else to do here?
		}
	}

	// extracts a single message
	private ByteBuffer extractMessage(SelectionKey key, ByteBuffer incoming)
			throws IOException {
		NIOTransport.AlternatingByteBuffer abbuf = (NIOTransport.AlternatingByteBuffer) key
				.attachment();
		assert (abbuf != null);
		if (abbuf.headerBuf.remaining() > 0) {
			Util.put(abbuf.headerBuf, incoming);
			if (abbuf.headerBuf.remaining() == 0) {
				abbuf.bodyBuf = ByteBuffer.allocate(NIOTransport
						.getPayloadLength((ByteBuffer) abbuf.headerBuf.flip()));
				assert (abbuf.bodyBuf != null && abbuf.bodyBuf.capacity() > 0);
			}
		}
		ByteBuffer retval = null;
		if (abbuf.bodyBuf != null) {
			Util.put(abbuf.bodyBuf, incoming);
			if (abbuf.bodyBuf.remaining() == 0) {
				retval = (ByteBuffer) abbuf.bodyBuf.flip();
				abbuf.clear(); // prepare for next read
			}
		}
		return retval;
	}

	@Override
	public void demultiplexMessage(Object message) {
		this.decryptedWorker.demultiplexMessage(message);
	}
}
