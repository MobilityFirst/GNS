package edu.umass.cs.nio;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
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
	private static final int DEFAULT_PER_CONNECTION_IO_BUFFER_SIZE = 64 * 1024;
	// to handle incoming decrypted data
	private final InterfaceDataProcessingWorker decryptedWorker;

	private final ExecutorService taskWorkers = Executors.newFixedThreadPool(4);

	private ConcurrentHashMap<SelectableChannel, AbstractNIOSSL> sslMap = new ConcurrentHashMap<SelectableChannel, AbstractNIOSSL>();

	// to signal connection handshake completion to transport
	private InterfaceHandshakeCallback callbackTransport = null;

	private final SSLDataProcessingWorker.SSL_MODES mode;

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
	public SSLDataProcessingWorker(InterfaceDataProcessingWorker worker)
			throws NoSuchAlgorithmException, SSLException {
		this.decryptedWorker = worker;
		this.mode = NIOTransport.DEFAULT_SSL_MODE;
	}

	/**
	 * @param worker
	 * @param sslMode
	 * @throws NoSuchAlgorithmException
	 * @throws SSLException
	 */
	public SSLDataProcessingWorker(InterfaceDataProcessingWorker worker,
			SSLDataProcessingWorker.SSL_MODES sslMode)
			throws NoSuchAlgorithmException, SSLException {
		this.decryptedWorker = worker;
		this.mode = sslMode;
	}

	protected SSLDataProcessingWorker setHandshakeCallback(
			InterfaceHandshakeCallback callback) {
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
	}

	// invoke SSL wrap
	protected void wrap(SocketChannel channel, ByteBuffer unencrypted) {
		AbstractNIOSSL nioSSL = this.sslMap.get(channel);
		assert (nioSSL != null);
		log.log(Level.FINEST,
				"{0} wrapping unencrypted data of length {1} bytes from {2} to {3}",
				new Object[] { this, unencrypted.remaining(),
						channel.socket().getLocalSocketAddress(),
						channel.socket().getRemoteSocketAddress() });
		nioSSL.nioSend(unencrypted);
	}

	protected boolean isHandshakeComplete(SocketChannel socketChannel) {
		AbstractNIOSSL nioSSL = this.sslMap.get(socketChannel);
		// socketChannel may be unmapped under exceptions
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
		if (this.mode.equals(SSLDataProcessingWorker.SSL_MODES.MUTUAL_AUTH))
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
							socket	.getRemoteSocketAddress() });
			decryptedWorker.processData((SocketChannel) key.channel(),
					decrypted);
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
					log.log(Level.INFO,
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
	public void processMessage(InetSocketAddress sockAddr, String jsonMsg) {
		if (this.decryptedWorker instanceof InterfaceMessageExtractor)
			((InterfaceMessageExtractor) this.decryptedWorker).processMessage(
					sockAddr, jsonMsg);

	}
}
