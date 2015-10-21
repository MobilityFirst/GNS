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

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.util.concurrent.Executor;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLException;

import edu.umass.cs.utils.MyLogger;

/**
 * @author arun
 *
 */
public abstract class AbstractNIOSSL implements Runnable {
	private final static int MAX_BUFFER_SIZE = 2048 * 1024;
	private final static int MAX_DST_BUFFER_SIZE = 2*MAX_BUFFER_SIZE;

	// final
	ByteBuffer wrapSrc, unwrapSrc;
	// final
	ByteBuffer wrapDst, unwrapDst;

	final SSLEngine engine;
	final Executor taskWorkers;

	final SelectionKey key;
	private static final Logger log = NIOTransport.getLogger();

	/**
	 * @param key
	 * @param engine
	 * @param bufferSize
	 * @param taskWorkers
	 */
	public AbstractNIOSSL(SelectionKey key, SSLEngine engine, int bufferSize,
			Executor taskWorkers) {
		this.wrapSrc = ByteBuffer.allocateDirect(bufferSize);
		this.wrapDst = ByteBuffer.allocateDirect(bufferSize);
		this.unwrapSrc = ByteBuffer.allocateDirect(bufferSize);
		this.unwrapDst = ByteBuffer.allocateDirect(bufferSize);
		// this.unwrapSrc.limit(0);
		this.engine = engine;
		this.taskWorkers = taskWorkers;
		this.key = key;

		run();
	}

	/**
	 * @param decrypted
	 */
	public abstract void onInboundData(ByteBuffer decrypted);

	/**
	 * @param encrypted
	 */
	public abstract void onOutboundData(ByteBuffer encrypted);

	/**
	 * @param cause
	 */
	public abstract void onHandshakeFailure(Exception cause);

	/**
	 * 
	 */
	public abstract void onHandshakeSuccess();

	/**
	 * 
	 */
	public abstract void onClosed();

	/**
	 * To wrap encrypt-and-send outgoing data.
	 * 
	 * @param unencrypted
	 */
	public synchronized void nioSend(final ByteBuffer unencrypted) {
		try {
			wrapSrc.put(unencrypted);
		} catch (BufferOverflowException boe) {
			wrapSrc = getBiggerBuffer(wrapSrc, unencrypted);
			log.log(Level.INFO, MyLogger.FORMAT[1], new Object[] {
					"Increased wrapSrc buffer size to ", wrapSrc.capacity() });
		}
		run();
	}

	/**
	 * To unwrap (decrypt) data received from the network.
	 * 
	 * @param encrypted
	 */
	public synchronized void notifyReceived(ByteBuffer encrypted) {
		try {
			unwrapSrc.put(encrypted);
		} catch (BufferOverflowException boe) {
			// try increasing buffer
			unwrapSrc = getBiggerBuffer(unwrapSrc, encrypted);
			log.log(Level.FINE, MyLogger.FORMAT[1],
					new Object[] { "Increased unwrapSrc buffer size to ",
							unwrapSrc.capacity() });

		}
		run();
	}

	// trying to put buf2 into buf1
	private ByteBuffer getBiggerBuffer(ByteBuffer buf1, ByteBuffer buf2) {
		int biggerSize = buf1.position() + buf2.remaining();
		if (biggerSize > MAX_BUFFER_SIZE) {
			log.warning("Maximum allowed buffer size limit reached");
			throw new BufferOverflowException();
		}
		ByteBuffer biggerBuf = ByteBuffer.allocate(biggerSize);
		buf1.flip();
		biggerBuf.put(buf1);
		biggerBuf.put(buf2);
		buf1.compact(); // not really needed
		return biggerBuf;

	}

	public synchronized void run() {
		// executes non-blocking tasks on the IO-Worker
		while (this.step())
			continue;
	}

	private boolean step() {
		switch (engine.getHandshakeStatus()) {
		case NOT_HANDSHAKING:
			boolean anything = false;
			{
				if (wrapSrc.position() > 0)
					anything |= this.wrap();
				if (unwrapSrc.position() > 0)
					anything |= this.unwrap();
			}
			return anything;

		case NEED_WRAP:

			if (!this.wrap())
				return false;
			break;

		case NEED_UNWRAP:
			if (!this.unwrap())
				return false;
			break;

		case NEED_TASK:
			final Runnable sslTask = engine.getDelegatedTask();
			if (sslTask == null)
				return false;
			Runnable wrappedTask = new Runnable() {
				@Override
				public void run() {
					try {
						log.log(Level.FINE, MyLogger.FORMAT[1], new Object[] {
								"async SSL task: ", sslTask });
						long t0 = System.nanoTime();
						sslTask.run();
						log.log(Level.FINE, MyLogger.FORMAT[2], new Object[] {
								"async SSL task took: ",
								(System.nanoTime() - t0) / 1000000, "ms" });

						// continue handling I/O
						AbstractNIOSSL.this.run();
					} catch(Exception e) {
						e.printStackTrace();
					}
				}
			};
			taskWorkers.execute(wrappedTask);
			return false;
			/*
			 * We could also run delegated tasks in blocking mode and return
			 * true here, but that would probably defeat the purpose of NIO as
			 * some delegated tasks may require remote calls.
			 */

		case FINISHED:
			throw new IllegalStateException("FINISHED");
		}

		return true;
	}

	private synchronized boolean wrap() {
		SSLEngineResult wrapResult;

		try {
			wrapSrc.flip();
			wrapResult = engine.wrap(wrapSrc, wrapDst);
			wrapSrc.compact();
		} catch (SSLException exc) {
			this.onHandshakeFailure(exc);
			return false;
		}

		switch (wrapResult.getStatus()) {
		case OK:
			if (wrapDst.position() > 0) {
				wrapDst.flip();
				this.onOutboundData(wrapDst);
				wrapDst.compact();
			}
			break;

		case BUFFER_UNDERFLOW:
			log.fine("wrap BUFFER_UNDERFLOW");
			// try again later
			break;

		case BUFFER_OVERFLOW:
			log.warning("wrap BUFFER_OVERFLOW: Wrapped data is coming faster than the network can send it out.");
			// Could attempt to drain the dst buffer of any already obtained
			// data, but we'll just increase it to the size needed.
			int biggerSize = engine.getSession().getApplicationBufferSize()
					+ wrapDst.capacity();
			if (biggerSize > MAX_DST_BUFFER_SIZE)
				throw new IllegalStateException("failed to wrap");
			ByteBuffer b = ByteBuffer.allocate(biggerSize);
			wrapDst.flip();
			b.put(wrapDst);
			wrapDst = b;
			log.log(Level.FINE, MyLogger.FORMAT[0], new Object[] {
					"Increased wrapDst buffer size to " + wrapDst.capacity() });
			// retry the operation.
			break;

		case CLOSED:
			this.onClosed();
			return false;
		}

		// inform server of handshake success
		switch (wrapResult.getHandshakeStatus()) {
		case FINISHED:
			this.onHandshakeSuccess();
			return false;
		default:
			break;
		}

		return true;
	}

	private synchronized boolean unwrap() {
		SSLEngineResult unwrapResult;

		try {
			unwrapSrc.flip();
			unwrapResult = engine.unwrap(unwrapSrc, unwrapDst);
			unwrapSrc.compact();
		} catch (SSLException exc) {
			this.onHandshakeFailure(exc);
			return false;
		}

		switch (unwrapResult.getStatus()) {
		case OK:
			if (unwrapDst.position() > 0) {
				unwrapDst.flip();
				this.onInboundData(unwrapDst);
				unwrapDst.compact();
			}
			break;

		case CLOSED:
			this.onClosed();
			return false;

		case BUFFER_OVERFLOW:
			log.info("unwrap BUFFER_OVERFLOW: Network data is coming in faster than can be unwrapped");
			// Could attempt to drain the dst buffer of any already obtained
			// data, but we'll just increase it to the size needed.
			int biggerSize = engine.getSession().getApplicationBufferSize()
					+ unwrapDst.capacity();
			if (biggerSize > MAX_BUFFER_SIZE)
				throw new IllegalStateException("failed to unwrap");
			ByteBuffer b = ByteBuffer.allocate(biggerSize);
			unwrapDst.flip();
			b.put(unwrapDst);
			unwrapDst = b;
			log.log(Level.FINE, MyLogger.FORMAT[1], new Object[] {
					"Increased unwrapDst buffer size to ", unwrapDst.capacity() });
			// retry the operation.
			break;

		case BUFFER_UNDERFLOW:
			log.fine("unwrap BUFFER_UNDERFLOW");
			return false;
		}

		// inform client of handshake success
		switch (unwrapResult.getHandshakeStatus()) {
		case FINISHED:
			this.onHandshakeSuccess();
			return false;
		default:
			break;
		}

		return true;
	}

	/**
	 * To flush stuck data if any.
	 */
	public void poke() {
		run();
	}
}