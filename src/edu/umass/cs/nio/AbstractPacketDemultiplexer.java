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

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import org.json.JSONException;

import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.nio.interfaces.PacketDemultiplexer;
import edu.umass.cs.nio.nioutils.NIOHeader;

import java.util.logging.Level;

/**
 * @author V. Arun
 * @param <MessageType>
 *            Indicates the generic type of messages processed by this
 *            demultiplexer.
 */
public abstract class AbstractPacketDemultiplexer<MessageType> implements
		PacketDemultiplexer<MessageType> {

	/**
	 * The default thread pool size.
	 * 
	 * FIXME: Unclear what a good value is.
	 */
	public static final int DEFAULT_THREAD_POOL_SIZE = 5;
	private static int threadPoolSize = DEFAULT_THREAD_POOL_SIZE;

	private final int myThreadPoolSize;

	/**
	 * @param threadPoolSize
	 *            The threadPoolSize parameter determines the level of
	 *            parallelism in NIO packet processing. Setting it to 0 means no
	 *            parallelism, i.e., each received packet will be fully
	 *            processed by NIO before it does anything else. So if the
	 *            {@link #handleMessage(Object)} implementation blocks, NIO may
	 *            deadlock.
	 * 
	 *            Setting the threadPoolSize higher allows
	 *            {@link #handleMessage(Object)} to include a limited number of
	 *            blocking operations, but NIO can still deadlock if the number
	 *            of pending {@link #handleMessage(Object)} invocations at a
	 *            node exceeds the thread pool size in this class. Thus, it is
	 *            best for {@link #handleMessage(Object)} methods to only
	 *            perform operations that return quickly; if longer packet
	 *            processing is needed, {@link #handleMessage(Object)} must
	 *            accordingly spawn its own helper threads. It is a bad idea,
	 *            for example, for {@link #handleMessage(Object)} to itself send
	 *            a request over the network and wait until it gets back a
	 *            response.
	 */
	public static synchronized void setThreadPoolSize(int threadPoolSize) {
		AbstractPacketDemultiplexer.threadPoolSize = threadPoolSize;
	}

	protected static synchronized int getThreadPoolSize() {
		return threadPoolSize;
	}

	private final ScheduledExecutorService executor;
	private final HashMap<Integer, PacketDemultiplexer<MessageType>> demuxMap = new HashMap<Integer, PacketDemultiplexer<MessageType>>();
	private final Set<Integer> orderPreservingTypes = new HashSet<Integer>();
	protected static final Logger log = NIOTransport.getLogger();

	abstract protected Integer getPacketType(MessageType message);

	abstract protected MessageType getMessage(String message);

	abstract protected MessageType processHeader(String message, NIOHeader header);

	private static final String DEFAULT_THREAD_NAME = AbstractPacketDemultiplexer.class
			.getSimpleName();
	private String threadName = DEFAULT_THREAD_NAME;

	/**
	 * 
	 * @param threadPoolSize
	 *            Refer documentation for {@link #setThreadPoolSize(int)
	 *            setThreadPoolsize(int)}.
	 */
	public AbstractPacketDemultiplexer(int threadPoolSize) {
		this.executor = Executors.newScheduledThreadPool(threadPoolSize,
				new ThreadFactory() {
					@Override
					public Thread newThread(Runnable r) {
						Thread thread = Executors.defaultThreadFactory()
								.newThread(r);
						thread.setName(threadName);
						return thread;
					}
				});
		this.myThreadPoolSize = threadPoolSize;
	}

	/**
	 * 
	 */
	public AbstractPacketDemultiplexer() {
		this(getThreadPoolSize());
	}

	protected void setThreadName(String name) {
		this.threadName = DEFAULT_THREAD_NAME + name;
	}

	// This method will be invoked by NIO
	protected boolean handleMessageSuper(String msg, NIOHeader header)
			throws JSONException {
		MessageType message = null;
		try {
			message = processHeader(msg, header);
		} catch(Exception e) {e.printStackTrace(); return false;}
		Integer type = getPacketType(message);
		if (type == null || !this.demuxMap.containsKey(type)) {
			/*
			 * It is natural for some demultiplexers to not handle some packet
			 * types, so it is not a "bad" thing that requires a warning log.
			 */
			log.log(Level.FINE, "Ignoring unknown packet type: {0}", type);
			return false;
		}
		Tasker tasker = new Tasker(message, this.demuxMap.get(type));
		if (this.myThreadPoolSize == 0 || isOrderPreserving(message))
			// task better be lightning quick
			tasker.run();
		else
			try {
				// task should still be non-blocking
				executor.schedule(tasker, 0, TimeUnit.MILLISECONDS);
			} catch (RejectedExecutionException ree) {
				if (!executor.isShutdown())
					ree.printStackTrace();
				return false;
			}
		/*
		 * Note: executor.submit() consistently yields poorer performance than
		 * scheduling at 0 as above even though they are equivalent. Probably
		 * garbage collection or heap optimization issues.
		 */
		return true;
	}
	protected boolean loopback(Object obj) {
		if(!this.matchesType(obj)) return false;
		@SuppressWarnings("unchecked") // checked above
		MessageType message = (MessageType)obj;
		Integer type = getPacketType(message);
		if (type == null || !this.demuxMap.containsKey(type)) 
			this.demuxMap.get(type).handleMessage(message);
		return true;
	}
	abstract protected boolean matchesType(Object message);
	
	/**
	 * @param msg
	 * @return True if message order is preserved.
	 */
	public boolean isOrderPreserving(MessageType msg) {
		return false;
	}

	/**
	 * Registers {@code type} with {@code this}.
	 * 
	 * @param type
	 */
	public void register(IntegerPacketType type) {
		register(type, this);
	}

	/**
	 * Registers {@code type} with {@code pd}.
	 * 
	 * @param type
	 * @param pd
	 */
	public void register(IntegerPacketType type,
			PacketDemultiplexer<MessageType> pd) {
		if (pd == null)
			return;
		if(this.demuxMap.containsKey(type)) throw new RuntimeException("re-regitering type " +type);
		log.finest("Registering type " + type.getInt() + " with " + pd);
		this.demuxMap.put(type.getInt(), pd);
	}
	
	/**
	 * @return True if congested
	 */
	protected boolean isCongested(NIOHeader header) {
		return false;
	}

	/**
	 * Registers {@code types} with {@code pd};
	 * 
	 * @param types
	 * @param pd
	 */
	public void register(Set<IntegerPacketType> types,
			PacketDemultiplexer<MessageType> pd) {
		log.info("Registering types " + types + " for " + pd);
		for (IntegerPacketType type : types)
			register(type, pd);
	}

	/**
	 * Registers {@code types} with {@code this}.
	 * 
	 * @param types
	 */
	public void register(Set<IntegerPacketType> types) {
		this.register(types, this);
	}

	/**
	 * Registers {@code types} with {@code this}.
	 * 
	 * @param types
	 * @param pd
	 */
	public void register(IntegerPacketType[] types,
			PacketDemultiplexer<MessageType> pd) {
		log.info("Registering types "
				+ (new HashSet<Object>(Arrays.asList(types))) + " for " + pd);
		for (Object type : types)
			register((IntegerPacketType) type, pd);
	}

	/**
	 * Registers {@code types} with {@code this}.
	 * 
	 * @param types
	 */
	public void register(IntegerPacketType[] types) {
		log.info("Registering types "
				+ (new HashSet<Object>(Arrays.asList(types))) + " for " + this);
		for (Object type : types)
			register((IntegerPacketType) type, this);
	}

	/**
	 * @param type
	 */
	public void registerOrderPreserving(IntegerPacketType type) {
		register(type);
		this.orderPreservingTypes.add(type.getInt());
	}


	/**
	 * Any created instance of AbstractPacketDemultiplexer or its inheritors
	 * must be cleanly closed by invoking this stop method.
	 */
	public void stop() {
		this.executor.shutdown();
	}

	// helper task for handleMessageSuper
	protected class Tasker implements Runnable {

		private final MessageType json;
		private final PacketDemultiplexer<MessageType> pd;

		Tasker(MessageType json, PacketDemultiplexer<MessageType> pd) {
			this.json = json;
			this.pd = pd;
		}

		public void run() {
			try {
				pd.handleMessage(this.json);
			} catch (RejectedExecutionException ree) {
				if (!executor.isShutdown())
					ree.printStackTrace(); 
			} catch (Exception e) {
				e.printStackTrace(); // unless printed task will die silently
			} catch (Error e) {
				e.printStackTrace();
			}
		}
	}

}
