package edu.umass.cs.nio;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import org.json.JSONException;

import java.util.logging.Level;

/**
 * @author V. Arun
 * @param <MessageType> Indicates the generic type of messages processed by this demultiplexer.
 */
public abstract class AbstractPacketDemultiplexer<MessageType> implements
		InterfacePacketDemultiplexer<MessageType> {

	// FIXME: Unclear what a good default value is.
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
	public static synchronized void setThreadPoolSize(int threadPoolSize) {AbstractPacketDemultiplexer.threadPoolSize = threadPoolSize;}
	public static synchronized int getThreadPoolSize() {return threadPoolSize;}
	
	private final ScheduledExecutorService executor;
	private final HashMap<Integer, InterfacePacketDemultiplexer<MessageType>> demuxMap = new HashMap<Integer, InterfacePacketDemultiplexer<MessageType>>();
	protected static final Logger log = NIOTransport.getLogger();

	abstract protected Integer getPacketType(MessageType message);
	abstract protected MessageType getMessage(String message);
	
	/**
	 * 
	 * @param threadPoolSize
	 *            Refer documentation for {@link #setThreadPoolSize(int)
	 *            setThreadPoolsize(int)}.
	 */
	public AbstractPacketDemultiplexer(int threadPoolSize) {
		this.executor = Executors.newScheduledThreadPool(threadPoolSize);
		this.myThreadPoolSize = threadPoolSize;
	}

	AbstractPacketDemultiplexer() {
		this(getThreadPoolSize());
	}

	// This method will be invoked by NIO
	protected boolean handleMessageSuper(MessageType message)
			throws JSONException {
		Integer type = getPacketType(message);
		if (type==null || !this.demuxMap.containsKey(type)) {
			/*
			 * It is natural for some demultiplexers to not handle some packet
			 * types, so it is not a "bad" thing that requires a warning log.
			 */
			log.log(Level.FINE, "Ignoring unknown packet type: {0}", type);
			return false;
		}
		// else
		Tasker tasker = new Tasker(message, this.demuxMap.get(type));
		if (this.myThreadPoolSize==0)
			tasker.run(); // task better be lightining quick
		else
			// task should still be non-blocking
			executor.schedule(tasker, 0, TimeUnit.MILLISECONDS);
		/*
		 * Note: executor.submit() consistently yields poorer performance than
		 * scheduling at 0 as above even though they are equivalent. Probably
		 * garbage collection or heap optimization issues.
		 */
		return true;
	}

	/** 
	 * Registers {@code type} with {@code this}.
	 * @param type
	 */
	public void register(IntegerPacketType type) {
		register(type, this);
	}

	/**
	 * Registers {@code type} with {@code pd}.
	 * @param type
	 * @param pd
	 */
	public void register(IntegerPacketType type,
			InterfacePacketDemultiplexer<MessageType> pd) {
		if(pd==null) return;
		log.finest("Registering type " + type.getInt() + " with " + pd);
		this.demuxMap.put(type.getInt(), pd);
	}

	/**
	 * Registers {@code types} with {@code pd};
	 * @param types
	 * @param pd
	 */
	public void register(Set<IntegerPacketType> types,
			InterfacePacketDemultiplexer<MessageType> pd) {
		log.info("Registering types " + types + " for " + pd);
		for (IntegerPacketType type : types) 
			register(type, pd);
	}

	/**
	 * Registers {@code types} with {@code this}.
	 * @param types
	 */
	public void register(Set<IntegerPacketType> types) {
		this.register(types, this);
	}

	/**
	 * Registers {@code types} with {@code this}.
	 * @param types
	 * @param pd
	 */
	public void register(Object[] types,
			InterfacePacketDemultiplexer<MessageType> pd) {
		log.info("Registering types "
				+ (new HashSet<Object>(Arrays.asList(types))) + " for " + pd);
		for (Object type : types) 
			register((IntegerPacketType) type, pd);
	}

	/**
	 * Any created instance of AbstractPacketDemultiplexer or its inheritors
	 * must be cleanly closed by invoking this stop method. 
	 */
	public void stop() {
		this.executor.shutdown();
	}

	// Helper task for handleMessageSuper
	protected class Tasker implements Runnable {

		private final MessageType json;
		private final InterfacePacketDemultiplexer<MessageType> pd;

		Tasker(MessageType json, InterfacePacketDemultiplexer<MessageType> pd) {
			this.json = json;
			this.pd = pd;
		}

		public void run() {
			try {
				pd.handleMessage(this.json);
			} catch (Exception e) {
				e.printStackTrace(); // unless printed task will die silently
			} catch (Error e) {
				e.printStackTrace();
			}
		}
	}

}
