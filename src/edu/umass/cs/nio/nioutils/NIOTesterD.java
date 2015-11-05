package edu.umass.cs.nio.nioutils;

import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;

import edu.umass.cs.gigapaxos.paxosutil.RateLimiter;
import edu.umass.cs.nio.AbstractPacketDemultiplexer;
import edu.umass.cs.nio.MessageNIOTransport;
import edu.umass.cs.nio.SSLDataProcessingWorker;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.utils.DelayProfiler;
import edu.umass.cs.utils.Util;

/**
 * @author arun
 * 
 */
public class NIOTesterD {

	static MessageNIOTransport<Integer, String> niot = null;
	static long t = System.currentTimeMillis();
	/**
	 * 
	 */
	public static String gibberish = "|47343289u23094322|";
	static boolean twoWay = true;

	/**
	 * @param args
	 * @throws UnsupportedEncodingException
	 */
	public static void main(String[] args) throws UnsupportedEncodingException {
		if(args.length<3) throw new RuntimeException("IP:port must be specified as args");
		InetSocketAddress isa1 = Util.getInetSocketAddressFromString(args[0]);
		final InetSocketAddress isa2 = Util.getInetSocketAddressFromString(args[1]);
		String mode = args[2];
		
		final int numTestMessages = 1000000;
		RateLimiter r = new RateLimiter(400000);

		int size = args.length > 3 ? Integer.valueOf(args[3]) : 1000;
		while (gibberish.length() < size)
			gibberish += gibberish;
		if(gibberish.length() > size) gibberish = gibberish.substring(0, size);
		System.out.println("message_size = " + size);
		
		final int msgSize = gibberish.length();
		final int batchSize = 1;
		final byte[] replyBytes = new byte[gibberish.length() / 10];
		final int replySize = replyBytes.length;
		final int replyRatio = 1;
		final int printFreq = numTestMessages / 10 / replyRatio;


		class PDEcho extends AbstractPacketDemultiplexer<String> {
			int count = 0;

			PDEcho() {
				this.register(new IntegerPacketType() {

					@Override
					public int getInt() {
						return 2;
					}
				});
			}

			@Override
			public boolean handleMessage(String message) {
				boolean sendReply = false;
				synchronized (this) {
					count += message.length();
					sendReply = (count / msgSize) % (batchSize*replyRatio) == 0;
				}
				try {
					if (sendReply) {
						if (twoWay)
							while(niot.send(isa2, replyBytes, batchSize) <=0) Thread.sleep(1);
					}
				} catch (Exception e) {
					e.printStackTrace();
				}

				if (count == numTestMessages * msgSize) {
					System.out.println("Receiver rate after receiving all "
							+ count
							/ msgSize
							+ " requests = "
							+ Util.df(count / msgSize * 1000.0
									/ (System.currentTimeMillis() - t))
							+ "/sec ");
				}
				return true;
			}

			@Override
			protected Integer getPacketType(String message) {
				return 2;
			}

			@Override
			protected String getMessage(String message) {
				return message;
			}

			@Override
			protected String processHeader(String message, NIOHeader header) {
				return message;
			}

			@Override
			protected boolean matchesType(Object message) {
				return message instanceof String;
			}

		}
		class PDSender extends AbstractPacketDemultiplexer<String> {
			int count = 0;
			int msgCount = 0;

			PDSender() {
				this.register(new IntegerPacketType() {
					@Override
					public int getInt() {
						return 1;
					}
				});
			}

			@Override
			public boolean handleMessage(String message) {
				synchronized (this) {
					count += message.length();
					msgCount++;
				}
				if (count == numTestMessages/replyRatio * replySize) {
					System.out.println("Response rate after "
							+ count
							/ replySize
							+ " responses = "
							+ Util.df(count / replySize * 1000.0
									/ (System.currentTimeMillis() - t))
							+ "/sec " + DelayProfiler.getStats());
					try {
						Thread.sleep(500);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					niot.stop();
				} else if (msgCount % printFreq == 0) {
					System.out.println("Response rate after "
							+ count
							/ replySize
							+ " = "
							+ Util.df(count / replySize * 1000.0
									/ (System.currentTimeMillis() - t))
							+ "/sec " + DelayProfiler.getStats());
				}

				return true;
			}

			@Override
			protected Integer getPacketType(String message) {
				return 1;
			}

			@Override
			protected String getMessage(String message) {
				return message;
			}

			@Override
			protected String processHeader(String message, NIOHeader header) {
				return message;
			}

			@Override
			protected boolean matchesType(Object message) {
				return message instanceof String;
			}

		}
		try {
			boolean isSender = mode.equals("sender");

			niot = new MessageNIOTransport<Integer, String>(isa1.getAddress(), isa1.getPort(),
					isSender ? new PDSender() : new PDEcho(), SSLDataProcessingWorker.SSL_MODES.CLEAR);

			t = System.currentTimeMillis();

			if(isSender) {
				int totalSent = 0;
				for (int i = 0; i < numTestMessages / batchSize; i++) {
					int sent = 0;
					while ((sent = niot.send(isa2,
							gibberish.getBytes("ISO-8859-1"), batchSize)) <= 0) Thread.sleep(1);
					totalSent += sent;
					r.record();
				}
				System.out.println("Sent "
						+ numTestMessages
						+ " and "
						+ totalSent
						+ " bytes at "
						+ Util.df(numTestMessages * 1000.0
				/ (System.currentTimeMillis() - t)) + "/sec");
			}

		} catch (Exception e) {
			e.printStackTrace();
			niot.stop();
		}
	}
}
