package edu.umass.cs.nio.nioutils;

import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;

import edu.umass.cs.gigapaxos.paxosutil.RateLimiter;
import edu.umass.cs.nio.AbstractPacketDemultiplexer;
import edu.umass.cs.nio.MessageNIOTransport;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.utils.DelayProfiler;
import edu.umass.cs.utils.Util;

/**
 * @author arun
 * 
 */
public class NIOTester {

	static MessageNIOTransport<Integer, String> niot1 = null, niot2 = null;
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
		int id1 = 101, id2 = 102;
		SampleNodeConfig<Integer> snc = new SampleNodeConfig<Integer>();
		snc.addLocal(101);
		snc.addLocal(102);
		final InetSocketAddress isa1 = new InetSocketAddress(snc.getNodeAddress(id1),
				snc.getNodePort(id1));
		InetSocketAddress isa2 = new InetSocketAddress(snc.getNodeAddress(id2),
				snc.getNodePort(id2));

		final int numTestMessages = 4000000;
		RateLimiter r = new RateLimiter(400000);

		int size = 1000;
		String gibberish = "|47343289u2309exi4322|";
		while (gibberish.length() < size)
			gibberish += gibberish;
		gibberish = gibberish.substring(0, size);
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
							while(niot2.send(isa1, replyBytes, batchSize) <= 0) Thread.yield();;
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
					System.out.println("Response rate after ALL "
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
					niot1.stop();
					niot2.stop();
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

			niot1 = new MessageNIOTransport<Integer, String>(id1, snc,
					new PDSender(), true);

			niot2 = new MessageNIOTransport<Integer, String>(id2, snc,
					new PDEcho(), true);

			t = System.currentTimeMillis();

			int totalSent = 0;
			for (int i = 0; i < numTestMessages / batchSize; i++) {
				int curSent = 0;
				while((curSent = niot1.send(isa2, gibberish.getBytes("ISO-8859-1"), batchSize)) <= 0) Thread.yield();;
				totalSent += curSent;
				r.record();
			}
			System.out.println("Sent "
					+ numTestMessages + " and " + totalSent + " bytes at "
					+ Util.df(numTestMessages * 1000.0
							/ (System.currentTimeMillis() - t)) + "/sec");

		} catch (Exception e) {
			e.printStackTrace();
			niot1.stop();
			niot2.stop();
		}
	}
}
