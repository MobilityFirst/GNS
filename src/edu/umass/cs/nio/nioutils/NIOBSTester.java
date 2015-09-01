package edu.umass.cs.nio.nioutils;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import edu.umass.cs.gigapaxos.paxosutil.RateLimiter;
import edu.umass.cs.nio.InterfaceDataProcessingWorker;
import edu.umass.cs.nio.NIOTransport;
import edu.umass.cs.utils.Util;

/**
 * @author arun
 * 
 *         A simple local tester for SSL. Run this class in two separate
 *         terminals, one with argument 100, and the other with argument 101.
 *         You also need to configure SSL parameters like keyStore, trustStore
 *         and their passwords for the JVM.
 */
public class NIOBSTester {

	static NIOTransport<Integer> niot1 = null, niot2 = null;
	static long t = System.currentTimeMillis();
	static boolean twoWay = true;

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		int offset = (args.length > 0 ? Integer.valueOf(args[0]) : 0);
		int id1 = 101 + offset, id2 = 102 + offset;
		SampleNodeConfig<Integer> snc = new SampleNodeConfig<Integer>();
		snc.addLocal(id1);
		snc.addLocal(id2);
		final InetSocketAddress isa1 = new InetSocketAddress(snc.getNodeAddress(id1),
				snc.getNodePort(id1));
		InetSocketAddress isa2 = new InetSocketAddress(snc.getNodeAddress(id2),
				snc.getNodePort(id2));

		final int numTestMessages = 1000000;
		RateLimiter r = new RateLimiter(400000);

		String gibberish = "|47343289u23094322|";
		while (gibberish.length() < 300)
			gibberish += gibberish;
		byte[] sendBytes = gibberish.getBytes();
		final int msgSize = sendBytes.length;
		final byte[] replyBytes = new byte[sendBytes.length / 1];
		final int replySize = replyBytes.length;
		final int replyRatio = 1;
		final int printFreq = numTestMessages / 10 / replyRatio;

		final int batchSize = 1;

		class DPWEcho implements InterfaceDataProcessingWorker {
			int count = 0;
			int msgCount = 0;

			@Override
			public void processData(SocketChannel socket, ByteBuffer incoming) {
				byte[] buf = new byte[incoming.remaining()];
				incoming.get(buf);
				boolean sendReply = false;
				synchronized (this) {
					count += buf.length;
					msgCount++;
					sendReply = ((count / msgSize) % (batchSize * replyRatio) == 0);
				}

				if (sendReply || true) {
					try {
						if (twoWay)
							assert (niot2.send(isa1, buf, batchSize) > 0);
					} catch (Exception e) {
						e.printStackTrace();
					}

					if (count == numTestMessages * msgSize)
						System.out
								.println("Request receipt rate after receiving *ALL* "
										+ msgCount
										+ " requests and "
										+ count
										+ " bytes = "
										+ Util.df(count
												/ msgSize
												* 1000.0
												/ (System.currentTimeMillis() - t))
										+ "/sec ");
				}
			}

		}
		class DPWSender implements InterfaceDataProcessingWorker {
			int count = 0;
			int msgCount = 0;

			@Override
			public void processData(SocketChannel socket, ByteBuffer incoming) {
				byte[] message = new byte[incoming.remaining()];
				incoming.get(message);

				synchronized (this) {
					count += message.length;
					msgCount++;
				}
				if (count == (numTestMessages * replySize) / replyRatio) {
					// if (msgCount == numTestMessages) {
					System.out.println("Response rate after ALL "
							// + msgCount
							+ count
							+ " bytes and "
							+ msgCount
							+ " packets  = "
							+ Util.df(count / replySize * 1000.0
									/ (System.currentTimeMillis() - t))
							+ "/sec " + "; total_time = "
							+ (System.currentTimeMillis() - t) / 1000
							+ " secs; exiting");
					niot1.stop();
					niot2.stop();
				} else if (msgCount % printFreq == 0) {
					System.out.println("Response rate after "
							+ count
							+ " bytes and "
							+ msgCount
							+ " packets = "
							+ Util.df(count / replySize * 1000.0
									/ (System.currentTimeMillis() - t))
							+ "/sec ");
				}
			}
		}

		try {
			niot1 = new NIOTransport<Integer>(id1, snc, new DPWSender());
			niot2 = new NIOTransport<Integer>(id2, snc, new DPWEcho());

			t = System.currentTimeMillis();
			int numBytesSent = 0;
			for (int i = 0; i < numTestMessages / batchSize; i++) {
				numBytesSent += niot1.send(isa2, sendBytes, batchSize);
				r.record();
			}
			System.out.println("Sent "
					+ numTestMessages
					+ " and "
					+ numBytesSent
					+ " bytes at "
					+ Util.df(numTestMessages * 1000.0
							/ (System.currentTimeMillis() - t)) + "/sec");

			// if(false)
			{
				Thread.sleep(10000);
				numBytesSent += niot1.send(isa2,
						("*******poke*******" + gibberish).getBytes(),
						batchSize) + 8;
			}

		} catch (Exception e) {
			e.printStackTrace();
			niot1.stop();
			niot2.stop();
		}
	}
}
