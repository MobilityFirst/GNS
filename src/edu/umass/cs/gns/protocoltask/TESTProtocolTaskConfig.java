package edu.umass.cs.gns.protocoltask;

/**
 * @author V. Arun
 */

/*
 * Mostly static methods to help with testing or instrumentation.
 */
public class TESTProtocolTaskConfig {

	private static boolean enable_drop = false;
	public static final boolean DETERMINISTIC_DROP = true;
	public static final int DETERMINISTIC_DROP_FREQUENCY = 3; // drop every third message
	public static final double RANDOM_DROP_RATE = DETERMINISTIC_DROP ? 0 : 0.8; // random drop probability

	// returns whether to simulate loss given a message number
	public static boolean shouldDrop(int id) {
		if (!enable_drop)
			return false;
		if (DETERMINISTIC_DROP)
			return (id % DETERMINISTIC_DROP_FREQUENCY == 0);
		else
			return Math.random() < RANDOM_DROP_RATE;
	}

	public static void setDrop(boolean b) {
		enable_drop = b;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		for (int i = 0; i < 100; i++)
			assert (!TESTProtocolTaskConfig.shouldDrop(i));
		TESTProtocolTaskConfig.setDrop(true);
		for (int i = 0; i < 100; i++) {
			if (i % TESTProtocolTaskConfig.DETERMINISTIC_DROP_FREQUENCY == 0) {
				assert (TESTProtocolTaskConfig.shouldDrop(i));
			}
		}
		System.out.println("SUCCESS");
	}

}
