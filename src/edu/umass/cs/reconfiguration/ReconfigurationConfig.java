package edu.umass.cs.reconfiguration;

import edu.umass.cs.nio.SSLDataProcessingWorker;
import edu.umass.cs.reconfiguration.reconfigurationutils.DemandProfile;

/**
 * @author arun
 * Reconfiguration configuration parameters. FIXME: Woefully incomplete.
 */
public class ReconfigurationConfig {
	private static final Class<?> DEFAULT_DEMAND_PROFILE_TYPE = DemandProfile.class;
	private static Class<?> demandProfileType = DEFAULT_DEMAND_PROFILE_TYPE;
	
	private static final boolean RECONFIGURE_IN_PLACE = true;//false;
	private static boolean reconfigureInPlace = RECONFIGURE_IN_PLACE;
	
	private static final SSLDataProcessingWorker.SSL_MODES DEFAULT_CLIENT_SSL_MODE = SSLDataProcessingWorker.SSL_MODES.CLEAR;
	private static SSLDataProcessingWorker.SSL_MODES clientSSLMode = DEFAULT_CLIENT_SSL_MODE;
	
	private static final SSLDataProcessingWorker.SSL_MODES DEFAULT_SERVER_SSL_MODE = SSLDataProcessingWorker.SSL_MODES.CLEAR;
	private static SSLDataProcessingWorker.SSL_MODES serverSSLMode = DEFAULT_SERVER_SSL_MODE;

	/**
	 * @param newDP
	 * @return Old DemandProfile class.
	 */
	public static Class<?> setDemandProfile(Class<?> newDP) {
		Class<?> oldDP = demandProfileType;
		demandProfileType = newDP;
		return oldDP;
	}
	/**
	 * @return DemandProfile class.
	 */
	public static Class<?> getDemandProfile() {
		return demandProfileType;
	}
	
	/**
	 * @param b
	 */
	public static void setReconfigureInPlace(boolean b) {
		reconfigureInPlace = b;
	}
	
	/**
	 * @return True means in-place reconfigurations will still be conducted.
	 */
	public static boolean shouldReconfigureInPlace() {
		return reconfigureInPlace;
	}
	
	/**
	 * @param sslMode 
	 */
	public static void setClientSSLMode(SSLDataProcessingWorker.SSL_MODES sslMode) {
		clientSSLMode = sslMode;
	}
	
	/**
	 * @return The default SSL mode for client-server communication.
	 */
	public static SSLDataProcessingWorker.SSL_MODES getClientSSLMode() {
		return clientSSLMode;
	}
	
	/**
	 * @param sslMode 
	 */
	public static void setServerSSLMode(SSLDataProcessingWorker.SSL_MODES sslMode) {
		serverSSLMode = sslMode;
	}
	
	/**
	 * @return The default SSL mode for server-server communication.
	 */
	public static SSLDataProcessingWorker.SSL_MODES getServerSSLMode() {
		return serverSSLMode;
	}
}
