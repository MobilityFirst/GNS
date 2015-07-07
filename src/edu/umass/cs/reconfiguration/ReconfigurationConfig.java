package edu.umass.cs.reconfiguration;

import edu.umass.cs.gigapaxos.PaxosManager;
import edu.umass.cs.nio.SSLDataProcessingWorker;
import edu.umass.cs.reconfiguration.reconfigurationutils.DemandProfile;

/**
 * @author arun
 * 
 *         Reconfiguration configuration parameters. These parameters are
 *         expected to be statically set at JVM initiation time and should not
 *         be changed subsequently. A static code block can be used to read
 *         parameters from a file and set them using methods here.
 *         Reconfiguration parameters that can be dynamically changed after
 *         initiation are generally supported using get/set methods in their
 *         respective classes.
 * 
 *         FIXME: Woefully incomplete. Many parameters that are static final
 *         constants in various classes should be maintained here.
 */
public class ReconfigurationConfig {
	/**
	 * The default demand profile type. This will reconfigure once per request,
	 * so you really want to use something else.
	 */
	public static final Class<?> DEFAULT_DEMAND_PROFILE_TYPE = DemandProfile.class;
	private static Class<?> demandProfileType = DEFAULT_DEMAND_PROFILE_TYPE;

	/**
	 * Whether reconfigurations should be performed even though
	 * AbstractDemandProfile returned a set of active replicas that are
	 * identical to the current one. Useful for testing, but should be false in
	 * production.
	 */
	public static final boolean RECONFIGURE_IN_PLACE = true;// false;
	private static boolean reconfigureInPlace = RECONFIGURE_IN_PLACE;

	/**
	 * Default TLS authentication mode for client-server communication. Here,
	 * "client" means an end-client, not the (more general) initiator of
	 * communication. An end-client generally also is the initiator of
	 * communication, but the converse is not true. We generally want this to
	 * either be CLEAR or SERVER_AUTH, not MUTUAL_AUTH, as it is not generally
	 * meaningful for a server to authenticate an end-client for TLS purposes
	 * (as opposed to application-level authentication).
	 */
	public static final SSLDataProcessingWorker.SSL_MODES DEFAULT_CLIENT_SSL_MODE = SSLDataProcessingWorker.SSL_MODES.CLEAR;
	private static SSLDataProcessingWorker.SSL_MODES clientSSLMode = DEFAULT_CLIENT_SSL_MODE;

	/**
	 * Default TLD authentication mode for server-server communication. We
	 * generally want this to be MUTUAL_AUTH as both parties need to
	 * authenticate each other.
	 */
	public static final SSLDataProcessingWorker.SSL_MODES DEFAULT_SERVER_SSL_MODE = SSLDataProcessingWorker.SSL_MODES.CLEAR;
	private static SSLDataProcessingWorker.SSL_MODES serverSSLMode = DEFAULT_SERVER_SSL_MODE;

	/**
	 * The default offset added to the active replica or reconfigurator port
	 * number in order to get the client-facing port for client-facing request
	 * types (CREATE_SERVICE_NAME, DELETE_SERVICE_NAME, REQUEST_ACTIVE_REPLICAS
	 * at reconfigurators) and all app request types at active replicas. In
	 * general, we need the port for client-facing requests to be different
	 * because the TLS authentication mode for client-server and server-server
	 * communication may be different.
	 */
	public static final int DEFAULT_CLIENT_PORT_OFFSET = 00;// default 100
	private static int clientPortOffset = DEFAULT_CLIENT_PORT_OFFSET;

	/**
	 * True if deletes are completed based on probing all actives.
	 * 
	 * Assumption for safety: Safe only if the set of actives does not change or
	 * is consistent across reconfigurators (not true by default). This option
	 * should only be false in production runs.
	 */
	public static final boolean DEFAULT_AGGRESSIVE_DELETIONS = true;
	private static boolean aggressiveDeletions = DEFAULT_AGGRESSIVE_DELETIONS;

	/**
	 * True if further reconfigurations can progress without waiting for the
	 * previous epoch final state to be dropped cleanly.
	 */
	public static final boolean DEFAULT_AGGRESSIVE_RECONFIGURATIONS = true;
	private static boolean aggressiveReconfigurations = DEFAULT_AGGRESSIVE_RECONFIGURATIONS;

	/**
	 * Necessary to ensure safety under name re-creations (see
	 * {@link #getDelayedDeleteWaitDuration()} below). We also use this timeout
	 * for garbage collecting remote checkpoints transferred using the file
	 * system.
	 * 
	 * @return The value of MAX_FINAL_STATE_AGE used by paxos. This is the time
	 *         after which paxos' epoch final state can be safely deleted.
	 */
	public static final long getMaxFinalStateAge() {
		return PaxosManager.MAX_FINAL_STATE_AGE;
	}

	/**
	 * The time for which we must wait before finally deleting a name's
	 * reconfiguration record (i.e., all memory of that name is lost) must be at
	 * least as high as paxos' MAX_FINAL_STATE_AGE, otherwise it can cause the
	 * creation of a name or addition of a reconfigurator to stall for
	 * arbitrarily long, or worse, violate safety by using incorrect state from
	 * previous incarnations.
	 * 
	 * @return The time for which deleted records should be left waiting so that
	 *         they don't get recreated subsequently with un-garbage-collected
	 *         copies of epoch final state from previous incarnations.
	 */
	public static final long getDelayedDeleteWaitDuration() {
		return getMaxFinalStateAge();
	}

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
	public static void setClientSSLMode(
			SSLDataProcessingWorker.SSL_MODES sslMode) {
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
	public static void setServerSSLMode(
			SSLDataProcessingWorker.SSL_MODES sslMode) {
		serverSSLMode = sslMode;
	}

	/**
	 * @return The default SSL mode for server-server communication.
	 */
	public static SSLDataProcessingWorker.SSL_MODES getServerSSLMode() {
		return serverSSLMode;
	}

	/**
	 * @return True if mutual authentication between servers is enabled.
	 */
	public static boolean isTLSEnabled() {
		return getServerSSLMode().equals(
				SSLDataProcessingWorker.SSL_MODES.MUTUAL_AUTH);
	}

	/**
	 * @param offset
	 */
	public static void setClientPortOffset(int offset) {
		clientPortOffset = offset;
	}

	/**
	 * @return The client port offset, i.e., the port number that is to be added
	 *         to the standard port in order to get the client-facing port. A
	 *         nonzero offset is needed to support transport layer security
	 *         between servers.
	 */
	public static int getClientPortOffset() {
		return clientPortOffset;
	}

	/**
	 * @return True is aggressive recreations allowed.
	 */
	public static boolean aggressiveDeletionsAllowed() {
		return aggressiveDeletions;
	}

	/**
	 * @param enable
	 */
	public static void setAggressiveDeletions(boolean enable) {
		aggressiveDeletions = enable;
	}

	/**
	 * @return True is aggressive reconfigurations are allowed.
	 */
	public static boolean aggressiveReconfigurationsAllowed() {
		return aggressiveReconfigurations;
	}

	/**
	 * @param enable
	 */
	public static void setAggressiveReconfigurations(boolean enable) {
		aggressiveReconfigurations = enable;
	}
}
