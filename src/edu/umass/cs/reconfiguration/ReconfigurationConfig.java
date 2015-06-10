package edu.umass.cs.reconfiguration;

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
}
