package edu.umass.cs.reconfiguration;

// import edu.umass.cs.gns.newApp.NullDemandProfile;
import edu.umass.cs.reconfiguration.reconfigurationutils.DemandProfile;

public class ReconfigurationConfig {
	private static final Class<?> DEFAULT_DEMAND_PROFILE_TYPE = DemandProfile.class;
	private static Class<?> demandProfileType = DEFAULT_DEMAND_PROFILE_TYPE;
	
	private static final boolean RECONFIGURE_IN_PLACE = false;
	private static boolean reconfigureInPlace = RECONFIGURE_IN_PLACE;

	public static Class<?> setDemandProfile(Class<?> newDP) {
		Class<?> oldDP = demandProfileType;
		demandProfileType = newDP;
		return oldDP;
	}
	public static Class<?> getDemandProfile() {
		return demandProfileType;
	}
	
	public static void setReconfigureInPlace(boolean b) {
		reconfigureInPlace = b;
	}
	public static boolean shouldReconfigureInPlace() {
		return reconfigureInPlace;
	}
}
