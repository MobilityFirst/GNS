package edu.umass.cs.gns.reconfiguration;

// import edu.umass.cs.gns.newApp.NullDemandProfile;
import edu.umass.cs.gns.reconfiguration.reconfigurationutils.DemandProfile;

public class ReconfigurationConfig {
	private static final Class<?> DEFAULT_DEMAND_PROFILE_TYPE = DemandProfile.class;
	private static Class<?> demandProfileType = DEFAULT_DEMAND_PROFILE_TYPE;

	public static Class<?> setDemandProfile(Class<?> newDP) {
		Class<?> oldDP = demandProfileType;
		demandProfileType = newDP;
		return oldDP;
	}
	public static Class<?> getDemandProfile() {
		return demandProfileType;
	}
}
