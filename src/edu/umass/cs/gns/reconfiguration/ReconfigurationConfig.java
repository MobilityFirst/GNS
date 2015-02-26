package edu.umass.cs.gns.reconfiguration;

// import edu.umass.cs.gns.newApp.NullDemandProfile;
import edu.umass.cs.gns.reconfiguration.reconfigurationutils.DemandProfile;

public class ReconfigurationConfig {
	public static Class<?> demandProfileType = DemandProfile.class;
	public static final Class<?> DEFAULT_DEMAND_PROFILE_TYPE = demandProfileType;

	public static Class<?> setDemandProfile(Class<?> newDP) {
		Class<?> oldDP = demandProfileType;
		demandProfileType = newDP;
		return oldDP;
	}
}
