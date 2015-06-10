package edu.umass.cs.reconfiguration.reconfigurationutils;

import java.lang.reflect.InvocationTargetException;
import java.net.InetAddress;
import java.util.ArrayList;

import org.json.JSONObject;

import edu.umass.cs.gigapaxos.InterfaceRequest;
import edu.umass.cs.reconfiguration.ReconfigurationConfig;

/**
 * @author V. Arun
 * 
 *         An implementation of this abstract class must also have a constructor
 *         that takes a single JSONObject as an argument. Otherwise, the last
 *         static method that automatically creates an instance of this class
 *         from a JSONObject will fail.
 * 
 *         Refer to reconfiguration.reconfigurationutils.DemandProfile for an
 *         example implementation.
 */

public abstract class AbstractDemandProfile {
	private static final Class<?> C = ReconfigurationConfig.getDemandProfile();

	protected static enum Keys {
		SERVICE_NAME
	};

	protected final String name;

	/**
	 * @param name The service name of the reconfiguree replica group.
	 */
	public AbstractDemandProfile(String name) {
		this.name = name;
	}

	/*********************** Start of abstract methods ***************/
	/*
	 * Creates a deep copy of this object. So, it must be the case that the
	 * return value != this, but the return value.equals(this).
	 */
	public abstract AbstractDemandProfile clone();

	/**
	 * Incorporate this new request information.
	 * @param request
	 * @param sender
	 * @param nodeConfig
	 */
	public abstract void register(InterfaceRequest request, InetAddress sender,
			InterfaceGetActiveIPs nodeConfig);

	/**
	 * A policy that tells whether it is time to report from an active replica
	 * to a reconfigurator. Actives in general should not report upon every
	 * request, but at some coarser frequency to limit overhead.
	 * 
	 * @return Whether the active replica should send a demand report to the reconfigurator.
	 */
	public abstract boolean shouldReport();

	/**
	 * All relevant stats must be serializable into JSON.
	 * @return Demand statistics as JSON.
	 */
	public abstract JSONObject getStats();

	/**
	 *  Clear all info, i.e., forget all previous stats.
	 */
	public abstract void reset();

	/**
	 * Combine the new information into "this". This method is useful at
	 * reconfigurators to combine a newly received demand report with an
	 * existing demand report.
	 * 
	 * @param update 
	 */
	public abstract void combine(AbstractDemandProfile update);

	/**
	 * The main reconfiguration policy
	 * 
	 * @param curActives
	 * @param nodeConfig
	 * 
	 * @return The list of new IP addresses for the new placement. If the new
	 *         placement is the same as the current one, a trivial
	 *         reconfiguration will happen unless
	 *         {@link ReconfigurationConfig#setReconfigureInPlace(boolean)} is
	 *         set to false.
	 */
	public abstract ArrayList<InetAddress> shouldReconfigure(
			ArrayList<InetAddress> curActives, InterfaceGetActiveIPs nodeConfig);

	/**
	 * Tells us that the current demand profile was just used to perform
	 * reconfiguration. Useful for implementing policies based on the difference
	 * between the current demand profile and the one at the time of the most
	 * recent reconfiguration.
	 */
	public abstract void justReconfigured();

	/* ********************** End of abstract methods ************** */

	/**
	 * @return Name of reconfiguree.
	 */
	public final String getName() {
		return this.name;
	}

	protected static AbstractDemandProfile createDemandProfile(String name) {
		try {
			return (AbstractDemandProfile) C.getConstructor(String.class)
					.newInstance(name);
		} catch (InstantiationException | IllegalAccessException
				| IllegalArgumentException | InvocationTargetException
				| NoSuchMethodException | SecurityException e) {
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * @param json
	 * @return Reflection-based constructor to create demand profile.
	 */
	public static AbstractDemandProfile createDemandProfile(JSONObject json) {
		try {
			return (AbstractDemandProfile) C.getConstructor(JSONObject.class)
					.newInstance(json);
		} catch (InstantiationException | IllegalAccessException
				| IllegalArgumentException | InvocationTargetException
				| NoSuchMethodException | SecurityException e) {
			e.printStackTrace();
		}
		return null;
	}
}
