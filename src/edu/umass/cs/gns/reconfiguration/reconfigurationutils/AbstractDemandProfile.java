package edu.umass.cs.gns.reconfiguration.reconfigurationutils;

import java.lang.reflect.InvocationTargetException;
import java.net.InetAddress;
import java.util.ArrayList;

import org.json.JSONObject;


import edu.umass.cs.gns.reconfiguration.InterfaceRequest;
import edu.umass.cs.gns.reconfiguration.ReconfigurationConfig;
import java.net.InetSocketAddress;

/**
 * @author V. Arun
 */

/*
 * An implementation of this abstract class must also have a constructor that takes a single
 * JSONObject as an argument. Otherwise, the last static method that automatically creates an
 * instance of this class from a JSONObject will fail.
 * 
 * Refer to reconfiguration.reconfigurationutils.DemandProfile for an example implementation.
 */

public abstract class AbstractDemandProfile {
	private static final Class<?> C = ReconfigurationConfig.getDemandProfile();

	protected static enum Keys {
		SERVICE_NAME
	};

	protected final String name;

	public AbstractDemandProfile(String name) {
		this.name = name;
	}

	/*********************** Start of abstract methods ***************/
	/**
	 * Creates a deep copy of this object. So, it must be the case that the return value != this,
	 * but the return value.equals(this).
         * @return a clone of this demand profile
	 */
        @Override
	public abstract AbstractDemandProfile clone();

	// Incorporate this new request information
	public abstract void register(InterfaceRequest request, 
                InetAddress sender,
                ConsistentReconfigurableNodeConfig nodeConfig
        );

	/*
	 * A policy that tells whether it is time to report from an active replica to a reconfigurator.
	 * Actives in general should not report upon every request, but at some coarser frequency to
	 * limit overhead.
	 */
	public abstract boolean shouldReport();

	// All relevant stats must be serializable into JSON.
	public abstract JSONObject getStats();

	// Clear all info, i.e., forget all previous stats.
	public abstract void reset();

	/*
	 * Combine the new information into "this". This method is useful at reconfigurators to combine
	 * a newly received demand report with an existing demand report.
	 */
	public abstract void combine(AbstractDemandProfile update);

	// The main reconfiguration policy
	public abstract ArrayList<InetAddress> shouldReconfigure(ArrayList<InetAddress> curActives,
                ConsistentReconfigurableNodeConfig nodeConfig);
      
	/*
	 * Tells us that the current demand profile was just used to perform reconfiguration. Useful for
	 * implementing policies based on the difference between the current demand profile and the one
	 * at the time of the most recent reconfiguration.
	 */
	public abstract void justReconfigured();

	/*********************** End of abstract methods ***************/

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
