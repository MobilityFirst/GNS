package edu.umass.cs.gnscommon.utils;

import java.util.HashSet;
import java.util.Set;

/**
 * 
 * 
 *         This is a utility class to check and enforce that disabled classes
 *         are no longer usable.
 * 
 * @author arun
 */
public class DisabledClasses {

	/**
	 * The list of disabled classes that CAN NOT be used anymore. Disabling is
	 * enforced at run time. Typically, disabled classes or methods are also
	 * deprecated, but disabling is more like an immediate cease-and-desist as
	 * it will break non-compliant clients.
	 */
	public static final Set<Class<?>> DISABLED = new HashSet<Class<?>>(
//			Arrays.asList(UniversalHttpClient.class,
//					UniversalHttpClientExtended.class)
        );

	/**
	 * Will throw a {@link RuntimeException} if {@code clazz} has been disabled.
	 * 
	 * @param clazz
	 */
	public static final void checkDisabled(Class<?> clazz) {
		if (DISABLED.contains(clazz))
			throw new RuntimeException(clazz
					+ " has been disabled, so it can no longer be used");
	}
}
