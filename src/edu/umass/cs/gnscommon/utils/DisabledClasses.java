package edu.umass.cs.gnscommon.utils;

import java.util.HashSet;
import java.util.Set;


@SuppressWarnings("deprecation")
public class DisabledClasses {


	public static final Set<Class<?>> DISABLED = new HashSet<Class<?>>(
//			Arrays.asList(UniversalHttpClient.class,
//					UniversalHttpClientExtended.class)
        );


	public static final void checkDisabled(Class<?> clazz) {
		if (DISABLED.contains(clazz))
			throw new RuntimeException(clazz
					+ " has been disabled, so it can no longer be used");
	}
}
