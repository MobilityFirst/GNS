package edu.umass.cs.gns.util;

import edu.umass.cs.utils.Keyable;

/**
@author V. Arun
 */
public interface MatchKeyable<KeyType extends Comparable<KeyType>,VersionType extends Comparable<VersionType>> extends Keyable<KeyType> {
	public VersionType getVersion();
}
