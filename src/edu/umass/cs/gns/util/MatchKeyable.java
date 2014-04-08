package edu.umass.cs.gns.util;
/**
@author V. Arun
 */
public interface MatchKeyable<KeyType,VersionType extends Comparable<VersionType>> extends Keyable<KeyType> {
	public VersionType getVersion();
}
