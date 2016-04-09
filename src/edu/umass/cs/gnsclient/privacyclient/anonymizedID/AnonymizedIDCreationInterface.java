package edu.umass.cs.gnsclient.privacyclient.anonymizedID;

import java.util.HashMap;
import java.util.List;

import edu.umass.cs.gnsclient.privacyclient.common.ACLEntry;

/**
 * This interface defines the methods needed to generate 
 * anonymized IDs.
 * 
 * Multiple approaches of generating anonymized IDs 
 * implement this interface.
 * @author adipc
 *
 */
public interface AnonymizedIDCreationInterface 
{
	public List<AnonymizedIDEntry> 
					computeAnonymizedIDs(HashMap<String, List<ACLEntry>> aclList);
}