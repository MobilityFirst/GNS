package edu.umass.cs.gnsclient.privacyclient.anonymizedID;

import java.util.HashMap;
import java.util.List;

import edu.umass.cs.gnsclient.privacyclient.common.ACLEntry;

/**
 * Doesn't do anything. Returns each guid in the ACL as its anonymized ID.
 * @author adipc
 *
 */
public class NoopAnonymizedIDCreator implements AnonymizedIDCreationInterface
{

	@Override
	public List<AnonymizedIDEntry> computeAnonymizedIDs(HashMap<String, List<ACLEntry>> aclList) {
		// TODO Auto-generated method stub
		return null;
	}
}