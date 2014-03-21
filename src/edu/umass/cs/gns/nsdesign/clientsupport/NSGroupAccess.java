package edu.umass.cs.gns.nsdesign.clientsupport;

import edu.umass.cs.gns.clientsupport.GroupAccess;
import edu.umass.cs.gns.exceptions.FieldNotFoundException;
import edu.umass.cs.gns.exceptions.RecordNotFoundException;
import edu.umass.cs.gns.nameserver.NameRecord;
import edu.umass.cs.gns.nameserver.ResultValue;
import edu.umass.cs.gns.nsdesign.activeReplica.ActiveReplica;

//import edu.umass.cs.gns.packet.QueryResultValue;

/**
 * GroupAccess provides an interface to the group information in the GNS.
 *
 * The members of a group are stored in a record whose key is the GROUP string. 
 *
 * @author westy
 */
public class NSGroupAccess {

  public static ResultValue lookup(String guid, ActiveReplica activeReplica) throws RecordNotFoundException, FieldNotFoundException {
    NameRecord nameRecord = NameRecord.getNameRecordMultiField(activeReplica.getNameRecordDB(), guid, null, GroupAccess.GROUP);
    return nameRecord.getKey(GroupAccess.GROUP);
  }
}
