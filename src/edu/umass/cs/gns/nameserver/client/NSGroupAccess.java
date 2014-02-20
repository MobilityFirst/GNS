package edu.umass.cs.gns.nameserver.client;

import edu.umass.cs.gns.client.*;
import edu.umass.cs.gns.exceptions.FieldNotFoundException;
import edu.umass.cs.gns.exceptions.RecordNotFoundException;
import edu.umass.cs.gns.nameserver.NameRecord;
import edu.umass.cs.gns.nameserver.NameServer;
import edu.umass.cs.gns.nameserver.ResultValue;

//import edu.umass.cs.gns.packet.QueryResultValue;
/**
 * GroupAccess provides an interface to the group information in the GNS.
 *
 * The members of a group are stored in a record whose key is the GROUP string. 
 *
 * @author westy
 */
public class NSGroupAccess {

  public static ResultValue lookup(String guid) throws RecordNotFoundException, FieldNotFoundException {
    NameRecord nameRecord = NameServer.getNameRecordMultiField(guid, null, GroupAccess.GROUP);
    return nameRecord.getKey(GroupAccess.GROUP);
  }
}
