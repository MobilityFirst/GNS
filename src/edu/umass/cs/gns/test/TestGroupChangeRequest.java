package edu.umass.cs.gns.test;


import edu.umass.cs.gns.nsdesign.nodeconfig.NodeId;
import java.util.HashSet;
import java.util.Set;

/**
 * A group change request for a name that LNS sends to a name server to test group change.
 *
 * Created by abhigyan on 4/10/14.
 */
public class TestGroupChangeRequest extends TestRequest {

  // version number of active replicas
  int version;

  // set of new active replicas
  Set<NodeId<String>> replicaSet;

  public TestGroupChangeRequest(String name, String request) {
    super(name, TestRequest.GROUP_CHANGE);
    String[] tokens = request.split("\\s+");
    this.version = Integer.parseInt(tokens[2]);
    String[] replicaString = tokens[3].split(":");
    this.replicaSet = new HashSet<NodeId<String>>();

    for (String s: replicaString) {
      this.replicaSet.add(new NodeId<String>(s));
    }

  }
}
