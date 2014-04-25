package edu.umass.cs.gns.util;

/**
 * FOR TEST ONLY!!!
 * Store information about a group change request sent by LNS.
 * Uniquely identifies each request so that we can send confirmation to client on completion.
 */
public class GroupChangeIdentifier {
  public String getName() {
    return name;
  }

  // name
  private String name;

  public int getVersion() {
    return version;
  }

  // version number of group
  private int version;
  // local name server

  public GroupChangeIdentifier(String name, int version) {
    this.name = name;
    this.version = version;
  }

  public boolean equals(Object o1) {
    GroupChangeIdentifier o = (GroupChangeIdentifier)o1;
    return o != null && o.name.equals(name) && o.version == version;
  }

  public int hashCode() {
    return toString().hashCode();
  }

  public String toString() {
    return name + "-" + version;
  }

}
