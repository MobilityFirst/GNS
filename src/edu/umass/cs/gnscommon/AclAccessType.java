
package edu.umass.cs.gnscommon;


public enum AclAccessType {

  READ_WHITELIST,
  WRITE_WHITELIST,
  READ_BLACKLIST,
  WRITE_BLACKLIST;


  public static String typesToString() {
    StringBuilder result = new StringBuilder();
    String prefix = "";
    for (AclAccessType type : AclAccessType.values()) {
      result.append(prefix);
      result.append(type.name());
      prefix = ", ";
    }
    return result.toString();
  }

}
