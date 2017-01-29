
package edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport;

import static edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.InternalField.makeInternalFieldString;


public enum MetaDataTypeName {


  READ_WHITELIST("ACL"), 


  WRITE_WHITELIST("ACL"), 


  READ_BLACKLIST("ACL"), 


  WRITE_BLACKLIST("ACL"), 


  TIMESTAMP("MD");
  
  private String prefix;

  private MetaDataTypeName(String prefix) {
    this.prefix = makeInternalFieldString(prefix);
  }


  public String getPrefix() {
    return prefix;
  }
  

  public String getFieldPath() {
    return prefix + "." + name();
  }
 
}
