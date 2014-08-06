package edu.umass.cs.gns.util;

/**
 * Used as a the key in the lookup of a key / value in a guid record.
 *
 * @author westy
 */
public class NameRecordKey {
  
 private String value; 
  
 // define some constants for GNS use
 public static final NameRecordKey EdgeRecord = new NameRecordKey("edge");
 public static final NameRecordKey CoreRecord = new NameRecordKey("core");
 public static final NameRecordKey GroupRecord = new NameRecordKey("group");

  public NameRecordKey(String value) {
    this.value = value;
  }
  
  public static NameRecordKey valueOf(String string) {
    return new NameRecordKey(string);
  }
  
  public String getName() {
    return this.value;
  }

  @Override
  public String toString() {
    return "NameRecordKey{" + value + '}';
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    final NameRecordKey other = (NameRecordKey) obj;
    if ((this.value == null) ? (other.value != null) : !this.value.equals(other.value)) {
      return false;
    }
    return true;
  }

  @Override
  public int hashCode() {
    int hash = 7;
    hash = 89 * hash + (this.value != null ? this.value.hashCode() : 0);
    return hash;
  }
  
}
