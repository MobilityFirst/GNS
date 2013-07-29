package edu.umass.cs.gnrs.nameserver;

/**
 *
 * @author westy
 */
public class NameRecordKey {
  
 private String value; 
  
 // define some constants for GNRS use
 public static final NameRecordKey EdgeRecord = new NameRecordKey("edgeRecord");
 public static final NameRecordKey CoreRecord = new NameRecordKey("coreRecord");
 public static final NameRecordKey GroupRecord = new NameRecordKey("groupRecord");

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
