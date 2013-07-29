package edu.umass.cs.gns.nameserver;

/**
 *
 * @author westy
 */
public class NameAndRecordKey {
  
  private String name;
  private NameRecordKey recordKey;
  
  public NameAndRecordKey(String name, NameRecordKey recordKey) {
    this.recordKey = recordKey;
    this.name = name;
  }

  public String getName() {
    return name;
  }

  public NameRecordKey getRecordKey() {
    return recordKey;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    final NameAndRecordKey other = (NameAndRecordKey) obj;
    if (this.recordKey != other.recordKey && (this.recordKey == null || !this.recordKey.equals(other.recordKey))) {
      return false;
    }
    if ((this.name == null) ? (other.name != null) : !this.name.equals(other.name)) {
      return false;
    }
    return true;
  }

  @Override
  public int hashCode() {
    int hash = 7;
    hash = 37 * hash + (this.recordKey != null ? this.recordKey.hashCode() : 0);
    hash = 37 * hash + (this.name != null ? this.name.hashCode() : 0);
    return hash;
  }

  @Override
  public String toString() {
    return "NameAndRecordKey{" + " name=" + name + ", key=" + recordKey.getName() +  '}';
  }
 
}
