
package edu.umass.cs.gnsserver.database;

import com.mongodb.BasicDBObject;
import java.util.ArrayList;
import java.util.List;


public class MongoCollectionSpec {

  private final String name;
  private final ColumnField primaryKey;
  private final BasicDBObject primaryIndex;
  private final List<BasicDBObject> otherIndexes = new ArrayList<>();


  public MongoCollectionSpec(String name, ColumnField primaryKey) {
    this.name = name;
    this.primaryKey = primaryKey;
    this.primaryIndex = new BasicDBObject(primaryKey.getName(), 1);
  }


  public String getName() {
    return name;
  }


  public ColumnField getPrimaryKey() {
    return primaryKey;
  }


  public BasicDBObject getPrimaryIndex() {
    return primaryIndex;
  }


  public void addOtherIndex(BasicDBObject index) {
    otherIndexes.add(index);
  }


  public List<BasicDBObject> getOtherIndexes() {
    return otherIndexes;
  }

}
