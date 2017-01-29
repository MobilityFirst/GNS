
package edu.umass.cs.gnsserver.database;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoException;
import edu.umass.cs.gnscommon.exceptions.server.FailedDBOperationException;
import edu.umass.cs.gnscommon.exceptions.server.ServerRuntimeException;
import org.json.JSONException;
import org.json.JSONObject;
import java.util.NoSuchElementException;


public class MongoRecordCursor extends AbstractRecordCursor {

  private DBCursor cursor;


  public MongoRecordCursor(DB db, String collectionName, ColumnField nameField) throws FailedDBOperationException {
    try {
      db.requestEnsureConnection();
      DBCollection collection = db.getCollection(collectionName);
      // get all documents that have a name field (all doesn't work because of extra stuff mongo adds to the database)
      BasicDBObject query = new BasicDBObject(nameField.getName(), new BasicDBObject("$exists", true));

      cursor = collection.find(query);
    } catch (MongoException e) {
      throw new FailedDBOperationException(collectionName, nameField.toString(), 
              "Original mongo exception:" + e.getMessage());
    }
  }


  public MongoRecordCursor(DBCursor cursor, ColumnField nameField) {
    this.cursor = cursor;
  }


  private JSONObject nextAllFieldsJSON() throws FailedDBOperationException {
    try {
      if (cursor.hasNext()) {
        DBObject dbObject = cursor.next();
        try {
          return new JSONObject(dbObject.toString());
        } catch (JSONException e) {
          // Since next can't throw anything which isn't a runtime exception.
          // replace these with not a runtime exception?
          throw new ServerRuntimeException("Error parsing JSON object.");
        }
      } else {
        // replace these with not a runtime exception?
        throw new NoSuchElementException();
      }
    } catch (MongoException e) {
      throw new FailedDBOperationException(cursor.getCollection().getName(), cursor.toString(),
              "Original mongo exception:" + e.getMessage());
    }
  }


  @Override
  public JSONObject nextJSONObject() throws FailedDBOperationException {
    return nextAllFieldsJSON();
  }


  public String nextRowField(String name) {
    if (cursor.hasNext()) {
      DBObject dbObject = cursor.next();
      try {
        // capitalizing on the fact that we know dbObjects are really JSONObjects
        return new JSONObject(dbObject.toString()).getString(name);
      } catch (JSONException e) {
        throw new ServerRuntimeException("Error parsing JSON object.");
      }
    } else {
      // replace these with not a runtime exception?
      throw new NoSuchElementException();
    }
  }


  @Override
  public boolean hasNext() throws FailedDBOperationException {
    try {
      return cursor.hasNext();
    } catch (MongoException e) {
      throw new FailedDBOperationException(cursor.getCollection().getName(), cursor.toString(),
              "Original mongo exception:" + e.getMessage());
    }
  }
}
