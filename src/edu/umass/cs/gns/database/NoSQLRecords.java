/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.umass.cs.gns.database;

import java.util.ArrayList;
import java.util.Set;
import org.json.JSONObject;

/**
 * Provides an interface insert, update, remove and lookup operations in a nosql database
 *
 * @author westy
 */
public interface NoSQLRecords {
  public void reset();
  public JSONObject lookup(String collection, String guid);
  public String lookup(String collection, String guid, String key);
  public void insert(String collection, String guid, JSONObject value);
  public void update(String collection, String guid, JSONObject value);
  public void updateListValue(String collection, String guid, String key,  ArrayList<String> value);
  public void updateField(String collectionName, String guid, String key, String string);
  public boolean contains(String collection, String guid);
  public void remove(String collection, String guid);
  public ArrayList<JSONObject> retrieveAllEntries(String collection);
  public Set<String> keySet(String collection);
  public void printAllEntries(); 
  @Override
  public String toString();
 
}
