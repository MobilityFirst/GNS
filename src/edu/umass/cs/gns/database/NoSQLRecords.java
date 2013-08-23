/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.umass.cs.gns.database;

import org.json.JSONObject;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * Provides an interface insert, update, remove and lookup operations in a nosql database
 *
 * @author westy
 */
public interface NoSQLRecords {
  public void reset(String collection);
  public JSONObject lookup(String collection, String guid);
  public String lookup(String collection, String guid, String key);
  public void insert(String collection, String guid, JSONObject value);
  public void update(String collection, String guid, JSONObject value);
  public void updateField(String collection, String guid, String key, Object object);
  public boolean contains(String collection, String guid);
  public void remove(String collection, String guid);
  public ArrayList<JSONObject> retrieveAllEntries(String collection);
  public Set<String> keySet(String collection);
  public void printAllEntries(String collection); 
  @Override
  public String toString();
 
}
