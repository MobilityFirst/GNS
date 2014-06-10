/*
 * Copyright (C) 2014x
 * University of Massachusetts
 * All Rights Reserved 
 */
package edu.umass.cs.gns.databaseV2;

import edu.umass.cs.gns.database.BasicRecordCursor;
import edu.umass.cs.gns.exceptions.FailedUpdateException;
import edu.umass.cs.gns.exceptions.RecordExistsException;
import edu.umass.cs.gns.exceptions.RecordNotFoundException;
import org.json.JSONObject;
import java.util.ArrayList;

/**
 * Provides an interface for insert, update, remove and lookup operations in a nosql database.
 *
 *
 * @author Westy
 */
public interface NoSQLRecordsV2 {

  /**
   * Create a new record (row) with the given name using the JSONObject.
   *
   * @param collection
   * @param name
   * @param value
   * @throws edu.umass.cs.gns.exceptions.FailedUpdateException
   * @throws edu.umass.cs.gns.exceptions.RecordExistsException
   */
  public void insert(String collection, String name, JSONObject value) throws FailedUpdateException, RecordExistsException;

  /**
   * Do a bulk insert of a bunch of documents into the database.
   *
   * @param collection collection to be inserted into.
   * @param values list of records to be inserted
   * @throws edu.umass.cs.gns.exceptions.FailedUpdateException
   * @throws edu.umass.cs.gns.exceptions.RecordExistsException
   */
  public void bulkInsert(String collection, ArrayList<JSONObject> values) throws FailedUpdateException, RecordExistsException;

  /**
   * Update the record with the given name using the JSONObject.
   *
   * @param collection
   * @param name
   * @param value
   * @throws edu.umass.cs.gns.exceptions.FailedUpdateException
   */
  public void update(String collection, String name, JSONObject value) throws FailedUpdateException;

  /**
   * Updates the record indexed by name using the JSONObject.
   *
   * @param collectionName
   * @param name
   * @param value
   * @param query
   * @return Returns true if the update happened, false otherwise.
   * @throws FailedUpdateException
   */
  public boolean update(String collectionName, String name, JSONObject value, JSONObject query) throws FailedUpdateException;

  /**
   * For the record with given name, return the entire record as a JSONObject.
   *
   * @param collection
   * @param name
   * @return
   * @throws RecordNotFoundException
   */
  public JSONObject find(String collection, String name) throws RecordNotFoundException;

  /**
   * In the record with given name, return the fields matching the projection.
   *
   * @param collection
   * @param name
   * @param projection
   * @return
   * @throws RecordNotFoundException
   */
  public JSONObject find(String collection, String name, JSONObject projection) throws RecordNotFoundException;

  /**
   * Return all the records as a BasicRecordCursor that match query.
   *
   * @param collectionName
   * @param query
   * @return BasicRecordCursor
   */
  public BasicRecordCursor find(String collectionName, JSONObject query);

  /**
   * Return all the records as a BasicRecordCursor that match query. Return the fields matching the projection.
   *
   * @param collectionName
   * @param query
   * @param projection
   * @return BasicRecordCursor
   */
  public BasicRecordCursor find(String collectionName, JSONObject query, JSONObject projection);

  /**
   * Updates the record indexed by name if the record matches the query criteria.
   * Returns true if a record with the given name exists, false otherwise.
   *
   * @param collection
   * @param name
   * @return
   */
  public boolean contains(String collection, String name);

  /**
   * Remove the record with the given name.
   *
   * @param collection
   * @param name
   * @throws edu.umass.cs.gns.exceptions.FailedUpdateException
   */
  public void remove(String collection, String name) throws FailedUpdateException;

  /**
   * For the record with given name, increment the values of field that match projection.
   *
   * @param collection
   * @param name
   * @param projection
   * @throws edu.umass.cs.gns.exceptions.FailedUpdateException
   */
  public abstract void increment(String collection, String name, JSONObject projection) throws FailedUpdateException;

  /**
   * Returns an iterator for all the rows in the collection with only the columns in fields filled in except
   * the NAME (AKA the primary key) is always there.
   *
   * @param collection
   * @param projection
   * @return
   */
  public BasicRecordCursor getAllRowsIterator(String collection, JSONObject projection);

  /**
   * Returns an iterator for all the rows in the collection with all fields filled in.
   *
   * @param collection
   * @return BasicRecordCursor
   */
  public BasicRecordCursor getAllRowsIterator(String collection);

  /**
   * Sets the collection back to an initial empty state with indexes also initialized.
   *
   * @param collection
   */
  public void reset(String collection);

  @Override
  public String toString();

  /**
   * Print all the records (ONLY FOR DEBUGGING).
   *
   * @param collection
   */
  public void printAllEntries(String collection);

}
