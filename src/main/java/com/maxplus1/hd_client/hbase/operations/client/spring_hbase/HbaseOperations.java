package com.maxplus1.hd_client.hbase.operations.client.spring_hbase;

import com.maxplus1.hd_client.hbase.operations.client.TableCallback;
import org.apache.hadoop.hbase.client.Scan;

import java.util.List;

/**
 * Created by qxloo on 2016/12/2.
 */
public interface HbaseOperations {

    /**
     * Executes the given action against the specified table handling resource management.
     * <p>
     * Application exceptions thrown by the action object get propagated to the caller (can only be unchecked).
     * Allows for returning a result object (typically a domain object or collection of domain objects).
     *
     * @param tableName the target table
     * @param action callback object that specifies the action
     * @param <T> action type
     * @return the result object of the callback action, or null
     */
    <T> T execute(String tableName, TableCallback<T> action) ;

    /**
     * Scans the target table, using the given family. The content is processed by the given action typically
     * returning a domain object or collection of domain objects.
     *
     * @param tableName target table
     * @param family column family
     * @param action action handling the scanner results
     * @param <T> action type
     * @return the result object of the callback action, or null
     */
    <T> T find(String tableName, String family, final ResultsExtractor<T> action) ;

    /**
     * Scans the target table, using the given column family and qualifier.
     * The content is processed by the given action typically returning a domain object or collection of domain objects.
     *
     * @param tableName target table
     * @param family column family
     * @param qualifier column qualifier
     * @param action action handling the scanner results
     * @param <T> action type
     * @return the result object of the callback action, or null
     */
    <T> T find(String tableName, String family, String qualifier, final ResultsExtractor<T> action) ;

    /**
     * Scans the target table using the given {@link Scan} object. Suitable for maximum control over the scanning
     * process.
     * The content is processed by the given action typically returning a domain object or collection of domain objects.
     *
     * @param tableName target table
     * @param scan table scanner
     * @param action action handling the scanner results
     * @param <T> action type
     * @return the result object of the callback action, or null
     */
    <T> T find(String tableName, final Scan scan, final ResultsExtractor<T> action) ;

    /**
     * Scans the target table, using the given column family.
     * The content is processed row by row by the given action, returning a list of domain objects.
     *
     * @param tableName target table
     * @param family column family
     * @param action row mapper handling the scanner results
     * @param <T> action type
     * @return a list of objects mapping the scanned rows
     */
    <T> List<T> find(String tableName, String family, final RowMapper<T> action) ;

    /**
     * Scans the target table, using the given column family.
     * The content is processed row by row by the given action, returning a list of domain objects.
     *
     * @param tableName target table
     * @param family column family
     * @param qualifier column qualifier
     * @param action row mapper handling the scanner results
     * @param <T> action type
     * @return a list of objects mapping the scanned rows
     */
    <T> List<T> find(String tableName, String family, String qualifier, final RowMapper<T> action) ;

    /**
     * Scans the target table using the given {@link Scan} object. Suitable for maximum control over the scanning
     * process.
     * The content is processed row by row by the given action, returning a list of domain objects.
     *
     * @param tableName target table
     * @param scan table scanner
     * @param action row mapper handling the scanner results
     * @param <T> action type
     * @return a list of objects mapping the scanned rows
     */
    <T> List<T> find(String tableName, final Scan scan, final RowMapper<T> action) ;

    /**
     * Gets an individual row from the given table. The content is mapped by the given action.
     *
     * @param tableName target table
     * @param rowName row name
     * @param mapper row mapper
     * @param <T> mapper type
     * @return object mapping the target row
     */
    <T> T get(String tableName, String rowName, final RowMapper<T> mapper) ;

    /**
     * Gets an individual row from the given table. The content is mapped by the given action.
     *
     * @param tableName target table
     * @param rowName row name
     * @param familyName column family
     * @param mapper row mapper
     * @param <T> mapper type
     * @return object mapping the target row
     */
    <T> T get(String tableName, String rowName, String familyName, final RowMapper<T> mapper) ;

    /**
     * Gets an individual row from the given table. The content is mapped by the given action.
     *
     * @param tableName target table
     * @param rowName row name
     * @param familyName family
     * @param qualifier column qualifier
     * @param mapper row mapper
     * @param <T> mapper type
     * @return object mapping the target row
     */
    <T> T get(String tableName, final String rowName, final String familyName, final String qualifier, final RowMapper<T> mapper) ;

    /**
     * Puts a single value in to the given table.
     *
     * @param tableName target table
     * @param rowName row name
     * @param familyName family
     * @param qualifier column qualifier
     * @param data the byte array of the data value to be put
     */
    void put(String tableName, final String rowName, final String familyName, final String qualifier, final byte[] data) ;

    /**
     * Deletes a single qualifier in the given table and family.
     *
     * @param tableName target table
     * @param rowName row name
     * @param familyName family
     */
    void delete(String tableName, final String rowName, final String familyName) ;

    /**
     * Deletes a single cell in the given table.
     *
     * @param tableName target table
     * @param rowName row name
     * @param familyName family
     * @param qualifier column qualifier
     */
    void delete(String tableName, final String rowName, final String familyName, final String qualifier) ;
}
