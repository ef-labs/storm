package org.apache.storm.cassandra.trident.state;

import org.apache.storm.trident.state.map.IBackingMap;

import java.util.Map;

/**
 * A Cassandra implementation of {@link IBackingMap}
 *
 * @param <T>
 */
public interface ICassandraBackingMap<T> extends IBackingMap<T> {

    /**
     * Prepares the backing map
     *
     * @param conf    the cassandra configuration
     * @param options the cassandra options
     */
    void prepare(Map conf, CassandraBackingMap.Options<T> options);

}
