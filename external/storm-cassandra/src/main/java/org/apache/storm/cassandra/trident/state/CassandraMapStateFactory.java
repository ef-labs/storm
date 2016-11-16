package org.apache.storm.cassandra.trident.state;

import org.apache.storm.task.IMetricsContext;
import org.apache.storm.trident.state.*;
import org.apache.storm.trident.state.map.*;

import java.util.Map;

/**
 * A StateFactory implementation that creates a MapState backed by CassandraBackingMap.
 *
 * The statefactory supports opaque, transactional and non-transactional configurations.
 * Optionally, the backing map can be wrapped in a {@link CachedMap} by specifying {@link #withCache} (off by default).
 *
 */
public class CassandraMapStateFactory implements StateFactory {

    private final StateType stateType;
    private final CassandraBackingMap.Options options;
    private final Serializer serializer;
    private int cacheSize;

    private CassandraMapStateFactory(StateType stateType, CassandraBackingMap.Options options, Serializer serializer) {
        this.stateType = stateType;
        this.options = options;
        this.serializer = serializer;
    }

    public static <T> CassandraMapStateFactory opaque(CassandraBackingMap.Options options) {
        return new CassandraMapStateFactory(StateType.OPAQUE, options, new JSONOpaqueSerializer());
    }

    public static <T> CassandraMapStateFactory opaque(CassandraBackingMap.Options options, Serializer<OpaqueValue<T>> serializer) {
        return new CassandraMapStateFactory(StateType.OPAQUE, options, serializer);
    }

    public static <T> CassandraMapStateFactory transactional(CassandraBackingMap.Options options) {
        return new CassandraMapStateFactory(StateType.TRANSACTIONAL, options, new JSONTransactionalSerializer());
    }

    public static <T> CassandraMapStateFactory transactional(CassandraBackingMap.Options options, Serializer<TransactionalValue<T>> serializer) {
        return new CassandraMapStateFactory(StateType.TRANSACTIONAL, options, serializer);
    }

    public static <T> CassandraMapStateFactory nonTransactional(CassandraBackingMap.Options options) {
        return new CassandraMapStateFactory(StateType.NON_TRANSACTIONAL, options, new JSONNonTransactionalSerializer());
    }

    public static <T> CassandraMapStateFactory nonTransactional(CassandraBackingMap.Options options, Serializer<T> serializer) {
        return new CassandraMapStateFactory(StateType.NON_TRANSACTIONAL, options, serializer);
    }

    public CassandraMapStateFactory withCache(int cacheSize) {
        this.cacheSize = cacheSize;
        return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {

        CassandraBackingMap cassandraBackingMap = new CassandraBackingMap(conf, options);
        cassandraBackingMap.prepare();

        IBackingMap backingMap = cacheSize > 0
                ? new CachedMap<>(cassandraBackingMap, cacheSize)
                : cassandraBackingMap;

        MapState<?> mapState;

        switch (stateType) {
            case OPAQUE:
                mapState = OpaqueMap.build((IBackingMap<OpaqueValue>) backingMap);
                break;

            case TRANSACTIONAL:
                mapState = TransactionalMap.build((IBackingMap<TransactionalValue>)backingMap);
                break;

            case NON_TRANSACTIONAL:
                mapState = NonTransactionalMap.build(backingMap);
                break;

            default:
                throw new IllegalArgumentException("Invalid state provided " + stateType);
        }

        return mapState;

    }
}
