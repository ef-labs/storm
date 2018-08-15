package org.apache.storm.cassandra.trident.state;

import java.io.Serializable;

public interface ICassandraBackingMapProvider extends Serializable {

    ICassandraBackingMap get();

}
