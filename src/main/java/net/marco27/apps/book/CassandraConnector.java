package net.marco27.apps.book;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

/**
 * http://www.baeldung.com/cassandra-with-java
 * https://github.com/eugenp/tutorials/tree/master/persistence-modules/java-cassandra
 */
public class CassandraConnector {

    private Cluster cluster;
    private Session session;

    public void connect(String node, Integer port) {
        Cluster.Builder b = Cluster.builder().addContactPoint(node);
        if (port != null) {
            b.withPort(port);
        }
        cluster = b.build();
        session = cluster.connect();
    }

    public Session getSession() {
        return this.session;
    }

    public void close() {
        session.close();
        cluster.close();
    }
}