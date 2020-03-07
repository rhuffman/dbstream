# dbstream
Classes to convert JDBC ResultSets to Java Streams, using Apache Commons DbUtils

## StreamingResultSetHandler and subclasses

The library provides a subclass of the DbUtils class ResultHandler called StreamingResultSetHandler. This class converts a JDBC ResultSet to a Stream of objects, where each object represents one row in the database. The advantage of using a Stream is that we can take advantage of database cursors. We do not have to read and convert every element of the ResultSet into memory before using them.

Three concrete subclasses of StreamingResultSetHandler are provided:

  * ArrayStreamingHandler: converts a ResultSet to a Stream<Object[]>. Each element of the stream is an Object[], and each element of the array is a column value from the ResultSet.
  * BeanStreamingHandler: converts a ResultSet to a String<Object>. Each element of the stream is a Java bean.
  * MapStreamingHandler: converts a ResultSet to a Map<String,Object>. Each element of the stream is a Map, and each Map entry is keyed by column name.

## StreamingQueryRunner

Unfortunately, the query methods in DbUtils QueryRunner cannot be used. Those methods open and close the Connection, PreparedStatement, and ResultSet that are used to query the database. To make use of cursors, the library provides a subclass of QueryRunner, StreamingQueryRunner, that adds these additional methods: 

```java
    public <T> Stream<T> queryStream(
        String sql, StreamingResultSetHandler<T> handler, Object... args)
        throws SQLException;

    public <T> Stream<T> queryStream(
        Connection connection, String sql, StreamingResultSetHandler<T> handler, Object... args)
        throws SQLException;

```

The first method opens a Connection, PreparedStatement, and ResultSet, each of which is closed when the returned Stream is closed. The second method opens a PreparedStatement and ResultSet, and those are closed when the returned Stream is closed.
