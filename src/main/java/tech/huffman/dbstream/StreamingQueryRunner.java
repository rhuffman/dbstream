/*
 * Copyright 2019 Robert Huffman
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package tech.huffman.dbstream;

import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;

import javax.sql.DataSource;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.stream.Stream;

/**
 * An extension of the Apache DbUtils QueryRunner that adds methods to produce Streams
 * where each element of the stream is a row in a ResultSet. This takes advantage of
 * database cursors (assuming the underlying JDBC ResultSet does) so the entire query
 * result does not have to be read into memory.q
 */
public class StreamingQueryRunner extends QueryRunner {

  public StreamingQueryRunner(DataSource dataSource) {
    super(dataSource);
  }

  /**
   * Executes a Query and returns a Stream in which each element represents a row in the result.
   * (This method cannot be named "query" because it would hide the corresponding methods in
   * the superclass.)
   *
   * @param sql The SQL query to execute
   * @param handler The ResultSetHandler that converts the ResultSet to a Stream
   * @param args The arguments to pass to the query as prepared statement parameters
   */
  public <T> Stream<T> queryAsStream(String sql, ResultSetHandler<Stream<T>> handler, Object... args)
      throws SQLException {
    Connection connection = getDataSource().getConnection();
    PreparedStatement statement = connection.prepareStatement(sql);
    fillStatement(statement, args);
    ResultSet resultSet = statement.executeQuery();
    Stream<T> stream = handler.handle(resultSet);

    //noinspection unchecked
    return (Stream<T>) Proxy.newProxyInstance(
        handler.getClass().getClassLoader(),
        new Class[]{Stream.class},
        new StreamProxyInvocationHandler(stream, connection, statement, resultSet));

  }

  /**
   * An InvocationHandler for a Stream Proxy that delegates all methods to a given
   * Stream and will close additional objects when the stream is closed.
   */
  static class StreamProxyInvocationHandler implements InvocationHandler {

    /**
     * The Stream to which all methods are delegated
     */
    private final Stream<?> stream;

    /**
     * Additional AutoCloseables that will be closed when the stream is closed
     */
    private final AutoCloseable[] closeables;

    StreamProxyInvocationHandler(Stream<?> stream, AutoCloseable... closeables) {
      this.stream = stream;
      this.closeables = closeables;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
      // If the method is "close()", then close all the Closeables
      if (method.getName().equals("close") && args == null) {
        for (AutoCloseable closeable : closeables) {
          closeable.close();
        }
      }
      return method.invoke(stream, args);
    }
  }

}
