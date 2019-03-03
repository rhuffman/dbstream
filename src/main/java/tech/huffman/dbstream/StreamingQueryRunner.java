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

import org.apache.commons.dbutils.AbstractQueryRunner;
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

public class StreamingQueryRunner extends AbstractQueryRunner {

  public StreamingQueryRunner(DataSource dataSource) {
    super(dataSource);
  }

  public <T> Stream<T> query(String sql, Object[] args, ResultSetHandler<Stream<T>> handler)
      throws SQLException {
    Connection connection = getDataSource().getConnection();
    PreparedStatement statement = connection.prepareStatement(sql);
    fillStatement(statement, args);
    ResultSet resultSet = statement.executeQuery();
    Stream<T> stream = handler.handle(resultSet);

    //noinspection unchecked
    return (Stream<T>) Proxy.newProxyInstance(
        handler.getClass().getClassLoader(),
        new Class[] { Stream.class },
        new StreamProxyInvocationHandler(stream, connection, statement, resultSet));

  }

  static class StreamProxyInvocationHandler implements InvocationHandler {

    private final Stream<?> stream;

    private final AutoCloseable[] closeables;

    StreamProxyInvocationHandler(Stream<?> stream, AutoCloseable...closeables) {
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
