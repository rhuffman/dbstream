/*
 * Copyright 2020 Robert Huffman
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

package tech.huffman.dbstream

import javax.sql.DataSource
import java.sql.CallableStatement
import java.sql.Connection
import java.sql.SQLException
import java.sql.Statement

@SuppressWarnings("unused")
class DataSourceProxy extends groovy.util.Proxy {

  static DataSourceProxy proxyDataSource(DataSource dataSource) {
    return new DataSourceProxy().wrap(dataSource) as DataSourceProxy
  }

  final List<Connection> connections = new ArrayList<>()
  final List<Statement> statements = new ArrayList<>()

  Connection getConnection() throws SQLException {
    def connection = (adaptee as DataSource).getConnection()
    connections.add(connection)
    return connection
  }

  Connection getConnection(String username, String password) throws SQLException {
    def connection = (adaptee as DataSource).getConnection(username, password)
    connections.add(connection)
    return connection
  }

  void reset() {
    connections.clear()
  }

  class ConnectionProxy extends groovy.util.Proxy {

    Statement prepareStatement(int resultSetType, int resultSetConcurrency) {
      def statement = (adaptee as Connection).createStatement(resultSetType, resultSetConcurrency)
      statements.add(statement)
      return statement
    }

  }
}
