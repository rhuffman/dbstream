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

package tech.huffman.dbstream

import org.apache.commons.dbutils.QueryRunner
import spock.lang.Specification

import javax.sql.DataSource
import java.sql.ResultSet
import java.sql.SQLException

class ResultSetIteratorTest extends Specification {

  DataSource dataSource

  QueryRunner queryRunner

  def setup() {
    dataSource = new DbTestUtility().dataSource
    queryRunner = new QueryRunner(dataSource)
    queryRunner.update("CREATE TABLE Foo (column1 VARCHAR, column2 INT)")
  }

  def cleanup() {
    queryRunner.update("DROP TABLE Foo")
  }

  def "test empty result set"() {
    given:
    def connection = dataSource.connection
    def statement = connection.prepareStatement("SELECT * FROM Foo")
    def resultSet = statement.executeQuery()
    def iterator = new ResultSetIterator<>(resultSet, { getRow() })

    expect:
    !iterator.hasNext()

    cleanup:
    resultSet?.close()
    statement?.close()
    connection?.close()
  }

  def "test basic iteration"() {
    given: "Two rows"
    insert("Pig", 1)
    insert("Cow", 2)

    and: "A ResultSet that contains the two rows"
    def connection = dataSource.connection
    def statement = connection.prepareStatement("SELECT * FROM Foo")
    def resultSet = statement.executeQuery()

    when:
    def iterator = new ResultSetIterator<>(resultSet, new Handler())

    then:
    iterator.hasNext()
    iterator.next() == new Row("Pig", 1)
    iterator.hasNext()
    iterator.next() == new Row("Cow", 2)
    !iterator.hasNext()

    cleanup:
    resultSet?.close()
    statement?.close()
    connection?.close()
  }

  def "hasNext does not advance cursor"() {
    given: "One row"
    insert("Pig", 1)

    and: "A ResultSet that returns the row"
    def connection = dataSource.connection
    def statement = connection.prepareStatement("SELECT * FROM Foo")
    def resultSet = statement.executeQuery()

    when:
    def iterator = new ResultSetIterator<>(resultSet, new Handler())

    then:
    iterator.hasNext()
    iterator.hasNext()
  }

  def "next invoked too many times"() {
    given: "Two rows"
    insert("Pig", 1)
    insert("Cow", 2)

    and: "A ResultSet that contains the two rows"
    def connection = dataSource.connection
    def statement = connection.prepareStatement("SELECT * FROM Foo")
    def resultSet = statement.executeQuery()

    when:
    def iterator = new ResultSetIterator<>(resultSet, new Handler())

    then:
    iterator.hasNext()
    iterator.hasNext()
  }

  def "test SQLException thrown by next"() {
    given: "One row"
    insert("Pig", 1)

    and: "A ResultSet that returns the row"
    def connection = dataSource.connection
    def statement = connection.prepareStatement("SELECT * FROM Foo")
    def resultSet = statement.executeQuery()
    resultSet.next()

    and: "A RowHandler that throws an exception"
    def sqlException = new SQLException("test excetion")
    def handler = new ThrowingHandler(sqlException)

    when:
    new ResultSetIterator(resultSet, handler).next()

    then:
    def e = thrown(RuntimeException)
    e.cause.is(sqlException)
  }


  def insert(String column1, int column2) {
    queryRunner.update("INSERT INTO Foo VALUES (?,?)", [column1, column2] as Object[])
  }

  static class Handler implements RowHandler<Row> {
    @Override
    Row handleRow(ResultSet resultSet) throws SQLException {
      return new Row(resultSet.getString(1), resultSet.getInt(2))
    }
  }

  static class ThrowingHandler implements RowHandler<Row> {

    private SQLException exceptionToThrow

    ThrowingHandler(SQLException exceptionToThrow) {
      this.exceptionToThrow = exceptionToThrow
    }

    @Override
    Row handleRow(ResultSet resultSet) throws SQLException {
      throw exceptionToThrow
    }
  }

  static class Row {

    String column1
    int column2

    Row(String column1, int column2) {
      this.column1 = column1
      this.column2 = column2
    }

    boolean equals(o) {
      if (this.is(o)) return true
      if (getClass() != o.class) return false

      Row row = (Row) o

      if (column2 != row.column2) return false
      if (column1 != row.column1) return false

      return true
    }

    int hashCode() {
      int result
      result = (column1 != null ? column1.hashCode() : 0)
      result = 31 * result + column2
      return result
    }
  }
}
