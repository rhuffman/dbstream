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

import spock.lang.Specification

import javax.sql.DataSource

class BeanStreamingHandlerTest extends Specification {

  DataSource dataSource = new DbTestUtility().createDataSource()

  StreamingQueryRunner queryRunner = new StreamingQueryRunner(dataSource)

  BeanStreamingHandler<Animal> handler = new BeanStreamingHandler<>(Animal)

  def setup() {
    queryRunner.execute("CREATE TABLE Foo (NAME VARCHAR, COUNT INT)")
  }

  def cleanup() {
    queryRunner.execute("DROP TABLE Foo")
  }

  def "test handleRow"() {
    given:
    queryRunner.execute("INSERT INTO Foo VALUES (?, ?)", "Pig", 42)
    def connection = dataSource.getConnection()
    def statement = connection.createStatement()
    def resultSet = statement.executeQuery("SELECT * FROM Foo")

    expect:
    resultSet.next()
    def actual = handler.handleRow(resultSet)
    actual.name == 'Pig'
    actual.count == 42
  }

  static class Animal {

    String name

    int count

  }
}
