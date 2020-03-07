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

import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

import javax.sql.DataSource
import java.util.stream.Collectors

import static tech.huffman.dbstream.DbTestUtility.createDataSource

class StreamingQueryRunnerTest extends Specification {

  def dataSource = DataSourceProxy.proxyDataSource(createDataSource())

  def queryRunner = new StreamingQueryRunner(dataSource as DataSource)

  @Shared
  def arrayHandler = new ArrayStreamingHandler()

  @Shared
  def mapHandler = new MapStreamingHandler()

  @Shared
  def beanHandler = new BeanStreamingHandler<>(Animal)

  def setup() {
    queryRunner.execute("CREATE TABLE Foo (NAME varchar, NUMBER int)")
  }

  def cleanup() {
    queryRunner.execute("DROP TABLE Foo")
  }

  @Unroll
  def "test stream count with empty result set: #handlerType"() {
    when:
    def stream = queryRunner.queryStream("SELECT NAME, NUMBER FROM Foo", handler)

    then:
    stream.count() == 0

    cleanup:
    stream?.close()

    where:
    handler      | _
    arrayHandler | _
    mapHandler   | _
    beanHandler  | _

    handlerType = handler.class.simpleName
  }

  @Unroll
  def "test stream iteration with empty result set: #handlerType"() {
    when:
    def stream = queryRunner.queryStream("SELECT NAME, NUMBER FROM Foo", handler)

    then:
    stream.each {}

    cleanup:
    stream?.close()

    where:
    handler      | _
    arrayHandler | _
    mapHandler   | _
    beanHandler  | _

    handlerType = handler.class.simpleName
  }

  @Unroll
  def "test ResultSet with one row: #handlerType"() {
    given:
    queryRunner.execute("INSERT INTO FOO VALUES (?, ?)", "Pig", 42)

    when:
    def stream = queryRunner.queryStream("SELECT NAME, NUMBER FROM Foo", handler)

    then:
    stream.collect(Collectors.toList()) == [expected]

    cleanup:
    stream?.close()

    where:
    handler      | expected
    arrayHandler | ['Pig', 42] as Object[]
    mapHandler   | [NAME: 'Pig', NUMBER: 42]
    beanHandler  | new Animal(name: 'Pig', number: 42)

    handlerType = handler.class.simpleName
  }

  @Unroll
  def "test ResultSet with multiple rows: #handlerType"() {
    given:
    queryRunner.execute("INSERT INTO FOO VALUES (?, ?)", "Pig", 42)
    queryRunner.execute("INSERT INTO FOO VALUES (?, ?)", "Cow", 84)
    queryRunner.execute("INSERT INTO FOO VALUES (?, ?)", "Dog", 92)

    when:
    def stream = queryRunner.queryStream("SELECT NAME, NUMBER FROM Foo", handler)

    then:
    stream.collect(Collectors.toList()) == expected

    cleanup:
    stream?.close()

    where:
    handler      | expected
    arrayHandler | [['Pig', 42] as Object[], ['Cow', 84] as Object[], ['Dog', 92] as Object[]]
    mapHandler   | [[NAME: 'Pig', NUMBER: 42], [NAME: 'Cow', NUMBER: 84], [NAME: 'Dog', NUMBER: 92]]
    beanHandler  | [new Animal(name: 'Pig', number: 42),
                    new Animal(name: 'Cow', number: 84),
                    new Animal(name: 'Dog', number: 92)]

    handlerType = handler.class.simpleName
  }

  @SuppressWarnings("GroovyAccessibility")
  def "test closing stream closes database connection"() {
    when:
    dataSource.reset()
    def stream = queryRunner.queryStream("SELECT NUMBER FROM Foo", arrayHandler)

    then:
    dataSource.connections.size() == 1
    !dataSource.connections[0].isClosed()

    when:
    stream.close()

    then:
    dataSource.connections.size() == 1
    dataSource.connections[0].isClosed()

    cleanup:
    stream?.close()
  }

  @SuppressWarnings("GroovyAccessibility")
  def "test connection is not closed when passed to the QueryRunner"() {
    when:
    def connection = dataSource.getConnection()
    def stream = queryRunner.queryStream(connection, "SELECT NUMBER FROM Foo", arrayHandler, null)
    stream.close()

    then:
    !connection.isClosed()

    cleanup:
    stream?.close()
    connection?.close()
  }

  static class Animal {

    String name

    int number

    boolean equals(o) {
      if (this.is(o)) return true
      if (getClass() != o.class) return false

      Animal animal = (Animal) o

      if (number != animal.number) return false
      if (name != animal.name) return false

      return true
    }

    int hashCode() {
      int result
      result = (name != null ? name.hashCode() : 0)
      result = 31 * result + number
      return result
    }


    @Override
    String toString() {
      return "Animal{" +
          "name='" + name + '\'' +
          ", number=" + number +
          '}'
    }
  }

}
