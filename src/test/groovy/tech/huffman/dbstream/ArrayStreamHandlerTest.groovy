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
import java.util.stream.Collectors
import java.util.stream.Stream

class ArrayStreamHandlerTest extends Specification {

  DataSource dataSource = new DbTestUtility().createDataSource()

  QueryRunner queryRunner = new QueryRunner(dataSource)

  StreamingQueryRunner streamingQueryRunner = new StreamingQueryRunner(dataSource)

  ArrayStreamHandler handler = new ArrayStreamHandler()

  def "test empty ResultSet"() {
    given:
    queryRunner.execute("CREATE TABLE Foo (i int)")

    when:
    Stream<Object[]> stream = streamingQueryRunner.query("SELECT i FROM Foo", null, handler)

    then:
    stream.count() == 0

    cleanup:
    stream?.close()
    queryRunner.execute("DROP TABLE Foo")
  }

  def "test ResultSet with one row"() {
    given:
    queryRunner.execute("CREATE TABLE Foo (i int)")
    queryRunner.execute("INSERT INTO FOO VALUES (?)", [42] as Object[])

    when:
    ArrayStreamHandler handler = new ArrayStreamHandler()
    Stream<Object[]> stream = streamingQueryRunner.query("SELECT i FROM Foo", null, handler)

    then:
    stream.collect(Collectors.toList()) == [ [42] as Object[] ]

    cleanup:
    stream?.close()
    queryRunner.execute("DROP TABLE Foo")
  }

}
