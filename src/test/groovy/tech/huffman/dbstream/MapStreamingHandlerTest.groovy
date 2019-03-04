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
import java.util.stream.Collectors
import java.util.stream.Stream

class MapStreamingHandlerTest extends Specification {

  DataSource dataSource = new DbTestUtility().createDataSource()

  StreamingQueryRunner queryRunner = new StreamingQueryRunner(dataSource)

  MapStreamingHandler handler = new MapStreamingHandler()

  def setup() {
    queryRunner.execute("CREATE TABLE Foo (NAME VARCHAR, COUNT INT)")
  }

  def cleanup() {
    queryRunner.execute("DROP TABLE Foo")
  }

  def "test empty ResultSet"() {
    when:
    Stream<Map<String, Object>> stream = queryRunner.queryAsStream(
        "SELECT NAME, COUNT FROM Foo", handler, null)

    then:
    stream.count() == 0

    cleanup:
    stream?.close()
  }

  def "test ResultSet with one row"() {
    given:
    queryRunner.execute("INSERT INTO Foo VALUES (?, ?)", ["Pig", 3] as Object[])

    when:
    MapStreamingHandler handler = new MapStreamingHandler()
    Stream<Map<String, Object>> stream = queryRunner.queryAsStream(
        "SELECT NAME, COUNT FROM Foo", handler, null)

    then:
    stream.collect(Collectors.toList()) == [[NAME: 'Pig', COUNT: 3]]

    cleanup:
    stream?.close()
  }

  def "test ResultSet with multiple rows"() {
    given:
    queryRunner.execute("INSERT INTO Foo VALUES (?, ?)", ["Pig", 3] as Object[])
    queryRunner.execute("INSERT INTO Foo VALUES (?, ?)", ["Cow", 4] as Object[])
    queryRunner.execute("INSERT INTO Foo VALUES (?, ?)", ["Dog", 2] as Object[])

    when:
    MapStreamingHandler handler = new MapStreamingHandler()
    Stream<Map<String, Object>> stream = queryRunner.queryAsStream(
        "SELECT NAME, COUNT FROM Foo", handler, null)

    then:
    stream.collect(Collectors.toList()) == [[NAME: 'Pig', COUNT: 3],
                                            [NAME: 'Cow', COUNT: 4],
                                            [NAME: 'Dog', COUNT: 2]]

    cleanup:
    stream?.close()
  }
}
