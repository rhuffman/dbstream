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

import org.apache.commons.dbutils.ResultSetHandler;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Abstract class that simplify development of <code>ResultSetHandler</code>
 * classes that convert <code>ResultSet</code> into <code>Stream</code>.
 *
 * @param <T> the target Stream generic type
 * @see org.apache.commons.dbutils.ResultSetHandler
 */
public abstract class AbstractStreamHandler<T> implements ResultSetHandler<Stream<T>> {

  /**
   * Whole <code>ResultSet</code> handler. It produces a <code>Stream</code> as
   * the result. To convert individual rows into Java objects it uses  the
   * <code>handleRow(ResultSet)</code> method.
   *
   * @param resultSet The <code>ResultSet</code> to process.
   * @return a <code>Stream</code> of all rows in the result set, each row
   * converted to T
   * @throws SQLException error occurs
   * @see #handleRow(ResultSet)
   */
  @Override
  public Stream<T> handle(ResultSet resultSet) throws SQLException {
    ResultSetIterator<T> iterator = new ResultSetIterator<>(resultSet, this::handleRow);
    Stream stream = StreamSupport.stream(
        Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED),
        false);
    return stream;
  }

  /**
   * Row handler. Method converts current row into some Java object.
   *
   * @param rs <code>ResultSet</code> to process.
   * @return row processing result
   * @throws SQLException error occurs
   */
  protected abstract T handleRow(ResultSet rs) throws SQLException;
}
