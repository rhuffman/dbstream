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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;

class ResultSetIterator<T> implements Iterator<T> {

  private final ResultSet resultSet;

  private final RowHandler<T> rowHandler;

  private boolean hasNext;

  ResultSetIterator(ResultSet resultSet, RowHandler<T> rowHandler) throws SQLException {
    this.resultSet = resultSet;
    this.rowHandler = rowHandler;
    hasNext = resultSet.next();
  }

  @Override
  public boolean hasNext() {
      return hasNext;
  }

  @Override
  public T next() {
    try {
      T result = rowHandler.handleRow(resultSet);
      hasNext = resultSet.next();
      return result;
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

}
