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

/**
 * ResultSetHandler implementation that converts the ResultSet into a Stream of Object[]s.
 *
 * @see org.apache.commons.dbutils.ResultSetHandler
 */

public class ArrayStreamHandler extends AbstractStreamHandler<Object[]> {

  @Override
  protected Object[] handleRow(ResultSet resultSet) throws SQLException {
    int columnCount = resultSet.getMetaData().getColumnCount();
    Object[] result = new Object[columnCount];
    for (int i = 0; i < columnCount; i++) {
      result[i] = resultSet.getObject(i+1);
    }
    return result;
  }

}
