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

import java.util.stream.Stream

class StreamProxyInvocationHandlerTest extends Specification {

  def "closing stream closes additional AutoCloseables()"() {
    given:
    def closeable1 = new TestCloseable()
    def closeable2 = new TestCloseable()
    Stream<?> stream = Collections.emptyList().stream()
    def proxy = java.lang.reflect.Proxy.newProxyInstance(
        Stream.getClass().getClassLoader(),
        [Stream.class] as Class[],
        new StreamingQueryRunner.StreamProxyInvocationHandler(stream, closeable1, closeable2))

    when:
    proxy.close()

    then:
    closeable1.isClosed
    closeable2.isClosed

  }

  private static class TestCloseable implements AutoCloseable {

    boolean isClosed = false

    @Override
    void close() throws Exception {
      isClosed = true
    }
  }
}
