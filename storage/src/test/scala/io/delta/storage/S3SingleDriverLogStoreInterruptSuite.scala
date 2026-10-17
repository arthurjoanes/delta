/*
 * Copyright (2026) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.delta.storage

import java.io.IOException
import java.nio.charset.StandardCharsets.UTF_8
import java.util.Collections

import org.scalatest.funsuite.AnyFunSuite

class S3SingleDriverLogStoreInterruptSuite extends AnyFunSuite {
  test("an interrupted writer does not release another writer's lock") {
    val conf = LogStoreLockTestUtils.configuration()
    val path = LogStoreLockTestUtils.logPath(conf)
    val store = new S3SingleDriverLogStore(conf)
    LogStoreLockTestUtils.checkInterruptedWaiter(classOf[S3SingleDriverLogStore], path) {
      store.write(path, Collections.emptyList[String]().iterator(), false, conf)
    }
  }

  test("successful writes and I/O failures release the path lock") {
    val conf = LogStoreLockTestUtils.configuration()
    val fs = new InMemoryLogFileSystem
    val path = LogStoreLockTestUtils.logPath(conf, fs)
    val store = new S3SingleDriverLogStore(conf)
    store.write(path, Collections.singletonList("action").iterator(), true, conf)
    assert(new String(fs.contents.toByteArray, UTF_8) == "action\n")
    LogStoreLockTestUtils.checkLockReleased(classOf[S3SingleDriverLogStore], path)
    fs.failWrites = true
    intercept[IOException] {
      store.write(path, Collections.emptyList[String]().iterator(), true, conf)
    }
    LogStoreLockTestUtils.checkLockReleased(classOf[S3SingleDriverLogStore], path)
  }
}
