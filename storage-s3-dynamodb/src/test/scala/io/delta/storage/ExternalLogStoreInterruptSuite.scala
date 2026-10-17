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
import java.util.{Collections, Optional}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest.funsuite.AnyFunSuite

class ExternalLogStoreInterruptSuite extends AnyFunSuite {
  private class Store(conf: Configuration, entry: ExternalCommitEntry)
      extends BaseExternalLogStore(conf) {
    var failRecovery = false
    var completed = false

    override protected def putExternalEntry(
        entry: ExternalCommitEntry, overwrite: Boolean): Unit = {
      completed = entry.complete
    }

    override protected def fixDeltaLogCopyTempFile(fs: FileSystem,
        src: Path, dst: Path): Unit = {
      if (failRecovery) throw new IOException("injected recovery failure")
    }

    override protected def getExternalEntry(
        tablePath: String, fileName: String): Optional[ExternalCommitEntry] = Optional.empty()

    override protected def getLatestExternalEntry(
        tablePath: Path): Optional[ExternalCommitEntry] = Optional.of(entry)
  }

  private def checkInterruptedOperation(recover: Boolean): Unit = {
    val conf = LogStoreLockTestUtils.configuration()
    val path = LogStoreLockTestUtils.logPath(conf)
    val entry = new ExternalCommitEntry(
      path.getParent.getParent, path.getName, ".tmp/test.json", false, null)
    val store = new Store(conf, entry)
    LogStoreLockTestUtils.checkInterruptedWaiter(classOf[BaseExternalLogStore], path) {
      if (recover) {
        store.listFrom(path, conf)
      } else {
        store.write(path, Collections.emptyList[String]().iterator(), false, conf)
      }
    }
  }

  test("an interrupted external writer does not release another writer's lock") {
    checkInterruptedOperation(recover = false)
  }

  test("an interrupted recovery does not release another writer's lock") {
    checkInterruptedOperation(recover = true)
  }

  test("successful external writes and I/O failures release the path lock") {
    val conf = LogStoreLockTestUtils.configuration()
    val fs = new InMemoryLogFileSystem
    val path = LogStoreLockTestUtils.logPath(conf, fs)
    val store = new Store(conf, null)
    store.write(path, Collections.singletonList("action").iterator(), true, conf)
    assert(new String(fs.contents.toByteArray, UTF_8) == "action\n")
    LogStoreLockTestUtils.checkLockReleased(classOf[BaseExternalLogStore], path)
    fs.failWrites = true
    intercept[IOException] {
      store.write(path, Collections.emptyList[String]().iterator(), true, conf)
    }
    LogStoreLockTestUtils.checkLockReleased(classOf[BaseExternalLogStore], path)
  }

  test("successful recovery and I/O failures release the path lock") {
    val conf = LogStoreLockTestUtils.configuration()
    val fs = new InMemoryLogFileSystem
    val path = LogStoreLockTestUtils.logPath(conf, fs)
    val entry = new ExternalCommitEntry(
      path.getParent.getParent, path.getName, ".tmp/test.json", false, null)
    val store = new Store(conf, entry)
    store.listFrom(path, conf)
    assert(store.completed)
    LogStoreLockTestUtils.checkLockReleased(classOf[BaseExternalLogStore], path)
    store.failRecovery = true
    intercept[IOException] {
      store.listFrom(path, conf)
    }
    LogStoreLockTestUtils.checkLockReleased(classOf[BaseExternalLogStore], path)
  }
}
