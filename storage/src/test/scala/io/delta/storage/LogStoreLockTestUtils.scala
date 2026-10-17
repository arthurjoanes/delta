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

import java.io.{ByteArrayOutputStream, File, InterruptedIOException, IOException}
import java.util.UUID
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}

import io.delta.storage.internal.PathLock
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, FSDataOutputStream, Path, RawLocalFileSystem}

private[storage] object LogStoreLockTestUtils {
  def configuration(): Configuration = {
    val conf = new Configuration(false)
    conf.set("fs.file.impl", classOf[RawLocalFileSystem].getName)
    conf.setBoolean("fs.file.impl.disable.cache", true)
    conf
  }

  def logPath(conf: Configuration): Path = {
    val file = new File(System.getProperty("java.io.tmpdir"),
      s"delta-lock-${UUID.randomUUID()}/_delta_log/00000000000000000000.json")
    val path = new Path(file.toURI)
    path.getFileSystem(conf).makeQualified(path)
  }

  def logPath(conf: Configuration, fs: FileSystem): Path = {
    new Path(logPath(conf).toUri) {
      override def getFileSystem(conf: Configuration): FileSystem = fs
    }
  }

  private def lockFor(owner: Class[_]): PathLock = {
    val field = owner.getDeclaredField("pathLock")
    field.setAccessible(true)
    field.get(null).asInstanceOf[PathLock]
  }

  private def awaitWaiter(thread: Thread, acquired: AtomicBoolean): Unit = {
    val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10)
    while (thread.isAlive && thread.getState != Thread.State.WAITING && !acquired.get() &&
        System.nanoTime() < deadline) {
      Thread.`yield`()
    }
  }

  def checkInterruptedWaiter(owner: Class[_], path: Path)(operation: => Unit): Unit = {
    val lock = lockFor(owner)
    val failure = new AtomicReference[Throwable]()
    val thirdAcquired = new AtomicBoolean(false)
    val waiter = new Thread(() => {
      try {
        operation
      } catch {
        case error: Throwable => failure.set(error)
      }
    })
    val third = new Thread(() => {
      try {
        lock.acquire(path)
        thirdAcquired.set(true)
      } catch {
        case _: InterruptedException => ()
      } finally {
        if (thirdAcquired.get()) lock.release(path)
      }
    })
    waiter.setDaemon(true)
    third.setDaemon(true)
    lock.acquire(path)
    try {
      waiter.start()
      awaitWaiter(waiter, new AtomicBoolean(false))
      assert(waiter.getState == Thread.State.WAITING, "writer never waited on the held lock")
      waiter.interrupt()
      waiter.join(TimeUnit.SECONDS.toMillis(10))
      assert(!waiter.isAlive, "interrupted lock waiter did not terminate")
      assert(failure.get().isInstanceOf[InterruptedIOException],
        s"unexpected error: ${failure.get()}")
      third.start()
      awaitWaiter(third, thirdAcquired)
      assert(!thirdAcquired.get(), "a third writer entered while the original owner held the lock")
      assert(third.getState == Thread.State.WAITING, "third writer did not wait on the held lock")
    } finally {
      waiter.interrupt()
      waiter.join(TimeUnit.SECONDS.toMillis(10))
      third.interrupt()
      third.join(TimeUnit.SECONDS.toMillis(10))
      // Only the original owner may release the lock. An interrupted waiter must leave it intact.
      if (!thirdAcquired.get()) lock.release(path)
    }
    lock.acquire(path)
    lock.release(path)
  }

  def checkLockReleased(owner: Class[_], path: Path): Unit = {
    val lock = lockFor(owner)
    val acquired = new AtomicBoolean(false)
    val probe = new Thread(() => {
      try {
        lock.acquire(path)
        acquired.set(true)
      } catch {
        case _: InterruptedException => ()
      } finally {
        if (acquired.get()) lock.release(path)
      }
    })
    probe.setDaemon(true)
    probe.start()
    probe.join(TimeUnit.SECONDS.toMillis(10))
    probe.interrupt()
    probe.join(TimeUnit.SECONDS.toMillis(10))
    assert(acquired.get(), "finished operation retained the path lock")
  }
}

private[storage] class InMemoryLogFileSystem extends RawLocalFileSystem {
  val contents = new ByteArrayOutputStream()
  var failWrites = false

  override def exists(path: Path): Boolean = false

  override def create(path: Path, overwrite: Boolean): FSDataOutputStream = {
    if (failWrites) throw new IOException("injected create failure")
    new FSDataOutputStream(contents, null)
  }

  override def listStatus(path: Path): Array[FileStatus] = Array.empty
}
