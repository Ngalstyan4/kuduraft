// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "kudu/util/rwc_lock.h"

#include <glog/logging.h>

namespace kudu {

RWCLock::RWCLock()
    : no_mutators_(&lock_),
      no_readers_(&lock_),
      reader_count_(0),
      write_locked_(false) {}

RWCLock::~RWCLock() {
  DCHECK(!HasReaders());
  DCHECK(!HasWriteLock());
}

void RWCLock::ReadLock() {
  MutexLock l(lock_);
  reader_count_++;
}

void RWCLock::ReadUnlock() {
  MutexLock l(lock_);
  DCHECK(HasReadersUnlocked());
  reader_count_--;
  if (reader_count_ == 0) {
    no_readers_.Signal();
  }
}

bool RWCLock::HasReaders() const {
  MutexLock l(lock_);
  return HasReadersUnlocked();
}

bool RWCLock::HasReadersUnlocked() const {
  lock_.AssertAcquired();
  return reader_count_ > 0;
}

bool RWCLock::HasWriteLock() const {
  MutexLock l(lock_);
  return HasWriteLockUnlocked();
}

bool RWCLock::HasWriteLockUnlocked() const {
  lock_.AssertAcquired();
  return write_locked_;
}

void RWCLock::WriteLock() {
  MutexLock l(lock_);
  // Wait for any other mutations to finish.
  while (write_locked_) {
    no_mutators_.Wait();
  }
  write_locked_ = true;
}

void RWCLock::WriteUnlock() {
  MutexLock l(lock_);
  DCHECK(HasWriteLockUnlocked());
  write_locked_ = false;
  no_mutators_.Signal();
}

void RWCLock::UpgradeToCommitLock() {
  lock_.lock();
  DCHECK(HasWriteLockUnlocked());
  while (reader_count_ > 0) {
    no_readers_.Wait();
  }
  DCHECK(HasWriteLockUnlocked());

  // Leaves the lock held, which prevents any new readers
  // or writers.
}

void RWCLock::CommitUnlock() {
  DCHECK(!HasReadersUnlocked());
  DCHECK(HasWriteLockUnlocked());
  write_locked_ = false;
  no_mutators_.Broadcast();
  lock_.unlock();
}

} // namespace kudu
