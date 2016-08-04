// Copyright (c) 2015, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
// This file implements the "bridge" between Java and C++ for
// rocksdb3131::CompactionFilter.

#include <jni.h>

#include "rocksdb3131/compaction_filter.h"

// <editor-fold desc="org.rocksdb.AbstractCompactionFilter">

/*
 * Class:     org_rocksdb_AbstractCompactionFilter
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_AbstractCompactionFilter_disposeInternal(
    JNIEnv* env, jobject jobj, jlong handle) {
  delete reinterpret_cast<rocksdb3131::CompactionFilter*>(handle);
}
// </editor-fold>
