// Copyright (c) 2014, Vlad Balan (vlad.gm@gmail.com).  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
// This file implements the "bridge" between Java and C++
// for rocksdb3131::MergeOperator.

#include <stdio.h>
#include <stdlib.h>
#include <jni.h>
#include <string>
#include <memory>

#include "include/org_rocksdb_StringAppendOperator.h"
#include "rocksjni/portal.h"
#include "rocksdb3131/db.h"
#include "rocksdb3131/options.h"
#include "rocksdb3131/statistics.h"
#include "rocksdb3131/memtablerep.h"
#include "rocksdb3131/table.h"
#include "rocksdb3131/slice_transform.h"
#include "rocksdb3131/merge_operator.h"
#include "utilities/merge_operators.h"

/*
 * Class:     org_rocksdb_StringAppendOperator
 * Method:    newMergeOperatorHandle
 * Signature: ()J
 */
jlong Java_org_rocksdb_StringAppendOperator_newMergeOperatorHandleImpl
(JNIEnv* env, jobject jobj) {
  std::shared_ptr<rocksdb3131::MergeOperator> *op =
    new std::shared_ptr<rocksdb3131::MergeOperator>();
  *op = rocksdb3131::MergeOperators::CreateFromStringId("stringappend");
  return reinterpret_cast<jlong>(op);
}
