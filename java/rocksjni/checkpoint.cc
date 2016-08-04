// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
// This file implements the "bridge" between Java and C++ and enables
// calling c++ rocksdb3131::Checkpoint methods from Java side.

#include <stdio.h>
#include <stdlib.h>
#include <jni.h>
#include <string>

#include "include/org_rocksdb_Checkpoint.h"
#include "rocksjni/portal.h"
#include "rocksdb3131/db.h"
#include "rocksdb3131/utilities/checkpoint.h"
/*
 * Class:     org_rocksdb_Checkpoint
 * Method:    newCheckpoint
 * Signature: (J)J
 */
jlong Java_org_rocksdb_Checkpoint_newCheckpoint(JNIEnv* env,
    jclass jclazz, jlong jdb_handle) {
  auto db = reinterpret_cast<rocksdb3131::DB*>(jdb_handle);
  rocksdb3131::Checkpoint* checkpoint;
  rocksdb3131::Checkpoint::Create(db, &checkpoint);
  return reinterpret_cast<jlong>(checkpoint);
}

/*
 * Class:     org_rocksdb_Checkpoint
 * Method:    dispose
 * Signature: (J)V
 */
void Java_org_rocksdb_Checkpoint_disposeInternal(JNIEnv* env, jobject jobj,
    jlong jhandle) {
  auto checkpoint = reinterpret_cast<rocksdb3131::Checkpoint*>(jhandle);
  assert(checkpoint);
  delete checkpoint;
}

/*
 * Class:     org_rocksdb_Checkpoint
 * Method:    createCheckpoint
 * Signature: (JLjava/lang/String;)V
 */
void Java_org_rocksdb_Checkpoint_createCheckpoint(
    JNIEnv* env, jobject jobj, jlong jcheckpoint_handle,
    jstring jcheckpoint_path) {
  auto checkpoint = reinterpret_cast<rocksdb3131::Checkpoint*>(
      jcheckpoint_handle);
  const char* checkpoint_path = env->GetStringUTFChars(
      jcheckpoint_path, 0);
  rocksdb3131::Status s = checkpoint->CreateCheckpoint(
      checkpoint_path);
  env->ReleaseStringUTFChars(jcheckpoint_path, checkpoint_path);
  if (!s.ok()) {
      rocksdb3131::RocksDBExceptionJni::ThrowNew(env, s);
  }
}
