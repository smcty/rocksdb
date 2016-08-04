// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
// This file implements the "bridge" between Java and C++ and enables
// calling c++ rocksdb3131::Env methods from Java side.

#include "include/org_rocksdb_Env.h"
#include "include/org_rocksdb_RocksEnv.h"
#include "include/org_rocksdb_RocksMemEnv.h"
#include "rocksdb3131/env.h"

/*
 * Class:     org_rocksdb_Env
 * Method:    getDefaultEnvInternal
 * Signature: ()J
 */
jlong Java_org_rocksdb_Env_getDefaultEnvInternal(
    JNIEnv* env, jclass jclazz) {
  return reinterpret_cast<jlong>(rocksdb3131::Env::Default());
}

/*
 * Class:     org_rocksdb_Env
 * Method:    setBackgroundThreads
 * Signature: (JII)V
 */
void Java_org_rocksdb_Env_setBackgroundThreads(
    JNIEnv* env, jobject jobj, jlong jhandle,
    jint num, jint priority) {
  auto* rocks_env = reinterpret_cast<rocksdb3131::Env*>(jhandle);
  switch (priority) {
    case org_rocksdb_Env_FLUSH_POOL:
      rocks_env->SetBackgroundThreads(num, rocksdb3131::Env::Priority::LOW);
      break;
    case org_rocksdb_Env_COMPACTION_POOL:
      rocks_env->SetBackgroundThreads(num, rocksdb3131::Env::Priority::HIGH);
      break;
  }
}

/*
 * Class:     org_rocksdb_sEnv
 * Method:    getThreadPoolQueueLen
 * Signature: (JI)I
 */
jint Java_org_rocksdb_Env_getThreadPoolQueueLen(
    JNIEnv* env, jobject jobj, jlong jhandle, jint pool_id) {
  auto* rocks_env = reinterpret_cast<rocksdb3131::Env*>(jhandle);
  switch (pool_id) {
    case org_rocksdb_RocksEnv_FLUSH_POOL:
      return rocks_env->GetThreadPoolQueueLen(rocksdb3131::Env::Priority::LOW);
    case org_rocksdb_RocksEnv_COMPACTION_POOL:
      return rocks_env->GetThreadPoolQueueLen(rocksdb3131::Env::Priority::HIGH);
  }
  return 0;
}

/*
 * Class:     org_rocksdb_RocksMemEnv
 * Method:    createMemEnv
 * Signature: ()J
 */
jlong Java_org_rocksdb_RocksMemEnv_createMemEnv(
    JNIEnv* env, jclass jclazz) {
  return reinterpret_cast<jlong>(rocksdb3131::NewMemEnv(
      rocksdb3131::Env::Default()));
}

/*
 * Class:     org_rocksdb_RocksMemEnv
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_RocksMemEnv_disposeInternal(
    JNIEnv* env, jobject jobj, jlong jhandle) {
  delete reinterpret_cast<rocksdb3131::Env*>(jhandle);
}
