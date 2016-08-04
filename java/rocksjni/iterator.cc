// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
// This file implements the "bridge" between Java and C++ and enables
// calling c++ rocksdb3131::Iterator methods from Java side.

#include <stdio.h>
#include <stdlib.h>
#include <jni.h>

#include "include/org_rocksdb_RocksIterator.h"
#include "rocksjni/portal.h"
#include "rocksdb3131/iterator.h"

/*
 * Class:     org_rocksdb_RocksIterator
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_RocksIterator_disposeInternal(
    JNIEnv* env, jobject jobj, jlong handle) {
  auto it = reinterpret_cast<rocksdb3131::Iterator*>(handle);
  delete it;
}

/*
 * Class:     org_rocksdb_RocksIterator
 * Method:    isValid0
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_RocksIterator_isValid0(
    JNIEnv* env, jobject jobj, jlong handle) {
  return reinterpret_cast<rocksdb3131::Iterator*>(handle)->Valid();
}

/*
 * Class:     org_rocksdb_RocksIterator
 * Method:    seekToFirst0
 * Signature: (J)V
 */
void Java_org_rocksdb_RocksIterator_seekToFirst0(
    JNIEnv* env, jobject jobj, jlong handle) {
  reinterpret_cast<rocksdb3131::Iterator*>(handle)->SeekToFirst();
}

/*
 * Class:     org_rocksdb_RocksIterator
 * Method:    seekToLast0
 * Signature: (J)V
 */
void Java_org_rocksdb_RocksIterator_seekToLast0(
    JNIEnv* env, jobject jobj, jlong handle) {
  reinterpret_cast<rocksdb3131::Iterator*>(handle)->SeekToLast();
}

/*
 * Class:     org_rocksdb_RocksIterator
 * Method:    next0
 * Signature: (J)V
 */
void Java_org_rocksdb_RocksIterator_next0(
    JNIEnv* env, jobject jobj, jlong handle) {
  reinterpret_cast<rocksdb3131::Iterator*>(handle)->Next();
}

/*
 * Class:     org_rocksdb_RocksIterator
 * Method:    prev0
 * Signature: (J)V
 */
void Java_org_rocksdb_RocksIterator_prev0(
    JNIEnv* env, jobject jobj, jlong handle) {
  reinterpret_cast<rocksdb3131::Iterator*>(handle)->Prev();
}

/*
 * Class:     org_rocksdb_RocksIterator
 * Method:    seek0
 * Signature: (J[BI)V
 */
void Java_org_rocksdb_RocksIterator_seek0(
    JNIEnv* env, jobject jobj, jlong handle,
    jbyteArray jtarget, jint jtarget_len) {
  auto it = reinterpret_cast<rocksdb3131::Iterator*>(handle);
  jbyte* target = env->GetByteArrayElements(jtarget, 0);
  rocksdb3131::Slice target_slice(
      reinterpret_cast<char*>(target), jtarget_len);

  it->Seek(target_slice);

  env->ReleaseByteArrayElements(jtarget, target, JNI_ABORT);
}

/*
 * Class:     org_rocksdb_RocksIterator
 * Method:    status0
 * Signature: (J)V
 */
void Java_org_rocksdb_RocksIterator_status0(
    JNIEnv* env, jobject jobj, jlong handle) {
  auto it = reinterpret_cast<rocksdb3131::Iterator*>(handle);
  rocksdb3131::Status s = it->status();

  if (s.ok()) {
    return;
  }

  rocksdb3131::RocksDBExceptionJni::ThrowNew(env, s);
}

/*
 * Class:     org_rocksdb_RocksIterator
 * Method:    key0
 * Signature: (J)[B
 */
jbyteArray Java_org_rocksdb_RocksIterator_key0(
    JNIEnv* env, jobject jobj, jlong handle) {
  auto it = reinterpret_cast<rocksdb3131::Iterator*>(handle);
  rocksdb3131::Slice key_slice = it->key();

  jbyteArray jkey = env->NewByteArray(static_cast<jsize>(key_slice.size()));
  env->SetByteArrayRegion(jkey, 0, static_cast<jsize>(key_slice.size()),
                          reinterpret_cast<const jbyte*>(key_slice.data()));
  return jkey;
}

/*
 * Class:     org_rocksdb_RocksIterator
 * Method:    value0
 * Signature: (J)[B
 */
jbyteArray Java_org_rocksdb_RocksIterator_value0(
    JNIEnv* env, jobject jobj, jlong handle) {
  auto it = reinterpret_cast<rocksdb3131::Iterator*>(handle);
  rocksdb3131::Slice value_slice = it->value();

  jbyteArray jkeyValue =
      env->NewByteArray(static_cast<jsize>(value_slice.size()));
  env->SetByteArrayRegion(jkeyValue, 0, static_cast<jsize>(value_slice.size()),
                          reinterpret_cast<const jbyte*>(value_slice.data()));
  return jkeyValue;
}
