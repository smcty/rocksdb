// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
// This file implements the "bridge" between Java and C++ and enables
// calling c++ rocksdb3131::WriteBatchWithIndex methods from Java side.

#include "include/org_rocksdb_WBWIRocksIterator.h"
#include "include/org_rocksdb_WriteBatchWithIndex.h"
#include "rocksdb3131/comparator.h"
#include "rocksdb3131/utilities/write_batch_with_index.h"
#include "rocksjni/portal.h"

/*
 * Class:     org_rocksdb_WriteBatchWithIndex
 * Method:    newWriteBatchWithIndex
 * Signature: ()V
 */
void Java_org_rocksdb_WriteBatchWithIndex_newWriteBatchWithIndex__(
    JNIEnv* env, jobject jobj) {
  rocksdb3131::WriteBatchWithIndex* wbwi = new rocksdb3131::WriteBatchWithIndex();
  rocksdb3131::WriteBatchWithIndexJni::setHandle(env, jobj, wbwi);
}

/*
 * Class:     org_rocksdb_WriteBatchWithIndex
 * Method:    newWriteBatchWithIndex
 * Signature: (Z)V
 */
void Java_org_rocksdb_WriteBatchWithIndex_newWriteBatchWithIndex__Z(
    JNIEnv* env, jobject jobj, jboolean joverwrite_key) {
  rocksdb3131::WriteBatchWithIndex* wbwi =
      new rocksdb3131::WriteBatchWithIndex(rocksdb3131::BytewiseComparator(), 0,
      static_cast<bool>(joverwrite_key));
  rocksdb3131::WriteBatchWithIndexJni::setHandle(env, jobj, wbwi);
}

/*
 * Class:     org_rocksdb_WriteBatchWithIndex
 * Method:    newWriteBatchWithIndex
 * Signature: (JIZ)V
 */
void Java_org_rocksdb_WriteBatchWithIndex_newWriteBatchWithIndex__JIZ(
    JNIEnv* env, jobject jobj, jlong jfallback_index_comparator_handle,
    jint jreserved_bytes, jboolean joverwrite_key) {
  rocksdb3131::WriteBatchWithIndex* wbwi =
      new rocksdb3131::WriteBatchWithIndex(
      reinterpret_cast<rocksdb3131::Comparator*>(jfallback_index_comparator_handle),
      static_cast<size_t>(jreserved_bytes), static_cast<bool>(joverwrite_key));
  rocksdb3131::WriteBatchWithIndexJni::setHandle(env, jobj, wbwi);
}

/*
 * Class:     org_rocksdb_WriteBatchWithIndex
 * Method:    count
 * Signature: ()I
 */
jint Java_org_rocksdb_WriteBatchWithIndex_count0(
    JNIEnv* env, jobject jobj) {
  rocksdb3131::WriteBatchWithIndex* wbwi =
      rocksdb3131::WriteBatchWithIndexJni::getHandle(env, jobj);
  assert(wbwi != nullptr);

  return static_cast<jint>(wbwi->GetWriteBatch()->Count());
}

/*
 * Class:     org_rocksdb_WriteBatchWithIndex
 * Method:    put
 * Signature: ([BI[BI)V
 */
void Java_org_rocksdb_WriteBatchWithIndex_put___3BI_3BI(
    JNIEnv* env, jobject jobj, jbyteArray jkey, jint jkey_len,
    jbyteArray jentry_value, jint jentry_value_len) {
  auto* wbwi =
      rocksdb3131::WriteBatchWithIndexJni::getHandle(env, jobj);
  assert(wbwi != nullptr);
  auto put = [&wbwi] (rocksdb3131::Slice key, rocksdb3131::Slice value) {
    wbwi->Put(key, value);
  };
  rocksdb3131::JniUtil::kv_op(put, env, jobj, jkey, jkey_len, jentry_value,
      jentry_value_len);
}

/*
 * Class:     org_rocksdb_WriteBatchWithIndex
 * Method:    put
 * Signature: ([BI[BIJ)V
 */
void Java_org_rocksdb_WriteBatchWithIndex_put___3BI_3BIJ(
    JNIEnv* env, jobject jobj, jbyteArray jkey, jint jkey_len,
    jbyteArray jentry_value, jint jentry_value_len, jlong jcf_handle) {
  auto* wbwi =
      rocksdb3131::WriteBatchWithIndexJni::getHandle(env, jobj);
  assert(wbwi != nullptr);
  auto* cf_handle = reinterpret_cast<rocksdb3131::ColumnFamilyHandle*>(jcf_handle);
  assert(cf_handle != nullptr);
  auto put = [&wbwi, &cf_handle] (rocksdb3131::Slice key, rocksdb3131::Slice value) {
    wbwi->Put(cf_handle, key, value);
  };
  rocksdb3131::JniUtil::kv_op(put, env, jobj, jkey, jkey_len, jentry_value,
      jentry_value_len);
}

/*
 * Class:     org_rocksdb_WriteBatchWithIndex
 * Method:    merge
 * Signature: ([BI[BI)V
 */
void Java_org_rocksdb_WriteBatchWithIndex_merge___3BI_3BI(
    JNIEnv* env, jobject jobj, jbyteArray jkey, jint jkey_len,
    jbyteArray jentry_value, jint jentry_value_len) {
  auto* wbwi =
      rocksdb3131::WriteBatchWithIndexJni::getHandle(env, jobj);
  assert(wbwi != nullptr);
  auto merge = [&wbwi] (rocksdb3131::Slice key, rocksdb3131::Slice value) {
    wbwi->Merge(key, value);
  };
  rocksdb3131::JniUtil::kv_op(merge, env, jobj, jkey, jkey_len, jentry_value,
      jentry_value_len);
}

/*
 * Class:     org_rocksdb_WriteBatchWithIndex
 * Method:    merge
 * Signature: ([BI[BIJ)V
 */
void Java_org_rocksdb_WriteBatchWithIndex_merge___3BI_3BIJ(
    JNIEnv* env, jobject jobj, jbyteArray jkey, jint jkey_len,
    jbyteArray jentry_value, jint jentry_value_len, jlong jcf_handle) {
  auto* wbwi =
      rocksdb3131::WriteBatchWithIndexJni::getHandle(env, jobj);
  assert(wbwi != nullptr);
  auto* cf_handle = reinterpret_cast<rocksdb3131::ColumnFamilyHandle*>(jcf_handle);
  assert(cf_handle != nullptr);
  auto merge = [&wbwi, &cf_handle] (rocksdb3131::Slice key, rocksdb3131::Slice value) {
    wbwi->Merge(cf_handle, key, value);
  };
  rocksdb3131::JniUtil::kv_op(merge, env, jobj, jkey, jkey_len, jentry_value,
      jentry_value_len);
}

/*
 * Class:     org_rocksdb_WriteBatchWithIndex
 * Method:    remove
 * Signature: ([BI)V
 */
void Java_org_rocksdb_WriteBatchWithIndex_remove___3BI(
    JNIEnv* env, jobject jobj, jbyteArray jkey, jint jkey_len) {
  auto* wbwi =
      rocksdb3131::WriteBatchWithIndexJni::getHandle(env, jobj);
  assert(wbwi != nullptr);
  auto remove = [&wbwi] (rocksdb3131::Slice key) {
    wbwi->Delete(key);
  };
  rocksdb3131::JniUtil::k_op(remove, env, jobj, jkey, jkey_len);
}

/*
 * Class:     org_rocksdb_WriteBatchWithIndex
 * Method:    remove
 * Signature: ([BIJ)V
 */
void Java_org_rocksdb_WriteBatchWithIndex_remove___3BIJ(
    JNIEnv* env, jobject jobj,
    jbyteArray jkey, jint jkey_len, jlong jcf_handle) {
  auto* wbwi =
      rocksdb3131::WriteBatchWithIndexJni::getHandle(env, jobj);
  assert(wbwi != nullptr);
  auto* cf_handle = reinterpret_cast<rocksdb3131::ColumnFamilyHandle*>(jcf_handle);
  assert(cf_handle != nullptr);
  auto remove = [&wbwi, &cf_handle] (rocksdb3131::Slice key) {
    wbwi->Delete(cf_handle, key);
  };
  rocksdb3131::JniUtil::k_op(remove, env, jobj, jkey, jkey_len);
}

/*
 * Class:     org_rocksdb_WriteBatchWithIndex
 * Method:    putLogData
 * Signature: ([BI)V
 */
void Java_org_rocksdb_WriteBatchWithIndex_putLogData(
    JNIEnv* env, jobject jobj, jbyteArray jblob, jint jblob_len) {
  auto* wbwi =
      rocksdb3131::WriteBatchWithIndexJni::getHandle(env, jobj);
  assert(wbwi != nullptr);
  auto putLogData = [&wbwi] (rocksdb3131::Slice blob) {
    wbwi->PutLogData(blob);
  };
  rocksdb3131::JniUtil::k_op(putLogData, env, jobj, jblob, jblob_len);
}

/*
 * Class:     org_rocksdb_WriteBatchWithIndex
 * Method:    clear
 * Signature: ()V
 */
void Java_org_rocksdb_WriteBatchWithIndex_clear0(
    JNIEnv* env, jobject jobj) {
  rocksdb3131::WriteBatchWithIndex* wbwi =
      rocksdb3131::WriteBatchWithIndexJni::getHandle(env, jobj);
  assert(wbwi != nullptr);

  wbwi->GetWriteBatch()->Clear();
}

/*
 * Class:     org_rocksdb_WriteBatchWithIndex
 * Method:    iterator0
 * Signature: ()J
 */
jlong Java_org_rocksdb_WriteBatchWithIndex_iterator0(
    JNIEnv* env, jobject jobj) {
  rocksdb3131::WriteBatchWithIndex* wbwi =
      rocksdb3131::WriteBatchWithIndexJni::getHandle(env, jobj);
  rocksdb3131::WBWIIterator* wbwi_iterator = wbwi->NewIterator();
  return reinterpret_cast<jlong>(wbwi_iterator);
}

/*
 * Class:     org_rocksdb_WriteBatchWithIndex
 * Method:    iterator1
 * Signature: (J)J
 */
jlong Java_org_rocksdb_WriteBatchWithIndex_iterator1(
    JNIEnv* env, jobject jobj, jlong jcf_handle) {
  rocksdb3131::WriteBatchWithIndex* wbwi =
      rocksdb3131::WriteBatchWithIndexJni::getHandle(env, jobj);
  auto* cf_handle = reinterpret_cast<rocksdb3131::ColumnFamilyHandle*>(jcf_handle);
  rocksdb3131::WBWIIterator* wbwi_iterator = wbwi->NewIterator(cf_handle);
  return reinterpret_cast<jlong>(wbwi_iterator);
}

/*
 * Class:     org_rocksdb_WriteBatchWithIndex
 * Method:    iteratorWithBase
 * Signature: (JJ)J
 */
jlong Java_org_rocksdb_WriteBatchWithIndex_iteratorWithBase(
    JNIEnv* env, jobject jobj, jlong jcf_handle, jlong jbi_handle) {
  rocksdb3131::WriteBatchWithIndex* wbwi =
      rocksdb3131::WriteBatchWithIndexJni::getHandle(env, jobj);
  auto* cf_handle = reinterpret_cast<rocksdb3131::ColumnFamilyHandle*>(jcf_handle);
  auto* base_iterator = reinterpret_cast<rocksdb3131::Iterator*>(jbi_handle);
  auto* iterator = wbwi->NewIteratorWithBase(cf_handle, base_iterator);
  return reinterpret_cast<jlong>(iterator);
}

/*
 * Class:     org_rocksdb_WriteBatchWithIndex
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_WriteBatchWithIndex_disposeInternal(
    JNIEnv* env, jobject jobj, jlong handle) {
  auto* wbwi = reinterpret_cast<rocksdb3131::WriteBatchWithIndex*>(handle);
  delete wbwi;
}

/* WBWIRocksIterator below */

/*
 * Class:     org_rocksdb_WBWIRocksIterator
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_WBWIRocksIterator_disposeInternal(
    JNIEnv* env, jobject jobj, jlong handle) {
  auto* it = reinterpret_cast<rocksdb3131::WBWIIterator*>(handle);
  delete it;
}

/*
 * Class:     org_rocksdb_WBWIRocksIterator
 * Method:    isValid0
 * Signature: (J)Z
 */
jboolean Java_org_rocksdb_WBWIRocksIterator_isValid0(
    JNIEnv* env, jobject jobj, jlong handle) {
  return reinterpret_cast<rocksdb3131::WBWIIterator*>(handle)->Valid();
}

/*
 * Class:     org_rocksdb_WBWIRocksIterator
 * Method:    seekToFirst0
 * Signature: (J)V
 */
void Java_org_rocksdb_WBWIRocksIterator_seekToFirst0(
    JNIEnv* env, jobject jobj, jlong handle) {
  reinterpret_cast<rocksdb3131::WBWIIterator*>(handle)->SeekToFirst();
}

/*
 * Class:     org_rocksdb_WBWIRocksIterator
 * Method:    seekToLast0
 * Signature: (J)V
 */
void Java_org_rocksdb_WBWIRocksIterator_seekToLast0(
    JNIEnv* env, jobject jobj, jlong handle) {
  reinterpret_cast<rocksdb3131::WBWIIterator*>(handle)->SeekToLast();
}

/*
 * Class:     org_rocksdb_WBWIRocksIterator
 * Method:    next0
 * Signature: (J)V
 */
void Java_org_rocksdb_WBWIRocksIterator_next0(
    JNIEnv* env, jobject jobj, jlong handle) {
  reinterpret_cast<rocksdb3131::WBWIIterator*>(handle)->Next();
}

/*
 * Class:     org_rocksdb_WBWIRocksIterator
 * Method:    prev0
 * Signature: (J)V
 */
void Java_org_rocksdb_WBWIRocksIterator_prev0(
    JNIEnv* env, jobject jobj, jlong handle) {
  reinterpret_cast<rocksdb3131::WBWIIterator*>(handle)->Prev();
}

/*
 * Class:     org_rocksdb_WBWIRocksIterator
 * Method:    seek0
 * Signature: (J[BI)V
 */
void Java_org_rocksdb_WBWIRocksIterator_seek0(
    JNIEnv* env, jobject jobj, jlong handle, jbyteArray jtarget,
    jint jtarget_len) {
  auto* it = reinterpret_cast<rocksdb3131::WBWIIterator*>(handle);
  jbyte* target = env->GetByteArrayElements(jtarget, 0);
  rocksdb3131::Slice target_slice(
      reinterpret_cast<char*>(target), jtarget_len);

  it->Seek(target_slice);

  env->ReleaseByteArrayElements(jtarget, target, JNI_ABORT);
}

/*
 * Class:     org_rocksdb_WBWIRocksIterator
 * Method:    status0
 * Signature: (J)V
 */
void Java_org_rocksdb_WBWIRocksIterator_status0(
    JNIEnv* env, jobject jobj, jlong handle) {
  auto* it = reinterpret_cast<rocksdb3131::WBWIIterator*>(handle);
  rocksdb3131::Status s = it->status();

  if (s.ok()) {
    return;
  }

  rocksdb3131::RocksDBExceptionJni::ThrowNew(env, s);
}

/*
 * Class:     org_rocksdb_WBWIRocksIterator
 * Method:    entry1
 * Signature: (JLorg/rocksdb/WBWIRocksIterator/WriteEntry;)V
 */
void Java_org_rocksdb_WBWIRocksIterator_entry1(
    JNIEnv* env, jobject jobj, jlong handle, jobject jwrite_entry) {
  auto* it = reinterpret_cast<rocksdb3131::WBWIIterator*>(handle);
  const rocksdb3131::WriteEntry& we = it->Entry();
  jobject jwe = rocksdb3131::WBWIRocksIteratorJni::getWriteEntry(env, jobj);
  rocksdb3131::WriteEntryJni::setWriteType(env, jwe, we.type);

  char* buf = new char[we.key.size()];
  memcpy(buf, we.key.data(), we.key.size());
  auto* key_slice = new rocksdb3131::Slice(buf, we.key.size());
  rocksdb3131::WriteEntryJni::setKey(env, jwe, key_slice);

  if (we.type == rocksdb3131::kDeleteRecord || we.type == rocksdb3131::kLogDataRecord) {
    // set native handle of value slice to null if no value available
    rocksdb3131::WriteEntryJni::setValue(env, jwe, nullptr);
  } else {
    char* value_buf = new char[we.value.size()];
    memcpy(value_buf, we.value.data(), we.value.size());
    auto* value_slice = new rocksdb3131::Slice(value_buf, we.value.size());
    rocksdb3131::WriteEntryJni::setValue(env, jwe, value_slice);
  }
}
