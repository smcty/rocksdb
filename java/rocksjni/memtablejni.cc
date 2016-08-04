// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
// This file implements the "bridge" between Java and C++ for MemTables.

#include "rocksjni/portal.h"
#include "include/org_rocksdb_HashSkipListMemTableConfig.h"
#include "include/org_rocksdb_HashLinkedListMemTableConfig.h"
#include "include/org_rocksdb_VectorMemTableConfig.h"
#include "include/org_rocksdb_SkipListMemTableConfig.h"
#include "rocksdb3131/memtablerep.h"

/*
 * Class:     org_rocksdb_HashSkipListMemTableConfig
 * Method:    newMemTableFactoryHandle
 * Signature: (JII)J
 */
jlong Java_org_rocksdb_HashSkipListMemTableConfig_newMemTableFactoryHandle(
    JNIEnv* env, jobject jobj, jlong jbucket_count,
    jint jheight, jint jbranching_factor) {
  rocksdb3131::Status s = rocksdb3131::check_if_jlong_fits_size_t(jbucket_count);
  if (s.ok()) {
    return reinterpret_cast<jlong>(rocksdb3131::NewHashSkipListRepFactory(
        static_cast<size_t>(jbucket_count),
        static_cast<int32_t>(jheight),
        static_cast<int32_t>(jbranching_factor)));
  }
  rocksdb3131::IllegalArgumentExceptionJni::ThrowNew(env, s);
  return 0;
}

/*
 * Class:     org_rocksdb_HashLinkedListMemTableConfig
 * Method:    newMemTableFactoryHandle
 * Signature: (JJIZI)J
 */
jlong Java_org_rocksdb_HashLinkedListMemTableConfig_newMemTableFactoryHandle(
    JNIEnv* env, jobject jobj, jlong jbucket_count, jlong jhuge_page_tlb_size,
    jint jbucket_entries_logging_threshold,
    jboolean jif_log_bucket_dist_when_flash, jint jthreshold_use_skiplist) {
  rocksdb3131::Status statusBucketCount =
      rocksdb3131::check_if_jlong_fits_size_t(jbucket_count);
  rocksdb3131::Status statusHugePageTlb =
      rocksdb3131::check_if_jlong_fits_size_t(jhuge_page_tlb_size);
  if (statusBucketCount.ok() && statusHugePageTlb.ok()) {
    return reinterpret_cast<jlong>(rocksdb3131::NewHashLinkListRepFactory(
        static_cast<size_t>(jbucket_count),
        static_cast<size_t>(jhuge_page_tlb_size),
        static_cast<int32_t>(jbucket_entries_logging_threshold),
        static_cast<bool>(jif_log_bucket_dist_when_flash),
        static_cast<int32_t>(jthreshold_use_skiplist)));
  }
  rocksdb3131::IllegalArgumentExceptionJni::ThrowNew(env,
      !statusBucketCount.ok()?statusBucketCount:statusHugePageTlb);
  return 0;
}

/*
 * Class:     org_rocksdb_VectorMemTableConfig
 * Method:    newMemTableFactoryHandle
 * Signature: (J)J
 */
jlong Java_org_rocksdb_VectorMemTableConfig_newMemTableFactoryHandle(
    JNIEnv* env, jobject jobj, jlong jreserved_size) {
  rocksdb3131::Status s = rocksdb3131::check_if_jlong_fits_size_t(jreserved_size);
  if (s.ok()) {
    return reinterpret_cast<jlong>(new rocksdb3131::VectorRepFactory(
        static_cast<size_t>(jreserved_size)));
  }
  rocksdb3131::IllegalArgumentExceptionJni::ThrowNew(env, s);
  return 0;
}

/*
 * Class:     org_rocksdb_SkipListMemTableConfig
 * Method:    newMemTableFactoryHandle0
 * Signature: (J)J
 */
jlong Java_org_rocksdb_SkipListMemTableConfig_newMemTableFactoryHandle0(
    JNIEnv* env, jobject jobj, jlong jlookahead) {
  rocksdb3131::Status s = rocksdb3131::check_if_jlong_fits_size_t(jlookahead);
  if (s.ok()) {
    return reinterpret_cast<jlong>(new rocksdb3131::SkipListFactory(
        static_cast<size_t>(jlookahead)));
  }
  rocksdb3131::IllegalArgumentExceptionJni::ThrowNew(env, s);
  return 0;
}
