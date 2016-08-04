//  Copyright (c) 2015, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#pragma once
#ifndef ROCKSDB_LITE

#include "rocksdb3131/db.h"
#include "rocksdb3131/options.h"
#include "rocksdb3131/utilities/optimistic_transaction_db.h"

namespace rocksdb3131 {

class OptimisticTransactionDBImpl : public OptimisticTransactionDB {
 public:
  explicit OptimisticTransactionDBImpl(DB* db)
      : OptimisticTransactionDB(db), db_(db) {}

  ~OptimisticTransactionDBImpl() {}

  OptimisticTransaction* BeginTransaction(
      const WriteOptions& write_options,
      const OptimisticTransactionOptions& txn_options) override;

  DB* GetBaseDB() override { return db_.get(); }

 private:
  std::unique_ptr<DB> db_;
};

}  //  namespace rocksdb3131
#endif  // ROCKSDB_LITE
