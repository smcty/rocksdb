//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#pragma once

#include "rocksdb3131/status.h"
#include "rocksdb3131/iterator.h"

namespace rocksdb3131 {

// Seek to the properties block.
// If it successfully seeks to the properties block, "is_found" will be
// set to true.
Status SeekToPropertiesBlock(Iterator* meta_iter, bool* is_found);

}  // namespace rocksdb3131
