//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef GFLAGS
#include <cstdio>
int main() {
  fprintf(stderr, "Please install gflags to run rocksdb tools\n");
  return 1;
}
#else

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include <inttypes.h>
#include <gflags/gflags.h>
#include <iostream>

#include "rocksdb3131/db.h"
#include "rocksdb3131/env.h"
#include "util/coding.h"

DEFINE_bool(anonymous, false, "Output an empty information blob.");

void usage(const char* name) {
  std::cout << "usage: " << name << " [--anonymous] <db> <dumpfile>"
            << std::endl;
}

int main(int argc, char** argv) {
  rocksdb3131::DB* dbptr;
  rocksdb3131::Options options;
  rocksdb3131::Status status;
  std::unique_ptr<rocksdb3131::WritableFile> dumpfile;
  char hostname[1024];
  int64_t timesec;
  std::string abspath;
  char json[4096];

  GFLAGS::ParseCommandLineFlags(&argc, &argv, true);

  static const char* magicstr = "ROCKDUMP";
  static const char versionstr[8] = {0, 0, 0, 0, 0, 0, 0, 1};

  if (argc != 3) {
    usage(argv[0]);
    exit(1);
  }

  rocksdb3131::Env* env = rocksdb3131::Env::Default();

  // Open the database
  options.create_if_missing = false;
  status = rocksdb3131::DB::OpenForReadOnly(options, argv[1], &dbptr);
  if (!status.ok()) {
    std::cerr << "Unable to open database '" << argv[1]
              << "' for reading: " << status.ToString() << std::endl;
    exit(1);
  }

  const std::unique_ptr<rocksdb3131::DB> db(dbptr);

  status = env->NewWritableFile(argv[2], &dumpfile, rocksdb3131::EnvOptions());
  if (!status.ok()) {
    std::cerr << "Unable to open dump file '" << argv[2]
              << "' for writing: " << status.ToString() << std::endl;
    exit(1);
  }

  rocksdb3131::Slice magicslice(magicstr, 8);
  status = dumpfile->Append(magicslice);
  if (!status.ok()) {
    std::cerr << "Append failed: " << status.ToString() << std::endl;
    exit(1);
  }

  rocksdb3131::Slice versionslice(versionstr, 8);
  status = dumpfile->Append(versionslice);
  if (!status.ok()) {
    std::cerr << "Append failed: " << status.ToString() << std::endl;
    exit(1);
  }

  if (FLAGS_anonymous) {
    snprintf(json, sizeof(json), "{}");
  } else {
    status = env->GetHostName(hostname, sizeof(hostname));
    status = env->GetCurrentTime(&timesec);
    status = env->GetAbsolutePath(argv[1], &abspath);
    snprintf(json, sizeof(json),
             "{ \"database-path\": \"%s\", \"hostname\": \"%s\", "
             "\"creation-time\": %" PRIi64 " }",
             abspath.c_str(), hostname, timesec);
  }

  rocksdb3131::Slice infoslice(json, strlen(json));
  char infosize[4];
  rocksdb3131::EncodeFixed32(infosize, (uint32_t)infoslice.size());
  rocksdb3131::Slice infosizeslice(infosize, 4);
  status = dumpfile->Append(infosizeslice);
  if (!status.ok()) {
    std::cerr << "Append failed: " << status.ToString() << std::endl;
    exit(1);
  }
  status = dumpfile->Append(infoslice);
  if (!status.ok()) {
    std::cerr << "Append failed: " << status.ToString() << std::endl;
    exit(1);
  }

  const std::unique_ptr<rocksdb3131::Iterator> it(
      db->NewIterator(rocksdb3131::ReadOptions()));
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    char keysize[4];
    rocksdb3131::EncodeFixed32(keysize, (uint32_t)it->key().size());
    rocksdb3131::Slice keysizeslice(keysize, 4);
    status = dumpfile->Append(keysizeslice);
    if (!status.ok()) {
      std::cerr << "Append failed: " << status.ToString() << std::endl;
      exit(1);
    }
    status = dumpfile->Append(it->key());
    if (!status.ok()) {
      std::cerr << "Append failed: " << status.ToString() << std::endl;
      exit(1);
    }

    char valsize[4];
    rocksdb3131::EncodeFixed32(valsize, (uint32_t)it->value().size());
    rocksdb3131::Slice valsizeslice(valsize, 4);
    status = dumpfile->Append(valsizeslice);
    if (!status.ok()) {
      std::cerr << "Append failed: " << status.ToString() << std::endl;
      exit(1);
    }
    status = dumpfile->Append(it->value());
    if (!status.ok()) {
      std::cerr << "Append failed: " << status.ToString() << std::endl;
      exit(1);
    }
  }
  if (!it->status().ok()) {
    std::cerr << "Database iteration failed: " << status.ToString()
              << std::endl;
    exit(1);
  }

  return 0;
}

#endif  // GFLAGS
