/* Copyright 2016 The TensorFlow Authors. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
==============================================================================*/

#include "tensorflow/core/platform/kvsns/kvsns_file_system.h"

#include "tensorflow/core/lib/core/status_test_util.h"
#include "tensorflow/core/lib/gtl/stl_util.h"
#include "tensorflow/core/lib/io/path.h"
#include "tensorflow/core/lib/strings/str_util.h"
#include "tensorflow/core/platform/file_system.h"
#include "tensorflow/core/platform/test.h"

namespace tensorflow {
namespace {

class KVSNSFileSystemTest : public ::testing::Test {
 protected:
  KVSNSFileSystemTest() {}

  string TmpDir(const string& path) {
    char* test_dir = getenv("KVSNS_TEST_TMPDIR");
    if (test_dir != nullptr) {
      return io::JoinPath(string(test_dir), path);
    } else {
      return "file://" + io::JoinPath(testing::TmpDir(), path);
    }
  }

  Status WriteString(const string& fname, const string& content) {
    std::unique_ptr<WritableFile> writer;
    TF_RETURN_IF_ERROR(kvsns.NewWritableFile(fname, &writer));
    TF_RETURN_IF_ERROR(writer->Append(content));
    TF_RETURN_IF_ERROR(writer->Close());
    return Status::OK();
  }

  Status ReadAll(const string& fname, string* content) {
    std::unique_ptr<RandomAccessFile> reader;
    TF_RETURN_IF_ERROR(kvsns.NewRandomAccessFile(fname, &reader));

    uint64 file_size = 0;
    TF_RETURN_IF_ERROR(kvsns.GetFileSize(fname, &file_size));

    content->resize(file_size);
    StringPiece result;
    TF_RETURN_IF_ERROR(
        reader->Read(0, file_size, &result, gtl::string_as_array(content)));
    if (file_size != result.size()) {
      return errors::DataLoss("expected ", file_size, " got ", result.size(),
                              " bytes");
    }
    return Status::OK();
  }

  KVSNSFileSystem kvsns;
};

TEST_F(KVSNSFileSystemTest, RandomAccessFile) {
  const string fname = TmpDir("RandomAccessFile");
  const string content = "abcdefghijklmn";

  TF_ASSERT_OK(WriteString(fname, content));

  std::unique_ptr<RandomAccessFile> reader;
  TF_EXPECT_OK(kvsns.NewRandomAccessFile(fname, &reader));

  string got;
  got.resize(content.size());
  StringPiece result;
  TF_EXPECT_OK(
      reader->Read(0, content.size(), &result, gtl::string_as_array(&got)));
  EXPECT_EQ(content.size(), result.size());
  EXPECT_EQ(content, result);

  got.clear();
  got.resize(4);
  TF_EXPECT_OK(reader->Read(2, 4, &result, gtl::string_as_array(&got)));
  EXPECT_EQ(4, result.size());
  EXPECT_EQ(content.substr(2, 4), result);
}

TEST_F(KVSNSFileSystemTest, WritableFile) {
  std::unique_ptr<WritableFile> writer;
  const string fname = TmpDir("WritableFile");
  TF_EXPECT_OK(kvsns.NewWritableFile(fname, &writer));
  TF_EXPECT_OK(writer->Append("content1,"));
  TF_EXPECT_OK(writer->Append("content2"));
  TF_EXPECT_OK(writer->Flush());
  TF_EXPECT_OK(writer->Sync());
  TF_EXPECT_OK(writer->Close());

  string content;
  TF_EXPECT_OK(ReadAll(fname, &content));
  EXPECT_EQ("content1,content2", content);
}

TEST_F(KVSNSFileSystemTest, FileExists) {
  const string fname = TmpDir("FileExists");
  EXPECT_EQ(error::Code::NOT_FOUND, kvsns.FileExists(fname).code());
  TF_ASSERT_OK(WriteString(fname, "test"));
  TF_EXPECT_OK(kvsns.FileExists(fname));
}

TEST_F(KVSNSFileSystemTest, GetChildren) {
  const string base = TmpDir("GetChildren");
  TF_EXPECT_OK(kvsns.CreateDir(base));

  const string file = io::JoinPath(base, "testfile.csv");
  TF_EXPECT_OK(WriteString(file, "blah"));
  const string subdir = io::JoinPath(base, "subdir");
  TF_EXPECT_OK(kvsns.CreateDir(subdir));

  std::vector<string> children;
  TF_EXPECT_OK(kvsns.GetChildren(base, &children));
  std::sort(children.begin(), children.end());
  EXPECT_EQ(std::vector<string>({"subdir", "testfile.csv"}), children);
}

TEST_F(KVSNSFileSystemTest, DeleteFile) {
  const string fname = TmpDir("DeleteFile");
  EXPECT_FALSE(kvsns.DeleteFile(fname).ok());
  TF_ASSERT_OK(WriteString(fname, "test"));
  TF_EXPECT_OK(kvsns.DeleteFile(fname));
}

TEST_F(KVSNSFileSystemTest, GetFileSize) {
  const string fname = TmpDir("GetFileSize");
  TF_ASSERT_OK(WriteString(fname, "test"));
  uint64 file_size = 0;
  TF_EXPECT_OK(kvsns.GetFileSize(fname, &file_size));
  EXPECT_EQ(4, file_size);
}

TEST_F(KVSNSFileSystemTest, CreateDirStat) {
  const string dir = TmpDir("CreateDirStat");
  TF_EXPECT_OK(kvsns.CreateDir(dir));
  FileStatistics stat;
  TF_EXPECT_OK(kvsns.Stat(dir, &stat));
  EXPECT_TRUE(stat.is_directory);
}

TEST_F(KVSNSFileSystemTest, DeleteDir) {
  const string dir = TmpDir("DeleteDir");
  EXPECT_FALSE(kvsns.DeleteDir(dir).ok());
  TF_EXPECT_OK(kvsns.CreateDir(dir));
  TF_EXPECT_OK(kvsns.DeleteDir(dir));
  FileStatistics stat;
  EXPECT_FALSE(kvsns.Stat(dir, &stat).ok());
}

TEST_F(KVSNSFileSystemTest, RenameFile) {
  const string fname1 = TmpDir("RenameFile1");
  const string fname2 = TmpDir("RenameFile2");
  TF_ASSERT_OK(WriteString(fname1, "test"));
  TF_EXPECT_OK(kvsns.RenameFile(fname1, fname2));
  string content;
  TF_EXPECT_OK(ReadAll(fname2, &content));
  EXPECT_EQ("test", content);
}

TEST_F(KVSNSFileSystemTest, RenameFile_Overwrite) {
  const string fname1 = TmpDir("RenameFile1");
  const string fname2 = TmpDir("RenameFile2");

  TF_ASSERT_OK(WriteString(fname2, "test"));
  TF_EXPECT_OK(kvsns.FileExists(fname2));

  TF_ASSERT_OK(WriteString(fname1, "test"));
  TF_EXPECT_OK(kvsns.RenameFile(fname1, fname2));
  string content;
  TF_EXPECT_OK(ReadAll(fname2, &content));
  EXPECT_EQ("test", content);
}

TEST_F(KVSNSFileSystemTest, StatFile) {
  const string fname = TmpDir("StatFile");
  TF_ASSERT_OK(WriteString(fname, "test"));
  FileStatistics stat;
  TF_EXPECT_OK(kvsns.Stat(fname, &stat));
  EXPECT_EQ(4, stat.length);
  EXPECT_FALSE(stat.is_directory);
}

TEST_F(KVSNSFileSystemTest, WriteWhileReading) {
  std::unique_ptr<WritableFile> writer;
  const string fname = TmpDir("WriteWhileReading");
  // Skip the test if we're not testing on HDFS. KVSNS's local filesystem
  // implementation makes no guarantees that writable files are readable while
  // being written.
  if (!str_util::StartsWith(fname, "kvsns://")) {
    return;
  }

  TF_EXPECT_OK(kvsns.NewWritableFile(fname, &writer));

  const string content1 = "content1";
  TF_EXPECT_OK(writer->Append(content1));
  TF_EXPECT_OK(writer->Flush());

  std::unique_ptr<RandomAccessFile> reader;
  TF_EXPECT_OK(kvsns.NewRandomAccessFile(fname, &reader));

  string got;
  got.resize(content1.size());
  StringPiece result;
  TF_EXPECT_OK(
      reader->Read(0, content1.size(), &result, gtl::string_as_array(&got)));
  EXPECT_EQ(content1, result);

  string content2 = "content2";
  TF_EXPECT_OK(writer->Append(content2));
  TF_EXPECT_OK(writer->Flush());

  got.resize(content2.size());
  TF_EXPECT_OK(reader->Read(content1.size(), content2.size(), &result,
                            gtl::string_as_array(&got)));
  EXPECT_EQ(content2, result);

  TF_EXPECT_OK(writer->Close());
}

// NewAppendableFile() is not testable. Local filesystem maps to
// ChecksumFileSystem in KVSNS, where appending is an unsupported operation.

}  // namespace
}  // namespace tensorflow
