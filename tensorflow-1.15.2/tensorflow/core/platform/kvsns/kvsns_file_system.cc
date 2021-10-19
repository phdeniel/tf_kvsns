/* Copyright 2015 The TensorFlow Authors. All Rights Reserved.

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

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>
#include <libgen.h>

#include "tensorflow/core/lib/core/error_codes.pb.h"
#include "tensorflow/core/lib/io/path.h"
#include "tensorflow/core/lib/strings/str_util.h"
#include "tensorflow/core/lib/core/status.h"
#include "tensorflow/core/lib/strings/strcat.h"
#include "tensorflow/core/platform/env.h"
#include "tensorflow/core/platform/file_system_helper.h"
#include "tensorflow/core/platform/logging.h"
#include "tensorflow/core/platform/kvsns/kvsns_file_system.h"

//#define NO_DEBUG

namespace tensorflow {

template <typename R, typename... Args>
Status BindFunc(void* handle, const char* name,
                std::function<R(Args...)>* func) {
  void* symbol_ptr = nullptr;
  TF_RETURN_IF_ERROR(
      Env::Default()->GetSymbolFromLibrary(handle, name, &symbol_ptr));
  *func = reinterpret_cast<R (*)(Args...)>(symbol_ptr);
  return Status::OK();
}

class LibKVSNS {
 public:
  static LibKVSNS* Load() {
#ifndef NO_DEBUG
    std::cerr << "Calling LibKVSNS::Load\n";
#endif
    static LibKVSNS* lib = []() -> LibKVSNS* {
      LibKVSNS* lib = new LibKVSNS;
      lib->LoadAndBind();
      return lib;
    }();

    return lib;
  }

  std::function<int(char *)> kvsns_start;
  std::function<int(kvsns_cred_t *cred, kvsns_ino_t *ino, int flags)> kvsns_access;
  std::function<int(kvsns_cred_t *cred, kvsns_ino_t *parent, char *name,
	            mode_t mode, kvsns_ino_t *newdir)> kvsns_mkdir;
  std::function<int(kvsns_cred_t *cred, kvsns_ino_t *parent,
		    char *name)>kvsns_rmdir;
  std::function<int(kvsns_cred_t *cred, kvsns_ino_t *parent,
		    char *name)>kvsns_unlink;
  std::function<int(kvsns_cred_t *cred, kvsns_ino_t *ino,
		   int flags, mode_t mode, kvsns_file_open_t *fd)>kvsns_open;
  std::function<int(kvsns_file_open_t *fd)>kvsns_close;
  std::function<ssize_t(kvsns_cred_t *cred, kvsns_file_open_t *fd,
		        void *buf, size_t count, off_t offset)>kvsns_read;
  std::function<ssize_t(kvsns_cred_t *cred, kvsns_file_open_t *fd,
		        void *buf, size_t count, off_t offset)>kvsns_write;
  std::function<int(kvsns_cred_t *cred, kvsns_ino_t *ino,
                    struct stat *buffstat)>kvsns_getattr;
  std::function<int(kvsns_cred_t *cred, kvsns_ino_t *sino, char *sname,
		    kvsns_ino_t *dino, char *dname)>kvsns_rename;
  std::function<int(kvsns_cred_t *cred, kvsns_ino_t *parent, char *name,
		    kvsns_ino_t *myino)>kvsns_lookup;
  std::function<int(kvsns_cred_t *cred, kvsns_ino_t *parent, char *name,
		   mode_t mode, kvsns_ino_t *newino)>kvsns_creat;
  std::function<int (kvsns_cred_t *cred, kvsns_ino_t *dir, kvsns_dir_t *ddir)>kvsns_opendir;
  std::function<int (kvsns_cred_t *cred, kvsns_dir_t *dir, off_t offset,
		     kvsns_dentry_t *dirent, int *size)>kvsns_readdir;
  std::function<int (kvsns_dir_t *ddir)>kvsns_closedir;


  Status status() { return status_; }

  int my_kvsns_lookup_path(kvsns_cred_t *cred, char *path,
                           kvsns_ino_t *ino)
  {
        char *saveptr;
        char sstr[MAXPATHLEN];
	char *str;
        char *token;
	kvsns_ino_t parent = KVSNS_ROOT_INODE;
        kvsns_ino_t iter_ino = 0LL;
        kvsns_ino_t *iter = NULL;
        int j = 0;
        int rc = 0;

        memcpy(&iter_ino, &parent, sizeof(kvsns_ino_t));
        iter = &iter_ino;
	strncpy(sstr, path, MAXPATHLEN);
        for (j = 1, str = sstr; ; j++, str = NULL) {
                memcpy(&parent, ino, sizeof(kvsns_ino_t));
                token = strtok_r(str, "/", &saveptr);
                if (token == NULL)
                        break;

#ifndef NO_DEBUG
    std::cerr << "kvsns_lookup " << *iter << " " << token << "\n";
#endif

                rc = kvsns_lookup(cred, iter, token, ino);
#ifndef NO_DEBUG
    std::cerr << "kvsns_lookup rc =" << rc << "\n";
#endif
                if (rc != 0) {
                        if (rc == -ENOENT)
                                break;
                        else
                                return rc;
                }

                iter = ino;
        }

        if (token != NULL) /* If non-existing file should be created */
                strcpy(path, token);

        return rc;
  }


  // The status, if any, from failure to load.
 private:
  void LoadAndBind() {
#ifndef NO_DEBUG
    std::cerr << "Calling LibKVSNS::LoadAndBind\n";
#endif
    auto TryLoadAndBind = [this](const char* name, void** handle) -> Status {
      TF_RETURN_IF_ERROR(Env::Default()->LoadLibrary(name, handle));
#define BIND_KVSNS_FUNC(function) \
  TF_RETURN_IF_ERROR(BindFunc(*handle, #function, &function));

       BIND_KVSNS_FUNC(kvsns_start);
       BIND_KVSNS_FUNC(kvsns_access);
       BIND_KVSNS_FUNC(kvsns_mkdir);
       BIND_KVSNS_FUNC(kvsns_rmdir);
       BIND_KVSNS_FUNC(kvsns_unlink);
       BIND_KVSNS_FUNC(kvsns_open);
       BIND_KVSNS_FUNC(kvsns_close);
       BIND_KVSNS_FUNC(kvsns_read);
       BIND_KVSNS_FUNC(kvsns_write);
       BIND_KVSNS_FUNC(kvsns_getattr);
       BIND_KVSNS_FUNC(kvsns_rename);
       BIND_KVSNS_FUNC(kvsns_lookup);
       BIND_KVSNS_FUNC(kvsns_creat);
       BIND_KVSNS_FUNC(kvsns_opendir);
       BIND_KVSNS_FUNC(kvsns_readdir);
       BIND_KVSNS_FUNC(kvsns_closedir);

#undef BIND_HDFS_FUNC
      return Status::OK();
    };

    const char* pathlib = "/usr/lib64/libkvsns.so";
    // Try to load the library dynamically in case it has been installed
    // to a in non-standard location.
    status_ = TryLoadAndBind(pathlib, &handle_);
  }

  Status status_;
  void* handle_ = nullptr;
};


Status ParseKVSNSPath(const string& fname, char* path)
{
  StringPiece scheme, dirp, pathp;
  string fname_copy;

  fname_copy = fname;

#ifndef NO_DEBUG
  std::cerr << "ParseKVSNSPath(" << fname_copy << ")\n";
#endif

  if (!path) {
    return errors::Internal("bucket and object cannot be null.");
  }
  io::ParseURI(fname_copy, &scheme, &dirp, &pathp);
  if (scheme != "kvsns") {
    return errors::InvalidArgument("KVSNS path doesn't start with 'kvsns://': ",
                                   fname_copy);
  }
#ifndef NO_DEBUG
  std::cerr << "scheme=" << scheme << " dir=" << dirp << " pathp=" << pathp << "\n";
#endif

  strncpy(path, (char *)(fname_copy.c_str() + 8), MAXPATHLEN); // remove kvsns://

#ifndef NO_DEBUG
  std::cerr << "  Found path=" << path << "\n";
#endif

  return Status::OK();
}

Status my_basename(const string& fname, char* name)
{
  string fname_copy;

  fname_copy = fname;
  strncpy(name, basename((char *)(fname_copy.c_str() + 8)), MAXNAMLEN); 

#ifndef NO_DEBUG
  std::cerr << "  Found basename=" << name << "\n";
#endif
  return Status::OK();
}

Status my_dirpath(const string& fname, char* path)
{
  string fname_copy;

  fname_copy = fname;
  strncpy(path, dirname((char *)(fname_copy.c_str() + 8)), MAXPATHLEN); 

#ifndef NO_DEBUG
  std::cerr << "  Found dirname=" << path << "\n";
#endif
  return Status::OK();
}

Status RcToStatus(int rc, string str)
{
#ifndef NO_DEBUG
  std::cerr << " RcToStatus " << str << " rc = " << rc << "\n";
#endif
	if (rc > 0)
		return Status::OK();

	switch(-rc) {
	case EPERM:
		return errors::PermissionDenied(str.c_str());
		break;
	case ENOENT:
		return errors::NotFound(str);
		break;
	case EINVAL:
		return errors::InvalidArgument(str.c_str());
		break;
	case EEXIST:
		return errors::AlreadyExists(str.c_str());
	default:
		return errors::Unknown(str.c_str());
		break;
	}
}

KVSNSFileSystem::KVSNSFileSystem() : kvsns_(LibKVSNS::Load())
{
	int rc;
#ifndef NO_DEBUG
	std::cerr << "Calling KVSNSFileSystem::KVSNSFileSystem\n";
#endif
	rc = kvsns_->kvsns_start((char *)KVSNS_DEFAULT_CONFIG);
	if (rc != 0) {
		std::cerr << "kvsns_start: err=" << rc << "\n";
		exit(1);
	}
#ifndef NO_DEBUG
	std::cerr << "Call to kvsns_start()succeeded rc=" << rc << "\n";
#endif
	cred_.uid = getuid();
	cred_.gid = getgid();
}


KVSNSFileSystem::~KVSNSFileSystem() {}


class KVSNSRandomAccessFile : public RandomAccessFile {
 public:
  KVSNSRandomAccessFile(LibKVSNS *kvsns,
		        const kvsns_cred_t *cred, 
		  	kvsns_file_open_t *fd,
			const string& fname) : kvsns_(kvsns)
  {
	  memcpy(&cred_, cred, sizeof(kvsns_cred_t));
	  memcpy(&fd_, fd, sizeof(kvsns_file_open_t));
	  filename_ = fname;
	  kvsns_ = kvsns;
  }

  ~KVSNSRandomAccessFile() override
  {
	  kvsns_->kvsns_close(&fd_);
  }

  Status Read(uint64 offset, size_t n, StringPiece* result,
              char* scratch) const override {
#ifndef NO_DEBUG
    std::cerr << "Calling KVSNSRandomAccessFile::Read\n";
#endif
    Status s;
    char *dst = scratch;
    while (n > 0 && s.ok()) {
      ssize_t r = kvsns_->kvsns_read((kvsns_cred_t *)&cred_, 
		      	     (kvsns_file_open_t *)&fd_,
			     dst, n, offset);
      if (r > 0) {
        dst += r;
        n -= r;
        offset += r;
      } else if (r == 0) {
        s = Status(error::OUT_OF_RANGE, "Read less bytes than requested");
      } else if (errno == EINTR || errno == EAGAIN) {
        // Retry
      } else {
	RcToStatus(r, filename_);
      }
    }
    *result = StringPiece(scratch, dst - scratch);
    return s;
  }

 private:
  kvsns_file_open_t fd_;
  kvsns_cred_t cred_;
  string filename_;
  LibKVSNS *kvsns_;
};

class KVSNSWritableFile : public WritableFile {
 public:
  KVSNSWritableFile(LibKVSNS *kvsns,
		    const kvsns_cred_t *cred,
                    kvsns_file_open_t *fd,
                    const string& fname) : kvsns_(kvsns)
  {
	  memcpy(&cred_, cred, sizeof(kvsns_cred_t));
	  memcpy(&fd_, fd, sizeof(kvsns_file_open_t));
	  filename_ = fname;
	  kvsns_ = kvsns;
  }

  ~KVSNSWritableFile() override 
  {
	  kvsns_->kvsns_close(&fd_);
  }

  Status Append(StringPiece data) override {
    struct stat stat;
    int rc;

#ifndef NO_DEBUG
    std::cerr << "Calling KVSNSWritableFile::Append\n";
#endif
    rc = kvsns_->kvsns_getattr((kvsns_cred_t *)&cred_,
		       (kvsns_ino_t *)&fd_.ino, 
		       &stat);

    if (rc < 0)
	return RcToStatus(rc, filename_);

    size_t r = kvsns_->kvsns_write((kvsns_cred_t *)&cred_,
			   (kvsns_file_open_t *)&fd_,
                           (void *)data.data(),
			   data.size(),
			   stat.st_size);
    if (r != data.size()) {
      return RcToStatus(r, filename_);
    }
    return Status::OK();
  }
    
  Status Close() override {
#ifndef NO_DEBUG
    std::cerr << "Calling KVSNSWritableFile::Close\n";
#endif
    int rc = kvsns_->kvsns_close(&fd_);
    if (rc <  0)
	    return RcToStatus(rc, filename_);

    return Status::OK();
  }

  Status Flush() override 
  { 
#ifndef NO_DEBUG
    std::cerr << "Calling KVSNSWritableFile::Flush\n";
#endif
    return Status::OK();
  }

  Status Sync() override 
  {
#ifndef NO_DEBUG
    std::cerr << "Calling KVSNSWritableFile::Sync\n";
#endif
    return Status::OK();
  }

 private:
  kvsns_file_open_t fd_;
  kvsns_cred_t cred_;
  string filename_;
  LibKVSNS *kvsns_;
};

Status KVSNSFileSystem::NewReadOnlyMemoryRegionFromFile(
    const string& fname, std::unique_ptr<ReadOnlyMemoryRegion>* result) {
#ifndef NO_DEBUG
	std::cerr << "Calling KVSNSFileSystem::NewReadOnlyMemoryRegionFromFile\n";
#endif
        return errors::Unimplemented("KVSNS does not support ReadOnlyMemoryRegion");
}

Status KVSNSFileSystem::NewRandomAccessFile(const string& fname, 
		          std::unique_ptr<tensorflow::RandomAccessFile>* result)
{
	char path[MAXPATHLEN];
	int rc;
	kvsns_ino_t ino;
	kvsns_file_open_t fd;

#ifndef NO_DEBUG
	std::cerr << "Calling KVSNSFileSystem::NewRandomAccessFile\n";
	std::cerr << "   Name = " << fname << "\n";
#endif

	TF_RETURN_IF_ERROR(ParseKVSNSPath(fname, path));

	rc = kvsns_->my_kvsns_lookup_path(&cred_,
				          path, &ino);
	if (rc != 0)
		return RcToStatus(rc, fname);

	rc = kvsns_->kvsns_open(&cred_, &ino, O_RDONLY,
				0777, &fd);
	if (rc != 0)
		return RcToStatus(rc, fname);

	result->reset(new KVSNSRandomAccessFile(kvsns_, &cred_,
						&fd, fname));
	return Status::OK();
}

Status KVSNSFileSystem::NewWritableFile(const string& fname,
                           std::unique_ptr<WritableFile>* result)
{
	int rc = 0;
	kvsns_ino_t ino;
	kvsns_ino_t dino;
	kvsns_file_open_t fd;
	char path[MAXPATHLEN];
	char dirpath[MAXPATHLEN];
	char filename[MAXNAMLEN];

#ifndef NO_DEBUG
	std::cerr << "Calling KVSNSFileSystem::NewWritableFile\n";
	std::cerr << "   Name = " << fname << "\n";
#endif

	TF_RETURN_IF_ERROR(ParseKVSNSPath(fname, path));

	my_basename(fname, filename);
	my_dirpath(fname, dirpath);

	rc = kvsns_->my_kvsns_lookup_path(&cred_,
				          path, &ino);

#ifndef NO_DEBUG
	std::cerr << "my_kvsns_lookup_pathi file rc =" << rc << "\n";
	std::cerr << "   Name = " << fname << "\n";
#endif
	if (rc != 0) {
		if (rc == -ENOENT) {
			rc = kvsns_->my_kvsns_lookup_path(&cred_, 
				       			  dirpath,
							  &dino);
#ifndef NO_DEBUG
	std::cerr << "my_kvsns_lookup_path dir rc =" << rc << "\n";
	std::cerr << "   Name = " << fname << "\n";
#endif
			if (rc != 0)
				return RcToStatus(rc, fname);

			rc = kvsns_->kvsns_creat(&cred_,
					         &dino, 
					         filename,
					         0777,
					         &ino);
			if (rc != 0)
				return RcToStatus(rc, fname);
		} else return RcToStatus(rc, fname);
	}

	rc = kvsns_->kvsns_open(&cred_, &ino, O_RDWR,
				0777, &fd);
	if (rc != 0)
		return RcToStatus(rc, fname);

	result->reset(new KVSNSWritableFile(kvsns_, &cred_,
					    &fd, fname));

	
	return Status::OK();
}

Status KVSNSFileSystem::NewAppendableFile(const string& fname,
                                       std::unique_ptr<WritableFile>* result)
{
	char path[MAXPATHLEN];
	int rc;
	kvsns_ino_t ino;
	kvsns_file_open_t fd;

#ifndef NO_DEBUG
	std::cerr << "Calling KVSNSFileSystem::NewAppendableFile\n";
	std::cerr << "   Name = " << fname << "\n";
#endif

        TF_RETURN_IF_ERROR(ParseKVSNSPath(fname, path));

        rc = kvsns_->my_kvsns_lookup_path(&cred_,
                                          path, &ino);
        if (rc != 0)
                return RcToStatus(rc, fname);

        rc = kvsns_->kvsns_open(&cred_, &ino, O_RDWR,
                                0777, &fd);
        if (rc != 0)
                return RcToStatus(rc, fname);

        result->reset(new KVSNSWritableFile(kvsns_, &cred_,
                                            &fd, fname));
        return Status::OK();
}

Status KVSNSFileSystem::FileExists(const string& fname)
{
	char path[MAXPATHLEN];
	kvsns_ino_t ino;
	int rc;

#ifndef NO_DEBUG
	std::cerr << "Calling KVSNSFileSystem::FileExists\n";
	std::cerr << "   Name = " << fname << "\n";
#endif

        TF_RETURN_IF_ERROR(ParseKVSNSPath(fname, path));

        rc = kvsns_->my_kvsns_lookup_path(&cred_,
                                          path, &ino);
        if (rc != 0)
                return RcToStatus(rc, fname);

	return Status::OK();
}

Status KVSNSFileSystem::GetChildren(const string& dir,
                                 std::vector<string>* result)
{
        char path[MAXPATHLEN];
        kvsns_ino_t ino;
	off_t offset;
	int size;
	kvsns_dentry_t dirent[10];
	kvsns_dir_t dirfd;
	int i;
	int rc;


#ifndef NO_DEBUG
	std::cerr << "Calling KVSNSFileSystem::GetChildren\n";
	std::cerr << "   Name = " << dir << "\n";
#endif

        TF_RETURN_IF_ERROR(ParseKVSNSPath(dir, path));

        rc = kvsns_->my_kvsns_lookup_path(&cred_,
                                          path, &ino);
        if (rc != 0)
                return RcToStatus(rc, dir);

	rc = kvsns_->kvsns_opendir(&cred_,
				   &ino,
				   &dirfd);
        if (rc != 0)
                return RcToStatus(rc, dir);

	offset = 0;
	do {
		size = 10;
		rc = kvsns_->kvsns_readdir(&cred_,
					   &dirfd,
					   offset,
					   dirent,
					   &size);
		if (rc != 0) 
                	return RcToStatus(rc, dir);
		
		for (i = 0; i < size; i++)
      			result->push_back(dirent[i].name);
		
		offset += size;

	} while (size != 0);

	rc = kvsns_->kvsns_closedir(&dirfd);
	if (rc != 0)
                return RcToStatus(rc, dir);

	return Status::OK();
}

Status KVSNSFileSystem::Stat(const string& fname, FileStatistics* stats)
{
        char path[MAXPATHLEN];
        kvsns_ino_t ino;
	struct stat sbuf;
        int rc;

#ifndef NO_DEBUG
	std::cerr << "Calling KVSNSFileSystem::Stat\n";
	std::cerr << "   Name = " << fname << "\n";
#endif

        TF_RETURN_IF_ERROR(ParseKVSNSPath(fname, path));

        rc = kvsns_->my_kvsns_lookup_path(&cred_,
                                          path, &ino);
        if (rc != 0)
                return RcToStatus(rc, fname);

	rc = kvsns_->kvsns_getattr(&cred_, &ino, &sbuf);
        if (rc != 0)
                return RcToStatus(rc, fname);

	stats->length = sbuf.st_size;
	stats->mtime_nsec = sbuf.st_mtime * 1e9;
	stats->is_directory = S_ISDIR(sbuf.st_mode);

	return Status::OK();
}

Status KVSNSFileSystem::GetMatchingPaths(const string& pattern,
                                      std::vector<string>* results)
{
#ifndef NO_DEBUG
	std::cerr << "Calling KVSNSFileSystem::GetMatchingPaths\n";
#endif
	return internal::GetMatchingPaths(this, Env::Default(), pattern, results);
}

Status KVSNSFileSystem::DeleteFile(const string& fname)
{
	char path[MAXPATHLEN];
	char dir[MAXPATHLEN];
	char name[MAXNAMLEN];

	kvsns_ino_t ino;
	int rc;

#ifndef NO_DEBUG
	std::cerr << "Calling KVSNSFileSystem::DeleteFile\n";
	std::cerr << "   Name = " << fname << "\n";
#endif

        TF_RETURN_IF_ERROR(ParseKVSNSPath(fname, path));
	my_dirpath(fname, dir);
	my_basename(fname, name);

        rc = kvsns_->my_kvsns_lookup_path(&cred_,
                                          dir,
					  &ino);
        if (rc != 0)
                return RcToStatus(rc, fname);

	rc = kvsns_->kvsns_unlink(&cred_,
		       		  &ino,
				  name);

        if (rc != 0)
                return RcToStatus(rc, fname);

	return Status::OK();
}

Status KVSNSFileSystem::CreateDir(const string& dname)
{
	char path[MAXPATHLEN];
	char dirpath[MAXPATHLEN];
	char name[MAXNAMLEN];
	kvsns_ino_t ino;
	kvsns_ino_t new_ino;
	int rc;

#ifndef NO_DEBUG
	std::cerr << "Calling KVSNSFileSystem::CreateDir\n";
	std::cerr << "   Name = " << dname << "\n";
#endif

        TF_RETURN_IF_ERROR(ParseKVSNSPath(dname, path));
	my_dirpath(dname, dirpath);
	my_basename(dname, name);

        rc = kvsns_->my_kvsns_lookup_path(&cred_,
                                          dirpath,
					  &ino);
        if (rc != 0)
                return RcToStatus(rc, dname);

	rc = kvsns_->kvsns_mkdir(&cred_,
				 &ino, 
				 name,
				 0755,
				 &new_ino);
        if (rc != 0)
                return RcToStatus(rc, dname);

	return Status::OK();
}

Status KVSNSFileSystem::DeleteDir(const string& dname)
{
        char path[MAXPATHLEN];
	char dirpath[MAXPATHLEN];
	char name[MAXNAMLEN];

        kvsns_ino_t ino;
        int rc;

#ifndef NO_DEBUG
	std::cerr << "Calling KVSNSFileSystem::DeleteDir\n";
	std::cerr << "   Name = " << dname << "\n";
#endif

        TF_RETURN_IF_ERROR(ParseKVSNSPath(dname, path));
	my_dirpath(dname, dirpath);
	my_basename(dname, name);

        rc = kvsns_->my_kvsns_lookup_path(&cred_,
                                          dirpath, &ino);
        if (rc != 0)
                return RcToStatus(rc, dname);

        rc = kvsns_->kvsns_rmdir(&cred_, &ino, name);
        if (rc != 0)
                return RcToStatus(rc, dname);

	return Status::OK();
}

Status KVSNSFileSystem::GetFileSize(const string& fname, uint64* file_size)
{
        char path[MAXPATHLEN];
        kvsns_ino_t ino;
        struct stat sbuf;
        int rc;

#ifndef NO_DEBUG
	std::cerr << "Calling KVSNSFileSystem::GetFileSize\n";
	std::cerr << "   Name = " << fname << "\n";
#endif

        TF_RETURN_IF_ERROR(ParseKVSNSPath(fname, path));

        rc = kvsns_->my_kvsns_lookup_path(&cred_,
                                          path, &ino);
        if (rc != 0)
                return RcToStatus(rc, fname);

        rc = kvsns_->kvsns_getattr(&cred_, &ino, &sbuf);
        if (rc != 0)
                return RcToStatus(rc, fname);

	*file_size = sbuf.st_size;

	return Status::OK();
}

Status KVSNSFileSystem::RenameFile(const string& src, const string& target)
{
        char spath[MAXPATHLEN];
	char sname[MAXNAMLEN];
        char dpath[MAXPATHLEN];
	char dname[MAXNAMLEN];
	char sdirpath[MAXPATHLEN];
	char ddirpath[MAXPATHLEN];

        kvsns_ino_t sino;
        kvsns_ino_t dino;
	int rc;

#ifndef NO_DEBUG
	std::cerr << "Calling KVSNSFileSystem::RenameFile\n";
	std::cerr << "  Src = " << src <<  "   Target = " << target << "\n";
#endif

	TF_RETURN_IF_ERROR(ParseKVSNSPath(src, spath));
	TF_RETURN_IF_ERROR(ParseKVSNSPath(target, dpath));

	my_basename(src, sname);
	my_dirpath(src, sdirpath);
	my_basename(target, dname);
	my_dirpath(target, ddirpath);

        rc = kvsns_->my_kvsns_lookup_path(&cred_,
                                          sdirpath,
					  &sino);
        if (rc != 0)
                return RcToStatus(rc, src);

        rc = kvsns_->my_kvsns_lookup_path(&cred_,
                                          ddirpath, 
					  &dino);
        if (rc != 0)
                return RcToStatus(rc, target);

	rc = kvsns_->kvsns_rename(&cred_,
				  &sino, sname,
				  &dino, dname);

        if (rc != 0) {
		if (rc == -EEXIST) {
		/* 
		 * KVSNS does not "delete on rename"
		 * we handle this cas here 
		 */
		rc = kvsns_->kvsns_unlink(&cred_,
					  &dino,
					  dname);
		if (rc != 0)
                	return RcToStatus(rc, target);
		
		/* Now rename after target was unlinked */
		rc = kvsns_->kvsns_rename(&cred_,
				  	  &sino, sname,
				  	  &dino, dname);
		if (rc != 0)
                	return RcToStatus(rc, target);
		} else
                	return RcToStatus(rc, target);
	}

	return Status::OK();
}



REGISTER_FILE_SYSTEM("kvsns", KVSNSFileSystem);

}  // namespace tensorflow
