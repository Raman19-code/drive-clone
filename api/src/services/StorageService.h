#pragma once
#include <string>
#include <chrono>

// Wraps MinIO S3 client: issues pre-signed PUT/GET URLs, checks object
// existence, and manages multipart uploads for large files.
class StorageService {
public:
    std::string presignUploadUrl(const std::string &storageKey,
                                  std::chrono::seconds ttl);
    std::string presignDownloadUrl(const std::string &storageKey,
                                    std::chrono::seconds ttl);
    bool objectExists(const std::string &storageKey);
};
