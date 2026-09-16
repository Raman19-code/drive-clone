#pragma once
#include <drogon/HttpController.h>

using namespace drogon;

/**
 * Handles file metadata operations. File bytes never pass through this
 * controller — clients upload/download directly to/from MinIO using
 * pre-signed URLs issued here.
 */
class FilesController : public drogon::HttpController<FilesController> {
public:
    METHOD_LIST_BEGIN
    ADD_METHOD_TO(FilesController::getUploadUrl, "/files/upload-url", Post);
    ADD_METHOD_TO(FilesController::confirmUpload, "/files/upload-complete", Post);
    ADD_METHOD_TO(FilesController::getDownloadUrl, "/files/{1}/download-url", Get);
    ADD_METHOD_TO(FilesController::updateFile, "/files/{1}", Patch);
    ADD_METHOD_TO(FilesController::deleteFile, "/files/{1}", Delete);
    ADD_METHOD_TO(FilesController::listVersions, "/files/{1}/versions", Get);
    METHOD_LIST_END

    void getUploadUrl(const HttpRequestPtr &req,
                       std::function<void(const HttpResponsePtr &)> &&callback);
    void confirmUpload(const HttpRequestPtr &req,
                        std::function<void(const HttpResponsePtr &)> &&callback);
    void getDownloadUrl(const HttpRequestPtr &req,
                         std::function<void(const HttpResponsePtr &)> &&callback,
                         std::string fileId);
    void updateFile(const HttpRequestPtr &req,
                     std::function<void(const HttpResponsePtr &)> &&callback,
                     std::string fileId);
    void deleteFile(const HttpRequestPtr &req,
                     std::function<void(const HttpResponsePtr &)> &&callback,
                     std::string fileId);
    void listVersions(const HttpRequestPtr &req,
                       std::function<void(const HttpResponsePtr &)> &&callback,
                       std::string fileId);
};
