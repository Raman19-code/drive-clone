#include "FilesController.h"

void FilesController::getUploadUrl(const HttpRequestPtr &req,
                                    std::function<void(const HttpResponsePtr &)> &&callback) {
    // TODO: check quota (QuotaService), generate a MinIO pre-signed PUT URL,
    // return { upload_url, storage_key } to the client.
    auto resp = HttpResponse::newHttpJsonResponse(Json::Value("not implemented"));
    callback(resp);
}

void FilesController::confirmUpload(const HttpRequestPtr &req,
                                     std::function<void(const HttpResponsePtr &)> &&callback) {
    // TODO: verify object exists in MinIO, write files row + file_versions row,
    // reconcile storage_used_bytes, publish upload-complete event to RabbitMQ.
    auto resp = HttpResponse::newHttpJsonResponse(Json::Value("not implemented"));
    callback(resp);
}

void FilesController::getDownloadUrl(const HttpRequestPtr &req,
                                      std::function<void(const HttpResponsePtr &)> &&callback,
                                      std::string fileId) {
    // TODO: check permission (PermissionService), generate pre-signed GET URL.
    auto resp = HttpResponse::newHttpJsonResponse(Json::Value("not implemented"));
    callback(resp);
}

void FilesController::updateFile(const HttpRequestPtr &req,
                                  std::function<void(const HttpResponsePtr &)> &&callback,
                                  std::string fileId) {
    auto resp = HttpResponse::newHttpJsonResponse(Json::Value("not implemented"));
    callback(resp);
}

void FilesController::deleteFile(const HttpRequestPtr &req,
                                  std::function<void(const HttpResponsePtr &)> &&callback,
                                  std::string fileId) {
    // TODO: soft delete (is_trashed = true, trashed_at = now()); hard delete after 30 days.
    auto resp = HttpResponse::newHttpJsonResponse(Json::Value("not implemented"));
    callback(resp);
}

void FilesController::listVersions(const HttpRequestPtr &req,
                                    std::function<void(const HttpResponsePtr &)> &&callback,
                                    std::string fileId) {
    auto resp = HttpResponse::newHttpJsonResponse(Json::Value("not implemented"));
    callback(resp);
}
