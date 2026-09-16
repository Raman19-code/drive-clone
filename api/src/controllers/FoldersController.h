#pragma once
#include <drogon/HttpController.h>

using namespace drogon;

class FoldersController : public drogon::HttpController<FoldersController> {
public:
    METHOD_LIST_BEGIN
    ADD_METHOD_TO(FoldersController::createFolder, "/folders", Post);
    ADD_METHOD_TO(FoldersController::listFolder, "/folders", Get);
    ADD_METHOD_TO(FoldersController::updateFolder, "/folders/{1}", Patch);
    ADD_METHOD_TO(FoldersController::deleteFolder, "/folders/{1}", Delete);
    METHOD_LIST_END

    void createFolder(const HttpRequestPtr &req,
                       std::function<void(const HttpResponsePtr &)> &&callback);
    void listFolder(const HttpRequestPtr &req,
                     std::function<void(const HttpResponsePtr &)> &&callback);
    void updateFolder(const HttpRequestPtr &req,
                       std::function<void(const HttpResponsePtr &)> &&callback,
                       std::string folderId);
    void deleteFolder(const HttpRequestPtr &req,
                       std::function<void(const HttpResponsePtr &)> &&callback,
                       std::string folderId);
};
