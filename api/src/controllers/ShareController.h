#pragma once
#include <drogon/HttpController.h>

using namespace drogon;

class ShareController : public drogon::HttpController<ShareController> {
public:
    METHOD_LIST_BEGIN
    ADD_METHOD_TO(ShareController::grantPermission, "/permissions/{1}", Post);
    ADD_METHOD_TO(ShareController::revokePermission, "/permissions/{1}", Delete);
    ADD_METHOD_TO(ShareController::createShareLink, "/share-links", Post);
    METHOD_LIST_END

    void grantPermission(const HttpRequestPtr &req,
                          std::function<void(const HttpResponsePtr &)> &&callback,
                          std::string resource);
    void revokePermission(const HttpRequestPtr &req,
                           std::function<void(const HttpResponsePtr &)> &&callback,
                           std::string resource);
    void createShareLink(const HttpRequestPtr &req,
                          std::function<void(const HttpResponsePtr &)> &&callback);
};
