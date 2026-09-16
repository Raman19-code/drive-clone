#pragma once
#include <drogon/HttpController.h>

using namespace drogon;

/**
 * Registration/login. Passwords hashed with argon2id; sessions issued as
 * short-lived JWT (RS256) access tokens + refresh tokens.
 */
class AuthController : public drogon::HttpController<AuthController> {
public:
    METHOD_LIST_BEGIN
    ADD_METHOD_TO(AuthController::registerUser, "/auth/register", Post);
    ADD_METHOD_TO(AuthController::login, "/auth/login", Post);
    METHOD_LIST_END

    void registerUser(const HttpRequestPtr &req,
                       std::function<void(const HttpResponsePtr &)> &&callback);
    void login(const HttpRequestPtr &req,
               std::function<void(const HttpResponsePtr &)> &&callback);
};
