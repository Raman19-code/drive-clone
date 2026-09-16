#pragma once
#include <drogon/HttpFilter.h>

using namespace drogon;

// Validates the RS256 JWT on protected routes and attaches the resolved
// user_id to the request attributes for downstream controllers.
class JwtAuthFilter : public HttpFilter<JwtAuthFilter> {
public:
    void doFilter(const HttpRequestPtr &req,
                  FilterCallback &&fcb,
                  FilterChainCallback &&fccb) override;
};
