#pragma once
#include <cstdint>
#include <string>

enum class Role { Viewer, Editor, Owner };

// Resolves effective access for a user on a file/folder, walking up the
// folder tree for inherited permissions where applicable.
class PermissionService {
public:
    bool canAccess(int64_t userId, const std::string &resourceType,
                    int64_t resourceId, Role minRole);
};
