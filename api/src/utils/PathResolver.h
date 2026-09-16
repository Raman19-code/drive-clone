#pragma once
#include <string>
#include <vector>
#include <cstdint>

// Resolves a folder's full path by walking parent_id pointers, and
// validates that a proposed move does not create a cycle.
namespace path_resolver {
    std::string resolvePath(int64_t folderId);
    bool wouldCreateCycle(int64_t folderId, int64_t newParentId);
}
