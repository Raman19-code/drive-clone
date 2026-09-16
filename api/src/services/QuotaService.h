#pragma once
#include <cstdint>
#include <string>

// Checks/reserves storage quota before upload; reconciles storage_used_bytes
// after upload-complete or delete events.
class QuotaService {
public:
    bool hasSpace(int64_t userId, int64_t bytesNeeded);
    void reconcile(int64_t userId, int64_t deltaBytes);
};
