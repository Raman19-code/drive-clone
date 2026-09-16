#include <drogon/drogon.h>

int main() {
    // Load config (DB, Redis, MinIO, JWT keys, etc.)
    drogon::app().loadConfigFile("config/config.dev.json");

    LOG_INFO << "DriveX API starting...";

    drogon::app().run();
    return 0;
}
