#include "storage/persistent_interface.h"

#include "storage/disk_storage.h"

namespace kv::storage {

// PersistentInterface *instance = nullptr;
//
// PersistentInterface *GetPersistentInterface() {
//   static bool init = []{
//     static DiskStorage ds;
//     instance = &ds;
//     return true;
//   }();
//
//   return instance;
// }
//
// void SetPersistentInterface(PersistentInterface *persister) {
//   instance = persister;
// }

}