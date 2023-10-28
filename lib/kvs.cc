#include "cloudlab/kvs.hh"

#include "fmt/core.h"
#include "rocksdb/db.h"

#include <algorithm>
#include <limits>
#include <random>

namespace cloudlab {

    auto KVS::open() -> bool {
        // only open db if it was not opened yet
        if (!db) {
            rocksdb::Options options;
            options.create_if_missing = true;
            bool b = rocksdb::DB::Open(options, path.string(), &db).ok();
            if (b) {
                for (int i = 0; i < partitions; i++) {
                    create_partition(i);
                }
            } else {
                std::vector<rocksdb::ColumnFamilyDescriptor> column_families;
                column_families.emplace_back(
                        rocksdb::kDefaultColumnFamilyName, rocksdb::ColumnFamilyOptions());
                for (int i = 0; i < 4; i++) {
                    column_families.emplace_back(
                            std::to_string(i), rocksdb::ColumnFamilyOptions());
                }
                b = rocksdb::DB::Open(options, path.string(), column_families, &partition_handles, &db).ok();

            }
            return b;
        }
        return true;
    }

    auto KVS::get(const std::string &key, std::string &result) -> bool {
        mtx.lock();
        if (!db) open();
        auto x =partition(key_to_partition(key)).handle;
        bool b = db && db->Get(rocksdb::ReadOptions(), x , key, &result).ok();
        mtx.unlock();
        return b;
    }

    auto KVS::get_all(std::vector<std::pair<std::string, std::string>> &buffer)
    -> bool {
        for (auto it = begin(); it != end(); ++it) {
            buffer.emplace_back(*it);
        }

        return true;
    }

    auto KVS::put(const std::string &key, const std::string &value) -> bool {
        mtx.lock();
        if (!db) open();
        bool b = db && db->Put(rocksdb::WriteOptions(), partition(key_to_partition(key)).handle, key, value).ok();
        mtx.unlock();
        return b;
    }

    auto KVS::remove(const std::string &key) -> bool {
        mtx.lock();
        if (!db) open();
        bool b = db && db->Delete(rocksdb::WriteOptions(), partition(key_to_partition(key)).handle, key).ok();
        mtx.unlock();
        return b;
    }

    auto KVS::clear() -> bool {
        mtx.lock();
        if (!db) open();
        bool b = true;
        for (auto &handle: partition_handles) {
            b = db->DropColumnFamily(handle).ok() && b;
            b = db->DestroyColumnFamilyHandle(handle).ok() && b;
        }
        mtx.unlock();
        return b;
    }

    auto KVS::clear_partition(size_t id) -> bool {
        mtx.lock();
        if (!db) open();
        auto k = std::find_if(partition_handles.begin(), partition_handles.end(),
                              [&id](rocksdb::ColumnFamilyHandle *handle) {
                                  return handle->GetName() == std::to_string(id);
                              });
        if (k != partition_handles.end()) {
            bool b = db->DropColumnFamily(*k).ok();
            b = db->DestroyColumnFamilyHandle(*k).ok() && b;
            partition_handles.erase(k);
            mtx.unlock();
            return b;
        }
        mtx.unlock();
        return true;
    }

    auto KVS::begin() -> KVS::Iterator {
        mtx.lock();
        if (!db) open();
        std::vector<rocksdb::Iterator *> its;
        db->NewIterators(rocksdb::ReadOptions(), partition_handles, &its);
        KVS::Iterator iterator;
        for (auto &it: its) {
            iterator.iterators.emplace_back(it);
        }
        mtx.unlock();
        return iterator;
    }

// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
    auto KVS::end() const -> KVS::Sentinel {
        return {};
    }

    auto KVS::Iterator::operator*()
    -> std::pair<std::string_view, std::string_view> {
        for (auto its = this->iterators.begin(); its != this->iterators.end();) {
            if (*its == nullptr || !(*its)->Valid()) {
                delete *its;
                its = this->iterators.erase(its);
            } else {
                return {(*its)->key().ToStringView(), (*its)->value().ToStringView()};
            }
        }
        return {};
    }

    auto KVS::Iterator::operator++() -> KVS::Iterator & {
        bool i{};
        for (auto its = this->iterators.begin(); its != this->iterators.end();) {
            if (*its == nullptr || !(*its)->Valid()) {
                i = true;
                delete *its;
                its = this->iterators.erase(its);
            } else {
                if (!i) {
                    (*its)->Next();
                    continue;
                }
                break;
            }
        }
        return *this;
    }

    auto operator==(const KVS::Iterator &it, const KVS::Sentinel &) -> bool {
        if (it.iterators.empty()) return true;
        for (auto *its: it.iterators) {
            if (its != nullptr && its->Valid()) return false;
        }
        return true;
    }

    auto operator!=(const KVS::Iterator &lhs, const KVS::Sentinel &rhs) -> bool {
        return !(lhs == rhs);
    }

    KVS::~KVS() {
        for (auto handle: partition_handles) {
            db->DestroyColumnFamilyHandle(handle);
        }
        delete db;
    }

    KVS::Iterator::~Iterator() {
        for (auto &it: iterators) {
            delete it;
        }
    }

    auto KVS::Partition::begin() const -> KVS::Iterator {
        auto it = db->NewIterator(rocksdb::ReadOptions(), handle);
        KVS::Iterator iterator;
        iterator.iterators.emplace_back(it);
        return iterator;
    }

// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
    auto KVS::Partition::end() const -> KVS::Sentinel {
        return {};
    }

}  // namespace cloudlab