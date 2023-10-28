#ifndef CLOUDLAB_KVS_HH
#define CLOUDLAB_KVS_HH

#include <deque>
#include <filesystem>
#include <set>
#include <shared_mutex>
#include <vector>

#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"

// actually 840 is a good number
const auto partitions = 4;

namespace rocksdb {
    class DB;

    class Iterator;

    class ColumnFamilyHandle;
}  // namespace rocksdb

namespace cloudlab {

/**
 * The key-value store. We use rocksdb for the actual key-value operations.
 */
    class KVS {
    public:
        struct Sentinel {
        };

        struct Iterator {
            std::deque<rocksdb::Iterator *> iterators;

            ~Iterator();

            friend auto operator==(const Iterator &it, const Sentinel &) -> bool;

            friend auto operator!=(const Iterator &lhs, const Sentinel &rhs) -> bool;

            auto operator*() -> std::pair<std::string_view, std::string_view>;

            auto operator++() -> Iterator &;
        };

        explicit KVS(const std::string &path = {}, bool open = false) : path{path} {
            if (open) this->open();
        }

        struct Partition {
            rocksdb::DB *db;
            rocksdb::ColumnFamilyHandle *handle;

            [[nodiscard]] auto begin() const -> Iterator;

            [[nodiscard]] auto end() const -> Sentinel;
        };

        ~KVS();

        // delete copy constructor and copy assignment
        KVS(const KVS &) = delete;

        auto operator=(const KVS &) -> KVS & = delete;

        [[nodiscard]] auto begin() -> Iterator;

        [[nodiscard]] auto end() const -> Sentinel;

        auto open() -> bool;

        auto get(const std::string &key, std::string &result) -> bool;

        auto get_all(std::vector<std::pair<std::string, std::string>> &buffer)
        -> bool;

        auto put(const std::string &key, const std::string &value) -> bool;

        auto remove(const std::string &key) -> bool;

        auto create_partition(size_t id) {
            rocksdb::ColumnFamilyHandle *cf;
            if (!has_partition(id)) db->CreateColumnFamily(rocksdb::ColumnFamilyOptions(), std::to_string(id), &cf);
            partition_handles.emplace_back(cf);
        }

        auto remove_partition(size_t id) {
            auto k = std::find_if(partition_handles.begin(), partition_handles.end(),
                                  [&id](rocksdb::ColumnFamilyHandle *handle) {
                                      return handle->GetName() == std::to_string(id);
                                  });
            if (k != partition_handles.end()) {
                db->DropColumnFamily(*k);
                db->DestroyColumnFamilyHandle(*k);
                partition_handles.erase(k);
            }
        }

        static auto key_to_partition(const std::string &key) -> size_t {
            return std::hash<std::string>{}(key) % partitions;
        }

        auto has_partition(size_t id) -> bool {
            return partition(id).handle != nullptr;
        }

        auto has_partition_for_key(const std::string &key) -> bool {
            return has_partition(key_to_partition(key));
        }

        auto partition(size_t id) -> Partition {
            auto k = std::find_if(partition_handles.begin(), partition_handles.end(),
                                  [&id](rocksdb::ColumnFamilyHandle *handle) {
                                      return handle->GetName() == std::to_string(id);
                                  });
            if (k == partition_handles.end()) return {db, nullptr};
            else return {db, *k};
        }

        auto clear() -> bool;

        auto clear_partition(size_t id) -> bool;
    private:
        std::filesystem::path path;
        rocksdb::DB *db{};
        std::vector<rocksdb::ColumnFamilyHandle *> partition_handles;

        std::set<size_t> partition_exists;
        std::mutex mtx;
    };

}  // namespace cloudlab

#endif  // CLOUDLAB_KVS_HH
