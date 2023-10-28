#ifndef CLOUDLAB_KVS_HH
#define CLOUDLAB_KVS_HH

#include <deque>
#include <filesystem>
#include <set>
#include <shared_mutex>
#include <vector>

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
  struct Sentinel {};

  struct Iterator {
    std::deque<rocksdb::Iterator*> iterators;

    ~Iterator();

    friend auto operator==(const Iterator& it, const Sentinel&) -> bool;
    friend auto operator!=(const Iterator& lhs, const Sentinel& rhs) -> bool;

    auto operator*() -> std::pair<std::string_view, std::string_view>;
    auto operator++() -> Iterator&;
  };

  explicit KVS(const std::string& path = {}, bool open = false) : path{path} {
    if (open) this->open();
  }

  struct Partition {
    rocksdb::DB* db;
    rocksdb::ColumnFamilyHandle* handle;

    [[nodiscard]] auto begin() const -> Iterator;

    [[nodiscard]] auto end() const -> Sentinel;
  };

  ~KVS();

  // delete copy constructor and copy assignment
  KVS(const KVS&) = delete;
  auto operator=(const KVS&) -> KVS& = delete;

  [[nodiscard]] auto begin() -> Iterator;

  [[nodiscard]] auto end() const -> Sentinel;

  auto open() -> bool;

  auto get(const std::string& key, std::string& result) -> bool;

  auto get_all(std::vector<std::pair<std::string, std::string>>& buffer)
    -> bool;

  auto put(const std::string& key, const std::string& value) -> bool;

  auto remove(const std::string& key) -> bool;

  auto create_partition(size_t id) {
    // TODO(you)
  }

  auto remove_partition(size_t id) {
    // TODO(you)
  }

  static auto key_to_partition(const std::string& key) -> size_t {
    // TODO(you)
    return {};
  }

  auto has_partition(size_t id)-> bool {
    // TODO(you)
    return {};
  }

  auto has_partition_for_key(const std::string& key) -> bool {
    // TODO(you)
    return {};
  }

  auto partition(size_t id) -> Partition {
    // TODO(you)
    return {};
  }

  auto clear() -> bool;

  auto clear_partition(size_t id) -> bool;

 private:
  std::filesystem::path path;
  rocksdb::DB* db{};
  std::vector<rocksdb::ColumnFamilyHandle*> partition_handles;
  std::set<size_t> partition_exists;
  std::mutex mtx;
};

}  // namespace cloudlab

#endif  // CLOUDLAB_KVS_HH
