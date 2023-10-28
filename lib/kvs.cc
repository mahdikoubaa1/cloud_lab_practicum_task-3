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
    // TODO(you)
  }

  return true;
}

auto KVS::get(const std::string& key, std::string& result) -> bool {
  // TODO(you)
  return {};
}

auto KVS::get_all(std::vector<std::pair<std::string, std::string>>& buffer)
    -> bool {
  // TODO(you)
  return {};
}

auto KVS::put(const std::string& key, const std::string& value) -> bool {
  // TODO(you)
  return {};
}

auto KVS::remove(const std::string& key) -> bool {
  // TODO(you)
  return {};
}

auto KVS::clear() -> bool {
  // TODO(you)
  return {};
}

auto KVS::clear_partition(size_t id) -> bool {
  // TODO(you)
  return {};
}

auto KVS::begin() -> KVS::Iterator {
  // TODO(you)
  return {};
}

// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
auto KVS::end() const -> KVS::Sentinel {
  return {};
}

auto KVS::Iterator::operator*()
    -> std::pair<std::string_view, std::string_view> {
  // TODO(you)
  return {};
}

auto KVS::Iterator::operator++() -> KVS::Iterator& {
  // TODO(you)

  return *this;
}

auto operator==(const KVS::Iterator& it, const KVS::Sentinel&) -> bool {
  // TODO(you)
  return {};
}

auto operator!=(const KVS::Iterator& lhs, const KVS::Sentinel& rhs) -> bool {
  return !(lhs == rhs);
}

KVS::~KVS() {
  // TODO(you)
}

KVS::Iterator::~Iterator() {
  // TODO(you)
}

auto KVS::Partition::begin() const -> KVS::Iterator {
  // TODO(you)
  return {};
}

// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
auto KVS::Partition::end() const -> KVS::Sentinel {
  return {};
}

}  // namespace cloudlab