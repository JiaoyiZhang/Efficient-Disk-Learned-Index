#pragma once

#include <immintrin.h>
#include <sched.h>

#include <atomic>
#include <cassert>
#include <cstring>

namespace btreeolc {

enum class PageType : uint8_t { BTreeInner = 1, BTreeLeaf = 2 };

static const uint64_t pageSize = 4 * 1024;

struct OptLock {
  std::atomic<uint64_t> typeVersionLockObsolete{0b100};

  bool isLocked(uint64_t version) { return ((version & 0b10) == 0b10); }

  uint64_t readLockOrRestart(bool& needRestart) {
    uint64_t version;
    version = typeVersionLockObsolete.load();
    if (isLocked(version) || isObsolete(version)) {
      _mm_pause();
      needRestart = true;
    }
    return version;
  }

  void writeLockOrRestart(bool& needRestart) {
    uint64_t version;
    version = readLockOrRestart(needRestart);
    if (needRestart) return;

    upgradeToWriteLockOrRestart(version, needRestart);
    if (needRestart) return;
  }

  void upgradeToWriteLockOrRestart(uint64_t& version, bool& needRestart) {
    if (typeVersionLockObsolete.compare_exchange_strong(version,
                                                        version + 0b10)) {
      version = version + 0b10;
    } else {
      _mm_pause();
      needRestart = true;
    }
  }

  void writeUnlock() { typeVersionLockObsolete.fetch_add(0b10); }

  bool isObsolete(uint64_t version) { return (version & 1) == 1; }

  void checkOrRestart(uint64_t startRead, bool& needRestart) const {
    readUnlockOrRestart(startRead, needRestart);
  }

  void readUnlockOrRestart(uint64_t startRead, bool& needRestart) const {
    needRestart = (startRead != typeVersionLockObsolete.load());
  }

  void writeUnlockObsolete() { typeVersionLockObsolete.fetch_add(0b11); }
};

struct NodeBase : public OptLock {
  PageType type;
  uint16_t count;
};

struct BTreeLeafBase : public NodeBase {
  static const PageType typeMarker = PageType::BTreeLeaf;
};

template <class Key, class Payload>
struct BTreeLeaf : public BTreeLeafBase {
  struct Entry {
    Key k;
    Payload p;
  };

  static const uint64_t maxEntries =
      (pageSize - sizeof(NodeBase)) / (sizeof(Key) + sizeof(Payload));

  Key keys[maxEntries];
  Payload payloads[maxEntries];

  BTreeLeaf<Key, Payload>* next_leaf = nullptr;

  BTreeLeaf() {
    count = 0;
    type = typeMarker;
  }

  bool isFull() { return count == maxEntries; };

  unsigned lowerBound(Key k) {
    unsigned lower = 0;
    unsigned upper = count;
    do {
      unsigned mid = ((upper - lower) / 2) + lower;
      if (k < keys[mid]) {
        upper = mid;
      } else if (k > keys[mid]) {
        lower = mid + 1;
      } else {
        return mid;
      }
    } while (lower < upper);
    return lower;
  }

  unsigned lowerBoundBF(Key k) {
    auto base = keys;
    unsigned n = count;
    while (n > 1) {
      const unsigned half = n / 2;
      base = (base[half] < k) ? (base + half) : base;
      n -= half;
    }
    return (*base < k) + base - keys;
  }

  void insert(Key k, Payload p) {
    assert(count < maxEntries);
    if (count) {
      unsigned pos = lowerBound(k);
      if ((pos < count) && (keys[pos] == k)) {
        // Upsert
        payloads[pos] = p;
        return;
      }
      memmove(keys + pos + 1, keys + pos, sizeof(Key) * (count - pos));
      memmove(payloads + pos + 1, payloads + pos,
              sizeof(Payload) * (count - pos));
      keys[pos] = k;
      payloads[pos] = p;
    } else {
      keys[0] = k;
      payloads[0] = p;
    }
    count++;
  }

  BTreeLeaf* split(Key& sep) {
    BTreeLeaf* newLeaf = new BTreeLeaf();
    newLeaf->count = count - (count / 2);
    count = count - newLeaf->count;
    memcpy(newLeaf->keys, keys + count, sizeof(Key) * newLeaf->count);
    memcpy(newLeaf->payloads, payloads + count,
           sizeof(Payload) * newLeaf->count);
    sep = keys[count - 1];
    if (this->next_leaf != NULL) {
      newLeaf->next_leaf = this->next_leaf;
    }
    this->next_leaf = newLeaf;
    return newLeaf;
  }
};

struct BTreeInnerBase : public NodeBase {
  static const PageType typeMarker = PageType::BTreeInner;
};

template <class Key>
struct BTreeInner : public BTreeInnerBase {
  static const uint64_t maxEntries =
      (pageSize - sizeof(NodeBase)) / (sizeof(Key) + sizeof(NodeBase*));
  NodeBase* children[maxEntries];
  Key keys[maxEntries];

  BTreeInner() {
    count = 0;
    type = typeMarker;
  }

  bool isFull() { return count == (maxEntries - 1); };

  unsigned lowerBoundBF(Key k) {
    auto base = keys;
    unsigned n = count;
    while (n > 1) {
      const unsigned half = n / 2;
      base = (base[half] < k) ? (base + half) : base;
      n -= half;
    }
    return (*base < k) + base - keys;
  }

  unsigned lowerBound(Key k) {
    unsigned lower = 0;
    unsigned upper = count;
    do {
      unsigned mid = ((upper - lower) / 2) + lower;
      if (k < keys[mid]) {
        upper = mid;
      } else if (k > keys[mid]) {
        lower = mid + 1;
      } else {
        return mid;
      }
    } while (lower < upper);
    return lower;
  }

  BTreeInner* split(Key& sep) {
    BTreeInner* newInner = new BTreeInner();
    newInner->count = count - (count / 2);
    count = count - newInner->count - 1;
    sep = keys[count];
    memcpy(newInner->keys, keys + count + 1,
           sizeof(Key) * (newInner->count + 1));
    memcpy(newInner->children, children + count + 1,
           sizeof(NodeBase*) * (newInner->count + 1));
    return newInner;
  }

  void insert(Key k, NodeBase* child) {
    assert(count < maxEntries - 1);
    unsigned pos = lowerBound(k);
    memmove(keys + pos + 1, keys + pos, sizeof(Key) * (count - pos + 1));
    memmove(children + pos + 1, children + pos,
            sizeof(NodeBase*) * (count - pos + 1));
    keys[pos] = k;
    children[pos] = child;
    std::swap(children[pos], children[pos + 1]);
    count++;
  }
};

template <class Key, class Value>
struct BTree {
  std::atomic<NodeBase*> root;
  std::atomic<BTreeLeaf<Key, Value>*> first_leaf;
  std::atomic<uint64_t> item_count;
  std::atomic<uint64_t> leaf_count;
  std::atomic<uint64_t> inner_count;

  BTree() {
    root = new BTreeLeaf<Key, Value>();
    item_count.store(0);
    leaf_count.store(1);
    inner_count.store(0);
    first_leaf.store(static_cast<BTreeLeaf<Key, Value>*>(root.load()));
  }
  BTree& operator=(const BTree& other) {
    if (this != &other) {
      root.store(other.root.load());
      leaf_count.store(other.leaf_count.load());
      item_count.store(other.item_count.load());
      inner_count.store(other.inner_count.load());
      first_leaf.store(other.first_leaf.load());
    }
    return *this;
  }

  /// Return the number of key/data pairs in the B+ tree
  inline uint64_t size() const { return item_count.load(); }
  /// Return the memory usage of inner nodes in the B+ tree
  inline uint64_t inner_bytes() const { return inner_count.load() * pageSize; }
  /// Return the memory usage of leaf nodes in the B+ tree
  inline uint64_t leaf_bytes() const { return leaf_count.load() * pageSize; }

  // designed for hybrid index framework (immutable mode)
  void MergeData(std::vector<std::pair<Key, Value>>* merged_data) {
    BTreeLeaf<Key, Value>* leaf = first_leaf.load();
    size_t data_num = size();
    uint64_t merge_cnt = 0;
    int leaf_cnt = 0;
    while (leaf && merge_cnt < data_num) {
      leaf_cnt++;
      for (unsigned i = 0; i < leaf->count; i++) {
        if (merge_cnt >= merged_data->size()) {
          merged_data->push_back({leaf->keys[i], leaf->payloads[i]});
        } else {
          (*merged_data)[merge_cnt++] = {leaf->keys[i], leaf->payloads[i]};
        }
      }
      leaf = leaf->next_leaf;
    }
  }

  void makeRoot(Key k, NodeBase* leftChild, NodeBase* rightChild) {
    auto inner = new BTreeInner<Key>();
    inner->count = 1;
    inner->keys[0] = k;
    inner->children[0] = leftChild;
    inner->children[1] = rightChild;
    root = inner;
  }

  void yield(int count) {
    if (count > 3)
      sched_yield();
    else
      _mm_pause();
  }

  void insert(Key k, Value v) {
    int restartCount = 0;
  restart:
    if (restartCount++) yield(restartCount);
    bool needRestart = false;

    // Current node
    NodeBase* node = root;
    uint64_t versionNode = node->readLockOrRestart(needRestart);
    if (needRestart || (node != root)) goto restart;

    // Parent of current node
    BTreeInner<Key>* parent = nullptr;
    uint64_t versionParent;

    while (node->type == PageType::BTreeInner) {
      auto inner = static_cast<BTreeInner<Key>*>(node);

      // Split eagerly if full
      if (inner->isFull()) {
        // Lock
        if (parent) {
          parent->upgradeToWriteLockOrRestart(versionParent, needRestart);
          if (needRestart) goto restart;
        }
        node->upgradeToWriteLockOrRestart(versionNode, needRestart);
        if (needRestart) {
          if (parent) parent->writeUnlock();
          goto restart;
        }
        if (!parent && (node != root)) {  // there's a new parent
          node->writeUnlock();
          goto restart;
        }
        // Split
        Key sep;
        BTreeInner<Key>* newInner = inner->split(sep);
        inner_count.fetch_add(1, std::memory_order_relaxed);
        if (parent)
          parent->insert(sep, newInner);
        else
          makeRoot(sep, inner, newInner);
        // Unlock and restart
        node->writeUnlock();
        if (parent) parent->writeUnlock();
        goto restart;
      }

      if (parent) {
        parent->readUnlockOrRestart(versionParent, needRestart);
        if (needRestart) goto restart;
      }

      parent = inner;
      versionParent = versionNode;

      node = inner->children[inner->lowerBound(k)];
      inner->checkOrRestart(versionNode, needRestart);
      if (needRestart) goto restart;
      versionNode = node->readLockOrRestart(needRestart);
      if (needRestart) goto restart;
    }

    auto leaf = static_cast<BTreeLeaf<Key, Value>*>(node);

    // Split leaf if full
    if (leaf->count == leaf->maxEntries) {
      // Lock
      if (parent) {
        parent->upgradeToWriteLockOrRestart(versionParent, needRestart);
        if (needRestart) {
          goto restart;
        }
      }
      node->upgradeToWriteLockOrRestart(versionNode, needRestart);
      if (needRestart) {
        if (parent) parent->writeUnlock();
        goto restart;
      }
      if (!parent && (node != root)) {  // there's a new parent
        node->writeUnlock();
        goto restart;
      }
      // Split
      Key sep;
      BTreeLeaf<Key, Value>* newLeaf = leaf->split(sep);
      // if (first_leaf == NULL) {
      //   first_leaf.store(leaf);
      // }
      leaf_count.fetch_add(1, std::memory_order_relaxed);
      if (parent) {
        parent->insert(sep, newLeaf);
      } else {
        makeRoot(sep, leaf, newLeaf);
        inner_count.fetch_add(1, std::memory_order_relaxed);
      }
      // Unlock and restart
      node->writeUnlock();
      if (parent) parent->writeUnlock();
      goto restart;
    } else {
      // only lock leaf node
      node->upgradeToWriteLockOrRestart(versionNode, needRestart);
      if (needRestart) goto restart;
      if (parent) {
        parent->readUnlockOrRestart(versionParent, needRestart);
        if (needRestart) {
          node->writeUnlock();
          goto restart;
        }
      }
      leaf->insert(k, v);
      item_count.fetch_add(1, std::memory_order_relaxed);
      node->writeUnlock();
      return;  // success
    }
  }

  bool lookup(Key k, Value& result) {
    int restartCount = 0;
  restart:
    if (restartCount++) yield(restartCount);
    bool needRestart = false;

    NodeBase* node = root;
    uint64_t versionNode = node->readLockOrRestart(needRestart);
    if (needRestart || (node != root)) {
      goto restart;
    }

    // Parent of current node
    BTreeInner<Key>* parent = nullptr;
    uint64_t versionParent;

    while (node->type == PageType::BTreeInner) {
      auto inner = static_cast<BTreeInner<Key>*>(node);

      if (parent) {
        parent->readUnlockOrRestart(versionParent, needRestart);
        if (needRestart) {
          goto restart;
        }
      }

      parent = inner;
      versionParent = versionNode;

      node = inner->children[inner->lowerBound(k)];
      inner->checkOrRestart(versionNode, needRestart);
      if (needRestart) {
        goto restart;
      }
      versionNode = node->readLockOrRestart(needRestart);
      if (needRestart) {
        goto restart;
      }
    }

    BTreeLeaf<Key, Value>* leaf = static_cast<BTreeLeaf<Key, Value>*>(node);
    unsigned pos = leaf->lowerBound(k);
    bool success = false;
    if ((pos < leaf->count) && (leaf->keys[pos] == k)) {
      success = true;
      result = leaf->payloads[pos];
    }
    if (parent) {
      parent->readUnlockOrRestart(versionParent, needRestart);
      if (needRestart) {
        goto restart;
      }
    }
    node->readUnlockOrRestart(versionNode, needRestart);
    if (needRestart) {
      goto restart;
    }

    return success;
  }

  uint64_t scan(Key k, int range, Value* output) {
    int restartCount = 0;
  restart:
    if (restartCount++) yield(restartCount);
    bool needRestart = false;

    NodeBase* node = root;
    uint64_t versionNode = node->readLockOrRestart(needRestart);
    if (needRestart || (node != root)) goto restart;

    // Parent of current node
    BTreeInner<Key>* parent = nullptr;
    uint64_t versionParent;

    while (node->type == PageType::BTreeInner) {
      auto inner = static_cast<BTreeInner<Key>*>(node);

      if (parent) {
        parent->readUnlockOrRestart(versionParent, needRestart);
        if (needRestart) goto restart;
      }

      parent = inner;
      versionParent = versionNode;

      node = inner->children[inner->lowerBound(k)];
      inner->checkOrRestart(versionNode, needRestart);
      if (needRestart) goto restart;
      versionNode = node->readLockOrRestart(needRestart);
      if (needRestart) goto restart;
    }

    BTreeLeaf<Key, Value>* leaf = static_cast<BTreeLeaf<Key, Value>*>(node);
    unsigned pos = leaf->lowerBound(k);
    int count = 0;
    for (unsigned i = pos; i < leaf->count; i++) {
      if (count == range) break;
      output[count++] = leaf->payloads[i];
    }

    if (parent) {
      parent->readUnlockOrRestart(versionParent, needRestart);
      if (needRestart) goto restart;
    }
    node->readUnlockOrRestart(versionNode, needRestart);
    if (needRestart) goto restart;

    return count;
  }
};

}  // namespace btreeolc
