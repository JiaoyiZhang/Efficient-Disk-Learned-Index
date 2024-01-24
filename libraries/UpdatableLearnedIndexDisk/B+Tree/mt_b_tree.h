#pragma once
#include <algorithm>
#include <atomic>
#include <cassert>
#include <memory>
#include <shared_mutex>
#include <vector>

#include "mt_storage.h"
#include "bitset.h"

template <class Key, class Value>
class ThreadSafeBTreeDisk {
 private:
  static constexpr uint64_t page_size = 4 * 1024;

  struct RootNode {
    std::shared_mutex mtx;
    std::size_t height;
    void* child;
  };

  template <class T, std::size_t N>
  struct InnerNode {
    static constexpr auto fan_out = N;
    std::shared_mutex mtx;
    std::size_t count = 0;

    Key keys[fan_out - 1];
    T children[fan_out];

    template <class S>
    Key split_to(S* that) {
      constexpr auto count1 = fan_out / 2;
      constexpr auto count2 = fan_out - count1;
      this->count = count1;
      that->count = count2;
      std::copy(keys + count1, keys + fan_out - 1, that->keys);
      std::copy(children + count1, children + fan_out, that->children);
      return keys[count1 - 1];
    }

    void insert(std::size_t idx, Key k, T child) {
      assert(idx > 0);
      assert(idx <= count);
      insert_arr(keys, idx - 1, count - 1, k);
      insert_arr(children, idx, count, child);
      ++count;
    }

    std::size_t which_child(const Key& k) const {
      return std::upper_bound(keys, keys + count - 1, k) - keys;
    }

    bool is_full() const { return count == fan_out; }
  };

  static constexpr auto inner_fan_out =
      (page_size + sizeof(Key) - sizeof(std::shared_mutex) - sizeof(std::size_t)) /
      (sizeof(Key) + sizeof(void*));
  struct FanNode : public InnerNode<void*, inner_fan_out> {};

  // ptr * n + key * (n - 1) + mtx + count + ceil(n / 8) <= page_size
  // ceil((ptr + key + 1 / 8) * n) <= page_size + key - mtx - count
  // n <= (page_size + key - mtx - count) / (ptr + key + 1 / 8)
  static constexpr auto disk_fan_out =
      8 * (page_size + sizeof(Key) - sizeof(std::shared_mutex) - sizeof(std::size_t)) /
      ((sizeof(Key) + sizeof(std::size_t)) * 8 + 1);
  struct ToDiskNode : public InnerNode<std::size_t, disk_fan_out> {
    using super = InnerNode<std::size_t, disk_fan_out>;
    Bitset<disk_fan_out> bitmap;

    template <class S>
    Key split_to(S* that) {
      bitmap.split_to(that->bitmap);
      return super::split_to(that);
    }

    void insert(std::size_t idx, Key k, std::size_t child) {
      bitmap.insert(idx);
      super::insert(idx, k, child);
    }
  };

  static_assert(sizeof(FanNode) <= page_size);
  static_assert(sizeof(ToDiskNode) <= page_size);

  template <class T>
  inline static void insert_arr(T arr[], std::size_t idx, std::size_t n,
                                T val) {
    std::copy_backward(arr + idx, arr + n, arr + n + 1);
    arr[idx] = val;
  }

  struct LeafNode {
    std::size_t count = 0;
    static constexpr auto num_entries =
        (BlockSize - sizeof(std::size_t)) / (sizeof(Key) + sizeof(Value));
    Key keys[num_entries];
    Value values[num_entries];
    void insert(std::size_t idx, Key k, Value v) {
      assert(idx <= count);
      insert_arr(keys, idx, count, k);
      insert_arr(values, idx, count, v);
      ++count;
    }
    auto split() {
      auto that = leaf_alloc();
      constexpr auto count1 = num_entries / 2;
      constexpr auto count2 = num_entries - count1;
      this->count = count1;
      that->count = count2;
      std::copy(keys + count1, keys + num_entries, that->keys);
      std::copy(values + count1, values + num_entries, that->values);
      return that;
    }

    std::size_t which_child(const Key& k) const {
      return std::lower_bound(keys, keys + count, k) - keys;
    }

    bool is_full() const { return count == num_entries; }
  };

  ThreadSafeStorageManager sm;
  std::atomic<std::size_t> block_cnt = 0;
  RootNode root;

  std::size_t new_block() { return block_cnt.fetch_add(1); }

  static auto leaf_alloc() {
    struct AlignedDelete {
      void operator()(void* ptr) const { std::free(ptr); }
    };
    static_assert(sizeof(LeafNode) <= BlockSize);
    return std::unique_ptr<LeafNode, AlignedDelete>(
        static_cast<LeafNode*>(decltype(sm)::alloc()));
  }

 public:
  ThreadSafeBTreeDisk(std::string filename) : sm(filename) {
    auto to_disk = new ToDiskNode();
    to_disk->count = 1;
    to_disk->children[0] = new_block();
    auto leaf = leaf_alloc();
    leaf->count = 0;
    sm.write_block(to_disk->children[0], leaf.get());

    root.child = to_disk;
    root.height = 2;
  }

  void direct_open(std::string filename) { sm.direct_open(filename); }
  void no_direct_open(std::string filename) { sm.no_direct_open(filename); }

  void insert(Key k, Value v) {
    { // optimistic lookup
      std::shared_lock<std::shared_mutex> lock(root.mtx);
      void* node = root.child;
      auto tree_height = root.height;
      for (size_t depth = 1; depth < tree_height - 1; ++depth) {
        auto fan_node = static_cast<FanNode*>(node);
        lock = std::shared_lock<std::shared_mutex>(fan_node->mtx);
        node = fan_node->children[fan_node->which_child(k)];
      }
      auto to_disk_node = static_cast<ToDiskNode*>(node);
      // a unique lock is necessary to prevent other threads from
      // writing into the same disk block at the same time
      std::unique_lock<std::shared_mutex> to_disk_lock(to_disk_node->mtx);
      lock.unlock();
      auto to_disk_child_idx = to_disk_node->which_child(k);
      if (!to_disk_node->bitmap.get(to_disk_child_idx)) {
        // no need to split
        auto leaf_node = leaf_alloc();
        std::size_t block_id = to_disk_node->children[to_disk_child_idx];
        sm.read_block(block_id, leaf_node.get());
        assert(!leaf_node->is_full());
        auto key_idx = leaf_node->which_child(k);
        if (key_idx < leaf_node->count && leaf_node->keys[key_idx] == k) {
          if (leaf_node->values[key_idx] == v) return;
          leaf_node->values[key_idx] = v;
          sm.write_block(block_id, leaf_node.get());
          return;
        }
        leaf_node->insert(key_idx, k, v);
        if (leaf_node->is_full()) {
          to_disk_node->bitmap.set(to_disk_child_idx);
        }
        sm.write_block(block_id, leaf_node.get());
        return;
      }
    }
    struct InsertInfo {
      void* node;
      std::unique_lock<std::shared_mutex> lock;
      std::size_t child_idx;
    };
    std::vector<InsertInfo> path;
    path.push_back({&root, std::unique_lock<std::shared_mutex>(root.mtx), 0});
    const std::size_t tree_height = root.height;
    void* cur_node = root.child;
    for (std::size_t depth = 1; depth < tree_height - 1; ++depth) {
      auto node = static_cast<FanNode*>(cur_node);
      std::unique_lock<std::shared_mutex> lock(node->mtx);
      if (!node->is_full()) {
        path.clear();
      }
      auto child_idx = node->which_child(k);
      path.push_back({static_cast<void*>(node), std::move(lock), child_idx});
      cur_node = node->children[child_idx];
    }

    auto to_disk_node = static_cast<ToDiskNode*>(cur_node);
    assert(to_disk_node->count > 0);
    std::unique_lock<std::shared_mutex> to_disk_node_lock(to_disk_node->mtx);
    if (!to_disk_node->is_full()) {
      path.clear();
    }
    auto to_disk_child_idx = to_disk_node->which_child(k);
    std::size_t block_id = to_disk_node->children[to_disk_child_idx];
    if (!to_disk_node->bitmap.get(to_disk_child_idx)) {
      path.clear();
    }

    auto leaf_node = leaf_alloc();
    assert(block_id < block_cnt.load());
    sm.read_block(block_id, leaf_node.get());
    auto key_idx = leaf_node->which_child(k);
    assert(to_disk_node->bitmap.get(to_disk_child_idx) == leaf_node->is_full());
    if (key_idx < leaf_node->count && leaf_node->keys[key_idx] == k) {
      if (leaf_node->values[key_idx] == v) {
        return;
      }
      path.clear();
      leaf_node->values[key_idx] = v;
      sm.write_block(block_id, leaf_node.get());
      return;
    } else if (!leaf_node->is_full()) {
      assert(path.size() == 0);
      leaf_node->insert(key_idx, k, v);
      if (leaf_node->is_full()) {
        to_disk_node->bitmap.set(to_disk_child_idx);
      }
      sm.write_block(block_id, leaf_node.get());
      return;
    }

    // insert split
    Key split_key;
    void* node2;
    const auto block_id2 = new_block();
    {
      auto leaf_node2 = leaf_node->split();
      if (key_idx <= leaf_node->count) {
        leaf_node->insert(key_idx, k, v);
      } else {
        leaf_node2->insert(key_idx - leaf_node->count, k, v);
      }
      sm.write_block(block_id, leaf_node.get());
      sm.write_block(block_id2, leaf_node2.get());
      split_key = leaf_node2->keys[0];
    }

    {
      to_disk_node->bitmap.reset(to_disk_child_idx);
      if (to_disk_node->count < ToDiskNode::fan_out) {
        to_disk_node->insert(to_disk_child_idx + 1, split_key, block_id2);
        return;
      }
      auto to_disk_node2 = new ToDiskNode();
      auto new_split_key = to_disk_node->split_to(to_disk_node2);
      if (to_disk_child_idx < to_disk_node->count) {
        to_disk_node->insert(to_disk_child_idx + 1, split_key, block_id2);
      } else {
        to_disk_node2->insert(to_disk_child_idx + 1 - to_disk_node->count,
                              split_key, block_id2);
      }
      split_key = new_split_key;
      node2 = to_disk_node2;
    }

    bool path_starts_at_root = path.size() == tree_height - 1;
    for (;; path.pop_back()) {
      assert(!path.empty());
      if (path.size() == 1 && path_starts_at_root) {
        auto fan_node = new FanNode();
        static_assert(FanNode::fan_out >= 2);
        fan_node->count = 2;
        fan_node->keys[0] = split_key;
        fan_node->children[0] = root.child;
        fan_node->children[1] = node2;
        root.child = fan_node;
        root.height += 1;
        return;
      }
      // fan node split
      auto node = static_cast<FanNode*>(path.back().node);
      auto child_idx = path.back().child_idx;
      if (node->count < FanNode::fan_out) {
        node->insert(child_idx + 1, split_key, node2);
        return;
      }
      auto fan_node2 = new FanNode();
      auto new_split_key = node->split_to(fan_node2);
      if (child_idx < node->count) {
        node->insert(child_idx + 1, split_key, node2);
      } else {
        fan_node2->insert(child_idx + 1 - node->count, split_key, node2);
      }
      split_key = new_split_key;
      node2 = fan_node2;
    }
  }

  Value lookup(Key k) {
    std::shared_lock<std::shared_mutex> lock(root.mtx);
    void* node = root.child;
    auto tree_height = root.height;
    for (size_t depth = 1; depth < tree_height - 1; ++depth) {
      auto fan_node = static_cast<FanNode*>(node);
      lock = std::shared_lock<std::shared_mutex>(fan_node->mtx);
      node = fan_node->children[fan_node->which_child(k)];
    }
    auto to_disk_node = static_cast<ToDiskNode*>(node);
    lock = std::shared_lock<std::shared_mutex>(to_disk_node->mtx);
    std::size_t block_id = to_disk_node->children[to_disk_node->which_child(k)];
    auto leaf_node = leaf_alloc();
    sm.read_block(block_id, leaf_node.get());
    auto key_idx = leaf_node->which_child(k);
    if (leaf_node->keys[key_idx] == k) {
      return leaf_node->values[key_idx];
    }
    assert(false);
  }

  static void is_valid(LeafNode* node, Key &min_key, Key &max_key) {
    for (std::size_t i = 0; i < node->count; i++) {
      if (i > 0) {
        assert(node->keys[i - 1] < node->keys[i]);
      } else {
        min_key = node->keys[i];
      }
      if (i + 1 == node->count) {
        max_key = node->keys[i];
      }
    }
  }
  void is_valid(void* _node, std::size_t depth, Key &min_key, Key &max_key) const {
    if (depth == 1) {
      // to disk node
      auto node = static_cast<ToDiskNode*>(_node);
      for (std::size_t i = 0; i < node->count; i++) {
        Key cur_min = 0, cur_max = 0;
        auto leaf = leaf_alloc();
        sm.read_block(node->children[i], leaf.get());
        assert(node->bitmap.get(i) == leaf->is_full());
        is_valid(leaf.get(), cur_min, cur_max);
        if (i > 0) {
          if (i + 1 < node->count) {
            assert(node->keys[i - 1] < node->keys[i]);
          }
          assert(cur_min == node->keys[i - 1]);
        } else {
          min_key = cur_min;
        }
        if (i + 1 < node->count) {
          assert(cur_max < node->keys[i]);
        } else {
          max_key = cur_max;
        }
      }
    } else {
      // fan node
      auto node = static_cast<FanNode*>(_node);
      for (std::size_t i = 0; i < node->count; i++) {
        Key cur_min = 0, cur_max = 0;
        is_valid(node->children[i], depth - 1, cur_min, cur_max);
        if (i > 0) {
          if (i + 1 < node->count) {
            assert(node->keys[i - 1] < node->keys[i]);
          }
          assert(cur_min == node->keys[i - 1]);
        } else {
          min_key = cur_min;
        }
        if (i + 1 < node->count) {
          assert(cur_max < node->keys[i]);
        } else {
          max_key = cur_max;
        }
      }
    }
  }
  void is_valid() const {
    auto root = this->root.child;
    auto depth = this->root.height;
    Key min_key, max_key;
    is_valid(root, depth - 1, min_key, max_key);
  }
};
