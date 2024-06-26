//
// Created by chaochao on 2021/12/23.
//

#ifndef EXPERIMENTCC12_FILMADALRU_H
#define EXPERIMENTCC12_FILMADALRU_H

#include <unordered_map>
#define MAX_INT (((unsigned int)(-1)) >> 1)
// define the node in doubly-linked list
using namespace std;
namespace adalru {
template <class Key, class Value>
struct Node {
  Key key;
  Value value;
  Node *prev;
  Node *next;

  Node(Key k, Value v) : key(k), value(v), prev(nullptr), next(nullptr){};

  Node() : key(), value(), prev(nullptr), next(nullptr){};
  ~Node() {
    prev = nullptr;
    next = nullptr;
  }
  //    Node(): prev(nullptr), next(nullptr){ };
};

template <class Key, class Value,
          class mapvalue>  // mapvalue 是指向在 lru 中的node 的指针  在hash map
                           // 中使用
class hashLRU {
 public:
  int size = 0;
  int capacity;
  std::unordered_map<Key, mapvalue> map;
  Node<Key, Value> *head;
  Node<Key, Value> *tail;

  hashLRU(int def_capcity) {
    size = 0;
    capacity = def_capcity;
    head = new Node<Key, Value>;
    tail = new Node<Key, Value>;
    head->next = tail;
    tail->prev = head;
  }

  hashLRU() {
    size = 0;
    capacity = MAX_INT;
    head = new Node<Key, Value>;
    tail = new Node<Key, Value>;
    head->next = tail;
    tail->prev = head;
  }

  // get the k node in LRU and move it to the head of LRU
  Node<Key, Value> *get(Key k) {
    Node<Key, Value> *node;
    if (map.find(k) != map.end()) {
      node = map[k];
      moveTohead(node);
      return node;
    }  // k is existed in map
    else {
      return node;
    }
  }

  // 将node 插入到LRU 的head
  void appendhead(Node<Key, Value> *node) {
    node->prev = head;
    node->next = head->next;
    head->next->prev = node;
    head->next = node;
  }

  // 将 找到的node 移动到head
  void moveTohead(Node<Key, Value> *node) {
    if (node->prev == head) return;
    node->prev->next = node->next;
    node->next->prev = node->prev;
    appendhead(node);
  }

  // put the k node into the head of LRU
  void put(Key k, Value v) {
    if (map.find(k) == map.end())  // k is not existed in map
    {
      Node<Key, Value> *node = new Node<Key, Value>(k, v);
      map[k] = node;
      // 判断 size, 如果size = capacity,说明LRU 满了
      if (size == capacity) {
        poptail();
      }
      size += 1;
      appendhead(node);
    } else {
      //                map[k]->value = v;
      moveTohead(map[k]);
    }
  }

  // remove the k node from LRU
  inline void remove(Key k) {
    // 首先找到k 所属的node
    if (map.find(k) == map.end()) return;  // 说明要删除的node 不存在
    Node<Key, Value> *node = map[k];
    node->prev->next = node->next;
    node->next->prev = node->prev;
    map.erase(k);
    //            malloc_trim(0);
    size -= 1;
  }

  // remove the k node from LRU
  inline void removenode(Node<Key, Value> *node) {
    // 首先找到k 所属的node
    node->prev->next = node->next;
    node->next->prev = node->prev;
    if (map.find(node->key) == map.end()) {
      cout << "i need You, my lovely Lord, please come" << endl;
    } else
      map.erase(node->key);
    size -= 1;
  }

  // pop the tail of the LRU, that the least recent used item
  inline Node<Key, Value> *poptail() {
    //            map.erase(tail->prev->key);
    map.erase(tail->prev->key);
    Node<Key, Value> *node;
    node = tail->prev;
    tail->prev->prev->next = tail;
    tail->prev = tail->prev->prev;
    size -= 1;
    return node;
  }

  // get the tail node from local LRU that from this leaf evict key
  Value get_tail() {
    auto tailnode = tail->prev;
    if (tailnode->value->intrachain.size == 1) removenode(tailnode);
    return tailnode->value;
  }

  int deletelru() {
    while (head->next != NULL) {
      auto curnode = head->next;
      curnode->prev = head;
      head->next = curnode->next;
      delete curnode;
    }
    delete head;
    //            delete[] tail;
    std::unordered_map<Key, mapvalue>().swap(map);
    // malloc_trim(0);
    return 0;
  }
};

template <class Key, class Value>
class localLRU {
 public:
  int size = 0;

  Node<Key, Value> *head;
  Node<Key, Value> *tail;

  localLRU() {
    size = 0;
    head = new Node<Key, Value>;
    tail = new Node<Key, Value>;
    head->next = tail;
    tail->prev = head;
  }
  void deletelru() {
    while (head->next != NULL) {
      auto curnode = head->next;
      curnode->prev = head;
      head->next = curnode->next;
      //                delete []curnode->value;
      //                curnode->value = NULL;
      delete curnode;
      curnode = NULL;
    }
    //            malloc_trim(0);
  }

  // 将node 插入到LRU 的head
  inline void appendhead(Node<Key, Value> *node) {
    // inline Node<Key, Value> *appendhead(Node<Key, Value> *node) {
    node->prev = head;
    node->next = head->next;
    head->next->prev = node;
    head->next = node;
  }

  // 将 找到的node 移动到head
  inline void moveTohead(Node<Key, Value> *node) {
    if (node->prev == head) return;
    node->prev->next = node->next;
    node->next->prev = node->prev;
    appendhead(node);
  }

  // put the k node into the head of LRU
  inline Node<Key, Value> *put(Key k, Value v) {
    Node<Key, Value> *node = new Node<Key, Value>(k, v);
    size += 1;
    appendhead(node);
    //            if (k > 4294946590)
    //                cout<< "my Lord ,i need You!"<< endl;
    return node;
  }

  // remove the k node from LRU
  inline void remove_node(Node<Key, Value> *node) {
    node->prev->next = node->next;
    node->next->prev = node->prev;
    delete[] node->value;
    delete node;
    //            malloc_trim(0);
    size -= 1;
  }

  // modify the offset (key in lru node), 所有 nodes 中，key > pos 都都需要+ 1
  inline void modify(int pos) {
    // 遍历 intro chain
    // while 循环，直到找到node->key == k;
    Node<Key, Value> *node = head->next;
    while (node != tail) {
      if (node->key > pos) {
        node->key += 1;
      }
      node = node->next;
    }
  }

  // pop the tail of the local LRU, that the least recent used item
  inline Node<Key, Value> *poptail() {
    Node<Key, Value> *node;
    node = tail->prev;
    tail->prev->prev->next = tail;
    tail->prev = tail->prev->prev;
    size -= 1;
    return node;
  }
};

}  // namespace adalru
#endif  // EXPERIMENTCC12_FILMADALRU_H
