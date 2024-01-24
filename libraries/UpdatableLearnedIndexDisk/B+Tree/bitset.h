#pragma once

template <std::size_t N>
class Bitset {
 private:
  using word_t = std::uint64_t;
  static constexpr auto word_bits = sizeof(word_t) * 8;
  static constexpr auto num_blocks = (N + word_bits - 1) / word_bits;
  word_t blocks[num_blocks];

 public:
  void set(std::size_t i) {
    blocks[i / word_bits] |= word_t(1) << (i % word_bits);
  }
  void reset(std::size_t i) {
    blocks[i / word_bits] &= ~(word_t(1) << (i % word_bits));
  }
  bool get(std::size_t i) const {
    return (blocks[i / word_bits] >> (i % word_bits)) & 1;
  }
  void insert(std::size_t i) {
    auto block_id = i / word_bits;
    auto offset = i % word_bits;
    auto &cur = blocks[block_id];
    auto last = cur >> (word_bits - 1);
    cur =
        (offset + 1 == word_bits ? word_t(0) : cur >> offset << (offset + 1)) |
        (cur & ((word_t(1) << offset) - 1));
    for (auto j = block_id + 1; j < num_blocks; ++j) {
      auto &next = blocks[j];
      const auto new_last = next >> (word_bits - 1);
      next = (next << 1) | last;
      last = new_last;
    }
  }
  void split_to(Bitset &that) const {
    constexpr auto count1 = N / 2;
    constexpr auto count2 = N - count1;
    auto i = count1 / word_bits;
    constexpr auto offset = count1 % word_bits;
    constexpr auto that_blocks = (count2 + word_bits - 1) / word_bits;
    static_assert(that_blocks < num_blocks);
    for (std::size_t j = 0; j < that_blocks; ++j, ++i) {
      that.blocks[j] =
          (blocks[i] >> offset) | (blocks[i + 1] << (word_bits - offset));
    }
  }
};
