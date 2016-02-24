
#ifndef MESSAGE_H
#define MESSAGE_H

#include <cstdint>
#include <utility>

// class tag_t {
//     uint64_t raw_tag;

// public:
//     enum struct type : uint64_t {
//         DATA_BLOCK = 1ull << 56,
//         SHUTDOWN = 2ull << 56,
//         SMALL_SEND = 3ull << 56,
//         UNRECOGNIZED = 0xffull << 56
//     };

//     static constexpr uint64_t TYPE_MASK = 0xff00000000000000ull;
//     static constexpr uint64_t GROUP_MASK = 0x00ffff0000000000ull;
//     static constexpr uint64_t MESSAGE_MASK = 0x000000ff00000000ull;
//     static constexpr uint64_t INDEX_MASK = 0x00000000ffff0000ull;
//     static constexpr uint64_t SIZE_MASK = 0x000000000000ffffull;

//     tag_t() : raw_tag(~0ull) {}
//     tag_t(uint64_t t) : raw_tag(t) {}
//     tag_t(type t) : raw_tag(static_cast<uint64_t>(t)) {}

//     type message_type() {
//         uint64_t t = (raw_tag & TYPE_MASK) >> 56;
//         switch(t) {
//             case 1:
//                 return type::DATA_BLOCK;
//             case 2:
//                 return type::SHUTDOWN;
//             case 3:
//                 return type::SMALL_SEND;
//             default:
//                 return type::UNRECOGNIZED;
//         }
//     }

//     uint16_t group_number() { return (raw_tag & GROUP_MASK) >> 40; }
//     uint8_t message_number() { return (raw_tag & MESSAGE_MASK) >> 32; }
//     uint16_t index() { return (raw_tag & INDEX_MASK) >> 16; }
//     uint16_t message_size() { return raw_tag & SIZE_MASK; }

//     uint64_t raw() { return raw_tag; }
// };
// struct shutdown_msg {
//     uint32_t padding;
// };

enum class MessageType : uint8_t {
    DATA_BLOCK = 0x01,
    SMALL_MESSAGE = 0x02,
    READY_FOR_BLOCK = 0x03,
    BARRIER = 0x04,
};

struct ParsedTag {
    MessageType message_type;
    uint16_t group_number;
    uint32_t target;
};

inline ParsedTag parse_tag(uint64_t t) {
    return ParsedTag{(MessageType)((t & 0x00ff000000000000ull) >> 48),
                     (uint16_t)((t & 0x0000ffff00000000ull) >> 32),
                     (uint32_t)(t & 0x00000000ffffffffull)};
}
inline uint64_t form_tag(uint16_t group_number, uint32_t target,
                         MessageType message_type) {
    return (((uint64_t)message_type) << 48) | (((uint64_t)group_number) << 32) |
           (uint64_t)target;
}

struct ParsedImmediate {
    uint16_t total_blocks;
    uint16_t block_number;
};

inline ParsedImmediate parse_immediate(uint32_t imm) {
    return ParsedImmediate{(uint16_t)((imm & 0xffff0000) >> 16),
                           (uint16_t)(imm & 0x0000ffff)};
}
inline uint32_t form_immediate(uint16_t total_blocks, uint16_t block_number) {
    return ((uint32_t)total_blocks) << 16 | ((uint32_t)block_number);
}

#endif
