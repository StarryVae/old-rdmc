
#ifndef MESSAGE_H
#define MESSAGE_H

#include <utility>

class tag_t {
    uint64_t raw_tag;

public:
    enum struct type: uint64_t {DATA_BLOCK = 1ull<<56,
            SHUTDOWN = 2ull<<56,
            SMALL_SEND = 3ull<<56,
            UNRECOGNIZED = 0xffull<<56};

    static constexpr uint64_t TYPE_MASK    = 0xff00000000000000ull;
    static constexpr uint64_t GROUP_MASK   = 0x00ffff0000000000ull;
    static constexpr uint64_t MESSAGE_MASK = 0x000000ff00000000ull;
    static constexpr uint64_t INDEX_MASK   = 0x00000000ffff0000ull;
    static constexpr uint64_t SIZE_MASK    = 0x000000000000ffffull;
    
    tag_t(): raw_tag(~0ull){}
    tag_t(uint64_t t): raw_tag(t){}
    tag_t(type t): raw_tag(static_cast<uint64_t>(t)){}

    type message_type() {
        uint64_t t = (raw_tag & TYPE_MASK) >> 56;
        switch(t) {
        case 1: return type::DATA_BLOCK;
        case 2: return type::SHUTDOWN;
        case 3: return type::SMALL_SEND;
        default: return type::UNRECOGNIZED;
        }
    }

    uint16_t group_number() {return (raw_tag & GROUP_MASK) >> 40;}
    uint8_t message_number() {return (raw_tag & MESSAGE_MASK) >> 32;}
    uint16_t index() {return (raw_tag & INDEX_MASK) >> 16;}
    uint16_t message_size() {return raw_tag & SIZE_MASK;}

    uint64_t raw(){return raw_tag;}
};
struct shutdown_msg {
    uint32_t padding;
};

#endif
