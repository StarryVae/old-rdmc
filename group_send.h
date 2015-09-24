
#ifndef GROUP_SEND_H
#define GROUP_SEND_H

#include "util.h"
#include "rdmc.h"
#include "message.h"
#include "psm_helper.h"

#include <boost/optional.hpp>
#include <queue>
#include <vector>
#include <memory>

using boost::optional;
using std::vector;
using std::unique_ptr;
using rdmc::completion_callback_t;

class group {
protected:
    const uint16_t group_number;
    const size_t block_size;
    const uint16_t chunks_per_block;
    const vector<uint16_t> members;  // first element is the sender
    const uint16_t member_index;     // our index in the members list
    const size_t chunk_size;         // derived from block_size and
                                     // chunks_per_block.

    optional<size_t> first_block_number;
    uint16_t first_block_chunks = 0;
    psm_mq_req_t first_block_request;
    char* first_block_buffer;
    
    uint8_t message_number = 0;
    char* buffer = nullptr;
    size_t size;
    size_t num_blocks;

    size_t outgoing_block;
    bool sending = false;   // Whether a block send is in progress
    size_t send_step = 0;   // Number of blocks sent/stalls so far
    size_t chunks_left = 0; // How many chunks of the current block
                            // being sent have yet to complete.

    // Total number of blocks received and the number of chunks
    // received for ecah block, respectively.
    size_t num_received_blocks = 0;
    size_t receive_step = 0;
    vector<uint16_t> received_chunks;

    struct block_transfer{
        uint16_t target;
        size_t block_number;
        size_t forged_block_number;
    };

    virtual optional<block_transfer> get_outgoing_transfer(size_t send_step)=0;
    virtual optional<block_transfer> get_incoming_transfer(size_t receive_step)=0;
    virtual size_t get_total_steps()=0;
    virtual size_t get_first_block(uint16_t receiver)=0;

public:
    completion_callback_t completion_callback;
    completion_callback_t small_send_callback;

    group(uint16_t group_number, size_t block_size, vector<uint16_t> members,
          completion_callback_t callback, completion_callback_t ss_callback);
    ~group();
    
    void receive_chunk(tag_t tag);
    void complete_chunk_send();
    void send_message(char* data, size_t size);
    void small_send(char* data, uint16_t size);
    void init();

private:
    void post_recv(size_t block_number);
    void post_first_block();
    void send_next_block();
    void complete_message();
    void prepare_for_next_message();
};
class chain_group: public group{
public:
    using group::group;

    optional<block_transfer> get_outgoing_transfer(size_t send_step);
    optional<block_transfer> get_incoming_transfer(size_t receive_step);
    size_t get_total_steps();
    size_t get_first_block(uint16_t receiver);
};
class sequential_group: public group{
public:
    using group::group;

    optional<block_transfer> get_outgoing_transfer(size_t send_step);
    optional<block_transfer> get_incoming_transfer(size_t receive_step);
    size_t get_total_steps();
    size_t get_first_block(uint16_t receiver);
};
class tree_group: public group{
public:
    using group::group;

    optional<block_transfer> get_outgoing_transfer(size_t send_step);
    optional<block_transfer> get_incoming_transfer(size_t receive_step);
    size_t get_total_steps();
    size_t get_first_block(uint16_t receiver);
};
class binomial_group: public group {
private:
    optional<block_transfer> get_outgoing_transfer(uint16_t sender, size_t send_step);
public:
    using group::group;

    optional<block_transfer> get_outgoing_transfer(size_t send_step);
    optional<block_transfer> get_incoming_transfer(size_t receive_step);
    size_t get_total_steps();
    size_t get_first_block(uint16_t receiver);
};

#endif /* GROUP_SEND_H */
