
#include "group_send.h"

#include <cassert>
#include <climits>
#include <sys/mman.h>

using namespace std;
using namespace rdmc;

static const size_t buffer_size = 256<<20;


group::group(uint16_t _group_number, size_t _block_size,
             vector<uint16_t> _members, completion_callback_t callback,
             completion_callback_t ss_callback):
    group_number(_group_number),
    block_size(_block_size),
    chunks_per_block(1), // other values not supported
    members(_members),
    member_index(index_of(members, node_rank)),
    chunk_size(block_size / chunks_per_block),
    first_block_buffer(nullptr),
    completion_callback(callback),
    small_send_callback(ss_callback){

    if(member_index != 0){
        buffer = (char*)mmap(NULL, buffer_size, PROT_READ|PROT_WRITE, 
                             MAP_ANON|MAP_PRIVATE, -1, 0);
        memset(buffer, 1, buffer_size);
        memset(buffer, 0, buffer_size);

        first_block_buffer = (char*)mmap(NULL, block_size, PROT_READ|PROT_WRITE, 
                                         MAP_ANON|MAP_PRIVATE, -1, 0);
        memset(first_block_buffer, 1, block_size);
        memset(first_block_buffer, 0, block_size);
    }
}
group::~group(){
    munmap(buffer, buffer_size);
    munmap(first_block_buffer, block_size);
    // TODO: Destroy group and free all used resources.
}
void group::receive_chunk(tag_t tag){
    size_t block_number = tag.index() / chunks_per_block;

    assert(member_index > 0);
    assert(tag.message_type() == tag_t::type::DATA_BLOCK);
    assert(tag.group_number() == group_number);
    assert(tag.message_number() == message_number);
    
    if(receive_step == 0){
        if(++first_block_chunks == chunks_per_block){
            LOG_EVENT(group_number, message_number, block_number,
                      "received_first_block");
            
            num_blocks = (tag.message_size() - 1) / chunks_per_block + 1;
            size = tag.message_size() * block_size / chunks_per_block;

            first_block_number = min(get_first_block(member_index), num_blocks-1);

            num_received_blocks = 1;
            received_chunks = vector<uint16_t>(num_blocks);
            received_chunks[*first_block_number] = chunks_per_block;

            LOG_EVENT(group_number, message_number, *first_block_number,
                      "initialized_internal_datastructures");

            assert(receive_step == 0);
            auto transfer = get_incoming_transfer(receive_step);
            while((!transfer || transfer->block_number == *first_block_number) &&
                  receive_step < get_total_steps()){
                transfer = get_incoming_transfer(++receive_step);
            }
            LOG_EVENT(group_number, message_number, *first_block_number,
                      "found_next_transfer");

            if(transfer){
                LOG_EVENT(group_number, message_number, transfer->block_number,
                          "posting_recv");
                // printf("Posting recv #%d (receive_step = %d, *first_block_number = %d, total_steps = %d)\n", 
                //        (int)transfer->block_number, (int)receive_step, (int)*first_block_number, (int)get_total_steps());
                post_recv(transfer->block_number);
            }

            LOG_EVENT(group_number, message_number, *first_block_number,
                      "calling_send_next_block");

            send_next_block();

            LOG_EVENT(group_number, message_number, *first_block_number,
                      "returned_from_send_next_block");

            if(!sending && send_step == get_total_steps() && 
               num_received_blocks == num_blocks){
                complete_message();
            }
        }
    } else {
        assert(tag.index() <= tag.message_size());

        uint16_t num_chunks_received = ++received_chunks[block_number];
        uint16_t chunks_in_block = chunks_per_block;

        if(block_number == num_blocks - 1){
            size_t total_chunks = (size - 1) / chunk_size + 1;
            chunks_in_block = (total_chunks - 1) % chunks_per_block + 1;
        }

//    printf("Block #%d: %d/%d\n", (int)block_number, (int)num_chunks_received, chunks_in_block);
//    fflush(stdout);

        assert(num_chunks_received <= chunks_in_block);
        if(num_chunks_received == chunks_in_block){
            LOG_EVENT(group_number, message_number, block_number, "received_block");


            // Figure out the next block to receive.
            auto transfer = get_incoming_transfer(++receive_step);
            while(!transfer && receive_step < get_total_steps()){
                transfer = get_incoming_transfer(++receive_step);
            }
            // Post a receive for it.
            if(transfer){
                post_recv(transfer->block_number);
            }

            // If we just finished receiving a block and we weren't
            // previously sending, then try to send now.
            if(!sending){
                send_next_block();
            }
            // If we just received the last block then issue a completion callback
            if(++num_received_blocks == num_blocks && !sending && 
               send_step == get_total_steps()){
                complete_message();
            }
        }
    }
}
void group::complete_chunk_send(){
    if(!(chunks_left > 0 && chunks_left <= chunks_per_block))
        printf("chunks_left = %d\n", (int)chunks_left);
    assert(chunks_left > 0 && chunks_left <= chunks_per_block); 

    if(--chunks_left == 0){
        LOG_EVENT(group_number, message_number, outgoing_block,
                  "finished_sending_block");

        send_next_block();

        // If we just send the last block, and were already done
        // receiving, then signal completion and prepare for the next
        // message.
        if(!sending && send_step == get_total_steps() && 
           (member_index == 0 || num_received_blocks == num_blocks)){
            complete_message();
        }
    }
}
void group::send_message(char* data, size_t data_size){
    assert(data_size > 0);
    assert(member_index == 0);
    assert(receive_step == 0); // Queueing sends is not supported.
    assert(data_size <= (1<<30));

    buffer = data;
    size = data_size;
    num_blocks = (size - 1) / block_size + 1;

    LOG_EVENT(group_number, message_number, -1, "send_message");

    send_next_block();
    // No need to worry about completion here. We must send at least
    // one block, so we can't be done already.
}
void group::small_send(char* data, uint16_t data_size){
    LOG_EVENT(group_number, -1,-1, "small_send()");

    uint64_t tag = (uint64_t)tag_t::type::SMALL_SEND
        | ((uint64_t)group_number << 40)
        | ((uint64_t)size);

    psm_mq_req_t req;
    for(uint16_t target : members){
        if(target == member_index)
            continue;

        request_context* context = new request_context;
        context->data = buffer;
        context->tag = tag_t(tag);
        context->is_send = true;

        psm_mq_isend(mq, epaddrs[members[target]], PSM_MQ_FLAG_SENDSYNC,
                     tag, context->data, size, context, &req);
    }
    LOG_EVENT(group_number, -1,-1, "initiated_small_send");
}
void group::init(){
    if(member_index > 0)
        post_first_block();
}
void group::send_next_block(){
    sending = false;
    if(send_step == get_total_steps()){
        return;
    }
    auto transfer = get_outgoing_transfer(send_step);
    while(!transfer){
        if(++send_step == get_total_steps())
            return;

        transfer = get_outgoing_transfer(send_step);
    }

    size_t target = transfer->target;
    size_t block_number = transfer->block_number;
    size_t forged_block_number = transfer->forged_block_number;
    chunks_left = chunks_per_block;

    if(member_index > 0 && received_chunks[block_number] < chunks_per_block)
        return;

    sending = true;
    ++send_step;

//    printf("sending block #%d to node #%d\n", (int)block_number, (int)target);
//    fflush(stdout);

    psm_mq_req_t req;
    for(unsigned int chunk = 0; chunk < chunks_per_block; chunk++){
        char* ptr = buffer + block_number * block_size 
            + chunk * chunk_size;
        size_t nbytes = min(chunk_size, (size_t)(buffer + size - ptr));

        if(first_block_number && block_number == *first_block_number)
            ptr = first_block_buffer;

        if(nbytes == 0){
            // If this is the last block and we are sending fewer than
            // chunks_per_block chunks, then adjust chunks_left
            // accordingly
            chunks_left -= chunks_per_block - chunk;
            break;
        }

        uint64_t tag = (uint64_t)tag_t::type::DATA_BLOCK
            | ((uint64_t)group_number << 40)
            | ((uint64_t)message_number << 32)
            | ((uint64_t)(forged_block_number * chunks_per_block + chunk) << 16)
            | ((uint64_t)((size - 1) / chunk_size + 1));

        request_context* context = new request_context;
        context->data = ptr;
        context->tag = tag_t(tag);
        context->is_send = true;

        psm_mq_isend(mq, epaddrs[members[target]], PSM_MQ_FLAG_SENDSYNC,
                     tag, context->data, nbytes, context, &req);
    }
    outgoing_block = block_number;
    LOG_EVENT(group_number, message_number, block_number, "started_sending_block");
}
            
void group::complete_message(){
    // remap first_block into buffer
    if(member_index > 0 && first_block_number){
        LOG_EVENT(group_number, message_number, *first_block_number,
                  "starting_remap_first_block");
        if(block_size > (128<<10) && (block_size % 4096 == 0)){
            char* tmp_buffer = (char*)mmap(NULL, block_size, 
                                           PROT_READ|PROT_WRITE,
                                           MAP_ANON|MAP_PRIVATE, -1, 0);
            
            mremap(buffer + block_size * (*first_block_number), block_size,
                   block_size, MREMAP_FIXED | MREMAP_MAYMOVE, tmp_buffer);

            mremap(first_block_buffer, block_size, block_size, 
                   MREMAP_FIXED | MREMAP_MAYMOVE, 
                   buffer + block_size * (*first_block_number));
            first_block_buffer = tmp_buffer;
        }else{
            memcpy(buffer + block_size * (*first_block_number), first_block_buffer, 
                   block_size);
        }
        LOG_EVENT(group_number, message_number, *first_block_number,
                  "finished_remap_first_block");
    }
    completion_callback(group_number, rdmc::SUCCESS, buffer);

    ++message_number;
    sending = false;
    send_step = 0;
    receive_step = 0;
    chunks_left = 0;
    first_block_chunks = 0;
    // if(first_block_buffer == nullptr && member_index > 0){
    //     first_block_buffer = (char*)mmap(NULL, block_size, PROT_READ|PROT_WRITE, 
    //                                      MAP_ANON|MAP_PRIVATE, -1, 0);
    //     memset(first_block_buffer, 1, block_size);
    //     memset(first_block_buffer, 0, block_size);
    // }
    first_block_number = boost::none;

    if(member_index != 0){
        num_received_blocks = 0;
        received_chunks.clear();
        post_first_block();
    }
}
void group::post_recv(size_t block_number){
    psm_mq_req_t req;
    uint64_t mask = tag_t::TYPE_MASK
        | tag_t::GROUP_MASK
        | tag_t::MESSAGE_MASK
        | tag_t::INDEX_MASK;

    for(int chunk = 0; chunk < chunks_per_block; chunk++){
        char* ptr = buffer + block_size * block_number + chunk_size * chunk;
        size_t nbytes = min(chunk_size, (size_t)(buffer + size - ptr));

        if(nbytes == 0)
            break;

        uint64_t tag = (int64_t)tag_t::type::DATA_BLOCK
            | ((uint64_t)group_number << 40)
            | ((uint64_t)message_number << 32)
            | ((uint64_t)(block_number * chunks_per_block + chunk) << 16);

        request_context* context = new request_context;
        context->data = ptr;
        context->tag = tag_t(tag);
        context->is_send = false;

        psm_mq_irecv(mq, tag, mask, 0, ptr, nbytes, context, &req);
    }
    LOG_EVENT(group_number, message_number, block_number,
              "posted_receive_buffer");

}
void group::post_first_block(){
    size_t block_number = get_first_block(member_index);

    uint64_t mask = tag_t::TYPE_MASK
        | tag_t::GROUP_MASK
        | tag_t::MESSAGE_MASK
        | tag_t::INDEX_MASK;

    for(int chunk = 0; chunk < chunks_per_block; chunk++){
        char* ptr = first_block_buffer + chunk_size * chunk;

        uint64_t tag = (int64_t)tag_t::type::DATA_BLOCK
            | ((uint64_t)group_number << 40)
            | ((uint64_t)message_number << 32)
            | ((uint64_t)(block_number * chunks_per_block + chunk) << 16);

        request_context* context = new request_context;
        context->data = ptr;
        context->tag = tag_t(tag);
        context->is_send = false;

        psm_mq_irecv(mq, tag, mask, 0, ptr, chunk_size, context,
                     &first_block_request);
    }
    LOG_EVENT(group_number, message_number, block_number, "posted_first_block");
}
size_t chain_group::get_total_steps(){
    size_t total_steps = num_blocks + members.size() - 2;
    return total_steps;
}
optional<group::block_transfer> chain_group::get_outgoing_transfer(size_t step){
    size_t block_number = step - member_index;
    
    if(member_index > step || block_number >= num_blocks ||
       member_index == members.size() - 1){
        return boost::none;
    }

    return block_transfer{(uint16_t)(member_index + 1), block_number, block_number};
}
optional<group::block_transfer> chain_group::get_incoming_transfer(size_t step){
    size_t block_number = (step + 1) - member_index;
    if(member_index > step+1 || block_number >= num_blocks ||
       member_index == 0){
        return boost::none;
    }
    return block_transfer{(uint16_t)(member_index - 1), block_number, block_number};
}
size_t chain_group::get_first_block(uint16_t receiver){
    return 0;
}


size_t sequential_group::get_total_steps(){
    return num_blocks * (members.size()-1);
}
optional<group::block_transfer> sequential_group::get_outgoing_transfer(size_t step){
    if(member_index > 0 || step >= num_blocks*(members.size()-1)){
        return boost::none;
    }

    size_t block_number = step % num_blocks;
    return block_transfer{(uint16_t)(1 + step / num_blocks), block_number, block_number};
}
optional<group::block_transfer> sequential_group::get_incoming_transfer(size_t step){
    if(1 + step / num_blocks != member_index){
        return boost::none;
    }

    size_t block_number = step % num_blocks;
    return block_transfer{(uint16_t)0, block_number, block_number};
}
size_t sequential_group::get_first_block(uint16_t receiver){
    return 0;
}


size_t tree_group::get_total_steps(){
    unsigned int log2_num_members = ceil(log2(members.size()));
    return num_blocks * log2_num_members;
}
optional<group::block_transfer> tree_group::get_outgoing_transfer(size_t step){
    size_t stage = step / num_blocks;
    uint16_t neighbor = member_index ^ ((uint16_t)1 << stage);

    if(member_index >= (1<<stage) || stage >= 16 || neighbor < member_index || 
       neighbor >= members.size()){
        return boost::none;
    }

    size_t block_number = step % num_blocks;
    return block_transfer{neighbor, block_number, block_number};
}
optional<group::block_transfer> tree_group::get_incoming_transfer(size_t step){
    size_t stage = step / num_blocks;
    uint16_t neighbor = member_index ^ ((uint16_t)1 << stage);

    
    if(stage >= 16 || !((1<<stage) <= member_index && member_index < (2 << stage))){
        return boost::none;
    }

    size_t block_number = step % num_blocks;
    return block_transfer{neighbor, block_number, block_number};
}
size_t tree_group::get_first_block(uint16_t receiver){
    return 0;
}


size_t binomial_group::get_total_steps(){
    size_t total_steps = num_blocks + floor(log2(members.size())) - 1;
    return total_steps;
}
optional<group::block_transfer> binomial_group::get_outgoing_transfer(uint16_t sender, 
                                                                      size_t send_step){
/*
 * During a typical step, the rotated rank will indicate:
 *  
 *   0000...0001 -> block = send_step
 *   1000...0001 -> block = send_step - 1
 *   x100...0001 -> block = send_step - 2
 *   xx10...0001 -> block = send_step - 3
 *
 *   xxxx...xx11 -> block = send_step - log2_num_members + 1
 *   xxxx...xxx0 -> block = send_step - log2_num_members
 */

    unsigned int log2_num_members = floor(log2(members.size()));

    size_t rank_mask = (~((size_t)0)) >> (sizeof(size_t) * CHAR_BIT - log2_num_members);

    size_t step_index = send_step % log2_num_members;
    uint16_t neighbor = sender ^ (1 << step_index);
    if(/*log2(member_index|neighbor) > send_step+1 ||*/ send_step >= get_total_steps() ||
       neighbor == 0){
//        printf("send_step = %d, neighbor = %d, log2(...) = %f\n", (int)send_step, (int)neighbor, log2(member_index|neighbor));
//        fflush(stdout);
        return boost::none;
    }

    
    size_t rotated_rank = ((neighbor | (neighbor << log2_num_members)) 
                           >> step_index) & rank_mask;
//    printf("send_step = %d, rotated_rank = %x\n", (int)send_step, (int)rotated_rank);
//    fflush(stdout);

    if((rotated_rank & 1) == 0){
        if(send_step < log2_num_members){
//            printf("send_step < log2_num_members\n");
//            fflush(stdout);
            return boost::none;
        }
        return block_transfer{neighbor, send_step - log2_num_members, send_step - log2_num_members};
    }

    for(unsigned int index = 1; index < log2_num_members; index++){
        if(rotated_rank & (1 << index)){
            if(send_step + index < log2_num_members){
//                printf("send_step + index < log2_num_members\n");
//                fflush(stdout);
                return boost::none;
            }
            size_t forged_block_number = send_step + index - log2_num_members;
            size_t block_number = min(forged_block_number, num_blocks - 1);
            if(forged_block_number != get_first_block(neighbor))
                forged_block_number = block_number;
            return block_transfer{neighbor, block_number, forged_block_number};
        }
    }

    size_t forged_block_number = send_step;
    size_t block_number = min(forged_block_number, num_blocks - 1);
    if(forged_block_number != get_first_block(neighbor))
        forged_block_number = block_number;

    return block_transfer{neighbor, block_number, forged_block_number};
}
optional<group::block_transfer> binomial_group::get_outgoing_transfer(size_t send_step){
    return get_outgoing_transfer(member_index, send_step);
}
optional<group::block_transfer> binomial_group::get_incoming_transfer(size_t send_step){
    unsigned int log2_num_members = floor(log2(members.size()));

    size_t step_index = send_step % log2_num_members;
    uint16_t neighbor = member_index ^ (1 << step_index);

    auto transfer = get_outgoing_transfer(neighbor, send_step);
    if(!transfer) return boost::none;
    return block_transfer{neighbor, transfer->block_number, transfer->forged_block_number};
}
size_t binomial_group::get_first_block(uint16_t receiver){
    for(size_t n = 0; n < 16; n++){
        if((receiver & (1 << n)) != 0)
            return n;
    }

    return 0;
}
