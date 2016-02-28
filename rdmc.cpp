
#include "group_send.h"
#include "message.h"
#include "microbenchmarks.h"
#include "rdmc.h"
#include "util.h"
#include "verbs_helper.h"

#include <algorithm>
#include <atomic>
#include <cassert>
#include <chrono>
#include <cinttypes>
#include <condition_variable>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iostream>
#include <list>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <sstream>
#include <string>
#include <sys/mman.h>
#include <sys/resource.h>
#include <thread>
#include <queue>
#include <unistd.h>
#include <vector>

using namespace std;

namespace rdmc {
unsigned int job_number;
unsigned int job_step;
uint32_t node_rank;
uint32_t num_nodes;
vector<string> node_addresses;
	
// const unsigned int NUM_SMALL_SENDS = 1024;
// const size_t SMALL_SEND_SIZE = 0xffff;
// char *small_send_buffers[NUM_SMALL_SENDS];
// shutdown_msg shutdown_receive_buffer;

// queue<std::function<void()> > queued_operations;
// mutex queued_operations_lock;
// condition_variable queued_operations_cv;
// atomic<bool> queued_operation_flag;

atomic<bool> shutdown_flag;

// map from group number to group
map<uint16_t, shared_ptr<group> > groups;
mutex groups_lock;

struct {
    // Queue Pairs and associated remote memory regions used for performing a
    // barrier.
    vector<queue_pair> queue_pairs;
    vector<remote_memory_region> remote_memory_regions;

    // Additional queue pairs which will handle incoming writes (but which this
    // node does not need to interact with directly).
    vector<queue_pair> extra_queue_pairs;

	// RDMA memory region used for doing the barrier
    array<volatile int64_t, 32> steps;
    unique_ptr<memory_region> steps_mr;

	// Current barrier number, and a memory region to issue writes from.
    volatile int64_t number = -1;
    unique_ptr<memory_region> number_mr;

	// Number of steps per barrier.
    unsigned int total_steps;

	// Lock to ensure that only one barrier is in flight at a time.
    mutex lock;
} barrier_state;

// static void issue_queued_operation(float timeout) {
//     std::unique_lock<std::mutex> lock(queued_operations_lock);

//     auto duration = chrono::nanoseconds((unsigned int)(timeout * 1.e6));
//     auto send_ready = [&]() { return !queued_operations.empty(); };
//     if(!send_ready()) {
//         if(timeout == 0 ||
//            !queued_operations_cv.wait_for(lock, duration, send_ready)) {
//             return;
//         }
//     }

//     // apply operation
//     (queued_operations.front())();

//     // remove from queue
//     queued_operations.pop();
//     LOG_EVENT(-1, -1, -1, "completed_queued_operation");

//     queued_operation_flag = !queued_operations.empty();
// }

static void main_loop() {
    const int max_work_completions = 1024;
    unique_ptr<ibv_wc[]> work_completions(new ibv_wc[max_work_completions]);

    while(true) {
        int num_completions = poll_for_completions(
            max_work_completions, work_completions.get(), shutdown_flag);

        if(shutdown_flag) {
            groups.clear();
            verbs_destroy();
            exit(0);
        }

        assert(num_completions > 0);  // Negative indicates an IBV error.

        for(int i = 0; i < num_completions; i++) {
            ibv_wc& wc = work_completions[i];
            auto tag = parse_tag(wc.wr_id);

            if(wc.status != 0) {
                printf("wc.status = %d; wc.wr_id = 0x%llx; imm = 0x%x\n",
                       (int)wc.status, (long long)wc.wr_id,
                       (unsigned int)wc.imm_data);
                fflush(stdout);
            } else if(wc.opcode == IBV_WC_SEND) {
                if(tag.message_type == MessageType::DATA_BLOCK) {
                    shared_ptr<group> g;

                    {
                        unique_lock<mutex> lock(groups_lock);
                        auto it = groups.find(tag.group_number);
                        assert(it != groups.end());
                        g = it->second;
                    }

                    g->complete_block_send();
                } else if(tag.message_type == MessageType::READY_FOR_BLOCK) {
                    //                    TRACE("Completed sending ready for
                    //                    block message.");
                } else {
                    printf(
                        "sent unrecognized message type?! (message_type=%d)\n",
                        (int)tag.message_type);
                }
            } else if(wc.opcode == IBV_WC_RECV) {
                if(tag.message_type == MessageType::DATA_BLOCK) {
                    assert(wc.wc_flags & IBV_WC_WITH_IMM);
                    shared_ptr<group> g;

                    {
                        unique_lock<mutex> lock(groups_lock);
                        auto it = groups.find(tag.group_number);
                        assert(it != groups.end());
                        g = it->second;
                    }

                    g->receive_block(wc.imm_data);
                } else if(tag.message_type == MessageType::READY_FOR_BLOCK) {
                    shared_ptr<group> g;

                    {
                        unique_lock<mutex> lock(groups_lock);
                        auto it = groups.find(tag.group_number);
                        if(it != groups.end())
							g = it->second;
						else
							TRACE("Got RFB for group not yet created...");
                    }

					if(g){
						g->receive_ready_for_block(wc.imm_data, tag.target);
					}
                } else {
                    printf(
                        "received message with unrecognized type, even though "
                        "we posted the buffer?! (message_type=%d)\n",
                        (int)tag.message_type);
                }
            } else if(wc.opcode == IBV_WC_RDMA_WRITE) {
                if(tag.message_type == MessageType::BARRIER) {
                    // TRACE("Completed performing barrier write.");
                }
            } else {
                puts("Sent unrecognized completion type?!");
            }
        }
    }
}

void initialize() {
    TRACE("starting initialize");

    init_environment();
    TRACE("env initialized");

    assert(verbs_initialize());

    TRACE("verbs initialized");

    // const size_t buffer_size = 1024 * 1024 * 1024;
    // char *buffer = (char *)mmap(NULL, buffer_size, PROT_READ | PROT_WRITE,
    //                             MAP_ANON | MAP_PRIVATE, -1, 0);
    // memset(buffer, 1, buffer_size);
    // memset(buffer, 0, buffer_size);

    // auto t1 = get_time();
    // memory_region mr(buffer, buffer_size);
    // auto t2 = get_time();
    // cout << "MR create time = " << (t2 - t1) / 1000 << " us" << endl;

    // if(node_rank == 0) {
    //     memset(buffer + 4096, 13, 4096);
    //     memset(buffer + 8192, 14, 4096);

    // } else {
    //     memset(buffer + 4096, 15, 4096);
    //     memset(buffer + 8192, 16, 4096);
    // }
    // cout << "Buffer[4096 + 12] = " << (int)buffer[4096 + 12] << endl;

    // t1 = get_time();
    // auto r = mremap(buffer + 4096, 4096, 4096, MREMAP_FIXED | MREMAP_MAYMOVE,
    //                 buffer + 8192);
    // t2 = get_time();
    // cout << "remap time = " << (t2 - t1) / 1000 << " us\n";
    // cout << "Buffer[8192 + 12] = " << (int)buffer[8192 + 12] << endl;

    // TRACE("Created MR");
    // if(node_rank == 0) {
    //     queue_pair qp(1);
    //     TRACE("Created QP");
    //     qp.post_send(mr, 4096, 4096);
    //     TRACE("Posted SEND");
    //     poll_for_completion();
    //     TRACE("Finished polling");
    // } else {
    //     queue_pair qp(0);
    //     TRACE("Created QP");
    //     qp.post_recv(mr, 4096, 4096);
    //     TRACE("Posted RECV");
    //     poll_for_completion();
    //     TRACE("Finished polling");
    //     cout << "Buffer[8192 + 12] = " << (int)buffer[8192 + 12] << endl;
    // }

    barrier_state.total_steps = ceil(log2(num_nodes));
    for(unsigned int m = 0; m < barrier_state.total_steps; m++)
        barrier_state.steps[m] = -1;
    barrier_state.steps_mr =
        make_unique<memory_region>((char*)&barrier_state.steps[0],
                                   barrier_state.total_steps * sizeof(int64_t));
    barrier_state.number_mr = make_unique<memory_region>(
        (char*)&barrier_state.number, sizeof(barrier_state.number));

    set<uint32_t> targets;
    for(unsigned int m = 0; m < barrier_state.total_steps; m++) {
        auto target = (node_rank + (1 << m)) % num_nodes;
        auto target2 =
            (num_nodes * (1 << m) + node_rank - (1 << m)) % num_nodes;
        targets.insert(target);
        targets.insert(target2);
    }

    map<uint32_t, queue_pair> qps;
    for(auto target : targets) {
        qps.emplace(target, queue_pair(target));
    }

    auto remote_mrs =
        verbs_exchange_memory_regions(*barrier_state.steps_mr.get());
    for(unsigned int m = 0; m < barrier_state.total_steps; m++) {
        auto target = (node_rank + (1 << m)) % num_nodes;

        barrier_state.remote_memory_regions.push_back(
            remote_mrs.find(target)->second);

        auto qp_it = qps.find(target);
        barrier_state.queue_pairs.push_back(std::move(qp_it->second));
		qps.erase(qp_it);
    }

	for(auto it = qps.begin(); it != qps.end(); it++){
        barrier_state.extra_queue_pairs.push_back(std::move(it->second));
		qps.erase(it);
	}
	
	cout << "total_steps = " << barrier_state.total_steps << endl;

    // if(node_rank == 0){
    //     for(int i = 1; i < num_nodes; i++){
    //         barriers.emplace_back(i);

    //         barriers.back().qp.post_empty_recv(
    //             form_tag(0, i, MessageType::BARRIER));
    //     }
    // } else {
    //     barriers.emplace_back(0);

    //     barriers.back().qp.post_empty_recv(
    //         form_tag(0, 0, MessageType::BARRIER));
    // }

    TRACE("Spawning main loop");
    thread t(main_loop);
    t.detach();
}

void create_group(uint16_t group_number, std::vector<uint32_t> members,
                  size_t block_size, send_algorithm algorithm,
                  incoming_message_callback_t incoming_upcall,
                  completion_callback_t callback) {
    shared_ptr<group> g;
    if(algorithm == BINOMIAL_SEND) {
        g = make_shared<binomial_group>(group_number, block_size, members,
                                        incoming_upcall, callback);
    } else if(algorithm == CHAIN_SEND) {
        g = make_shared<chain_group>(group_number, block_size, members,
                                     incoming_upcall, callback);
    } else if(algorithm == SEQUENTIAL_SEND) {
        g = make_shared<sequential_group>(group_number, block_size, members,
                                          incoming_upcall, callback);
    } else if(algorithm == TREE_SEND) {
        g = make_shared<tree_group>(group_number, block_size, members,
                                    incoming_upcall, callback);
    } else {
        puts("Unsupported group type?!");
        fflush(stdout);
    }

    unique_lock<mutex> lock(groups_lock);
    auto p = groups.emplace(group_number, std::move(g));
    assert(p.second);
    p.first->second->init();
}

void destroy_group(uint16_t group_number) {
    unique_lock<mutex> lock(groups_lock);
    LOG_EVENT(group_number, -1, -1, "destroy_group");
    groups.erase(group_number);
}
void shutdown() {
    shutdown_flag = true;

    while(1) {
        /* do nothing */;
    }
}
void send(uint16_t group_number, shared_ptr<memory_region> mr, size_t offset,
          size_t length) {
    shared_ptr<group> g;
    {
        unique_lock<mutex> lock(groups_lock);
        auto it = groups.find(group_number);
        assert(it != groups.end());
        g = it->second;
    }
    LOG_EVENT(group_number, -1, -1, "preparing_to_send_message");
    g->send_message(mr, offset, length);
}
void barrier() {
    // See:
    // http://mvapich.cse.ohio-state.edu/static/media/publications/abstract/kinis-euro03.pdf

    unique_lock<mutex> lock(barrier_state.lock);
    LOG_EVENT(-1, -1, -1, "start_barrier");
    barrier_state.number++;

    for(unsigned int m = 0; m < barrier_state.total_steps; m++) {
        assert(barrier_state.queue_pairs[m].post_write(
            *barrier_state.number_mr.get(), 0, 8,
            form_tag(0, (rdmc::node_rank + (1 << m)) % rdmc::num_nodes,
                     MessageType::BARRIER),
            barrier_state.remote_memory_regions[m], m * 8, false, true));

        while(barrier_state.steps[m] < barrier_state.number)
			/* do nothing*/;
    }

    // if(node_rank == 0) {
    //     for(auto& b : barriers){
    //         if(!b.triggered) {
    //             barriers_cv.wait(lock, [&b]() { return b.triggered; });
    //         }
    //         b.qp.post_empty_send(form_tag(0, 0, MessageType::BARRIER),
    //                                        0);
    //         b.triggered = false;
    //     }
    // } else {
    //     auto& b = barriers.front();
    //     b.qp.post_empty_send(form_tag(0, node_rank, MessageType::BARRIER),
    //     0);
    //     if(!b.triggered) {
    //         barriers_cv.wait(lock, [&b]() { return b.triggered; });
    //     }
    //     b.triggered = false;
    // }

    LOG_EVENT(-1, -1, -1, "end_barrier");
}
}
