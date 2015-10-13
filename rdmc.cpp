
#include "util.h"
#include "psm_helper.h"
#include "message.h"
#include "rdmc.h"
#include "group_send.h"
#include "microbenchmarks.h"

#include <algorithm>
#include <atomic>
#include <cassert>
#include <chrono>
#include <cinttypes>
#include <condition_variable>
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
#include <vector>
#include <sched.h>

using namespace std;

namespace rdmc{
    unsigned int job_number;
    unsigned int job_step;
    uint16_t node_rank;
    uint16_t num_nodes;

//used to initialize the mq
    map<unsigned int, uint64_t> connections;
//used to send/receive data
    psm_mq_t mq;
    vector<psm_epaddr_t> epaddrs;

    const unsigned int NUM_SMALL_SENDS = 1024;
    const size_t SMALL_SEND_SIZE = 0xffff;
    char* small_send_buffers[NUM_SMALL_SENDS];

    vector<request_context*> unposted_small_sends;

    request_context shutdown_request_context;
    shutdown_msg shutdown_receive_buffer;

    queue<std::function<void()>> queued_operations;
    mutex queued_operations_lock;
    condition_variable queued_operations_cv;
    atomic<bool> queued_operation_flag;

    atomic<bool> shutdown_flag;

    //map from group number to group
    map<uint16_t, unique_ptr<group>> groups;

    static void issue_queued_operation(float timeout){
        std::unique_lock<std::mutex> lock(queued_operations_lock);

        auto duration = chrono::nanoseconds((unsigned int)(timeout * 1.e6));
        auto send_ready = [&](){return !queued_operations.empty();};
        if(!send_ready()) {
            if(timeout == 0 ||
               !queued_operations_cv.wait_for(lock, duration, send_ready)){
                return;
            }
        }

        // apply operation
        (queued_operations.front())();

        // remove from queue
        queued_operations.pop();
        LOG_EVENT(-1,-1,-1, "completed_queued_operation");

        queued_operation_flag = !queued_operations.empty();
    }

    static void main_loop(){
        bool fast_poll = true;

        psm_mq_req_t req;
        while(true){
            if(shutdown_flag){
                TRACE("Finalizing PSM");
                psm_mq_finalize(mq);
                psm_finalize();

                TRACE("Exiting...!");
                exit(0);
            }else if(fast_poll){
                if(psm_mq_ipeek(mq, &req, NULL) != PSM_OK){
                    bool got_event = false;
                    uint64_t end_time = get_time() + 15000000;
                    while(get_time() < end_time && !got_event){
                        if(queued_operation_flag)
                            issue_queued_operation(0.0);
                        got_event = psm_mq_ipeek(mq, &req, NULL) == PSM_OK;
                    }
                    if(!got_event){
//                        fast_poll = false;
                        continue;
                    }
                }
            }else if(psm_mq_ipeek(mq, &req, NULL) != PSM_OK){
                issue_queued_operation(1.0);
                continue;
            }
        
            fast_poll = true;
        
//        uint64_t t = get_time();

            psm_mq_status_t status;

            psm_error_t e = psm_mq_test(&req, &status);
            assert(e == PSM_OK);

            request_context context = *(request_context*)status.context;
            tag_t::type msg_type = context.tag.message_type();

            if(context.is_send){
                if(msg_type == tag_t::type::SHUTDOWN){
                    puts("Send completed (SHUTDOWN)");
                    fflush(stdout);
                    delete (shutdown_msg*)context.data;
                }
                else if(msg_type == tag_t::type::DATA_BLOCK){
                    TRACE("Send completed (DATA_BLOCK)");

                    auto it = groups.find(context.tag.group_number());
                    assert(it != groups.end());
                    it->second->complete_chunk_send();
                }else if(msg_type == tag_t::type::SMALL_SEND){
                    TRACE("Send completed (SMALL_SEND)");

                    LOG_EVENT(context.tag.group_number(), -1, -1,
                              "completed_small_send");

                    auto it = groups.find(context.tag.group_number());
                    assert(it != groups.end());

                    it->second->small_send_callback(context.tag.group_number(),
                                                    completion_status::SUCCESS,
                                                    (char*)context.data);
                }else{
                    puts("Sent unrecognized message type?!");
                }
            
                delete (request_context*)status.context;
            }else{
                tag_t tag{status.msg_tag};
                if(msg_type == tag_t::type::SHUTDOWN){
                    TRACE("Got shutdown message. Exiting...");

                    psm_mq_finalize(mq);
                    psm_finalize();
                    exit(0);
                }else if(msg_type == tag_t::type::DATA_BLOCK){
                    TRACE("Got data block");
                    auto it = groups.find(tag.group_number());
                    assert(it != groups.end());

                    it->second->receive_chunk(tag);
                    delete (request_context*)status.context;
                }else if(msg_type == tag_t::type::SMALL_SEND){
                    auto it = groups.find(context.tag.group_number());
                    assert(it != groups.end());

                    it->second->small_send_callback(context.tag.group_number(),
                                                    completion_status::SUCCESS,
                                                    (char*)context.data);


                    unposted_small_sends.emplace_back((request_context*)status.context);
                    if(unposted_small_sends.size() >= NUM_SMALL_SENDS / 2){
                        for(auto context : unposted_small_sends){
                            psm_mq_irecv(mq, context->tag.raw(), tag_t::TYPE_MASK, 0,
                                         context->data, SMALL_SEND_SIZE, context, &req);
                        }
                        unposted_small_sends.clear();
                    }
                }else{
                    put_flush("Got unrecognized packet type.");
                }
            }
        }
    }
    void print_stats(){
        psm_mq_stats_t stats;
        psm_mq_get_stats(mq, &stats);

#define PRINT_STAT(s) printf(#s " = %d\n", (int)s);
        PRINT_STAT(stats.rx_user_num);
        PRINT_STAT(stats.rx_sys_num);
        PRINT_STAT(stats.tx_num);
        PRINT_STAT(stats.tx_eager_num);
        PRINT_STAT(stats.tx_rndv_num);
        PRINT_STAT(stats.tx_shm_num);
        PRINT_STAT(stats.rx_shm_num);
        PRINT_STAT(stats.rx_sysbuf_num);
        fflush(stdout);
    }
    static void psm_initialize(){
        int major = PSM_VERNO_MAJOR;
        int minor = PSM_VERNO_MINOR;

//    setenv("PSM_DEVICES","self,ipath", 1);
//    setenv("PSM_TRACEMASK", "0x183", 1);
//    setenv("PSM_RCVTHREAD_FREQ", "500:1000:1", 1);

        psm_init(&major, &minor);
        LOG_EVENT(-1,-1,-1,"psm initialized");
        psm_ep_t ep;
        psm_epid_t epid;
        struct psm_ep_open_opts opts;
        psm_uuid_t uuid = {
            (uint8_t)(job_number & 0xff),       (uint8_t)((job_number>>8) & 0xff),
            (uint8_t)((job_number>>16) & 0xff), (uint8_t)((job_number>>24) & 0xff),
            (uint8_t)(job_step & 0xff),         (uint8_t)((job_step>>8) & 0xff),
            (uint8_t)0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0, 0, 0};

        psm_ep_open_opts_get_defaults(&opts);
//    opts.affinity = PSM_EP_OPEN_AFFINITY_SKIP;
        psm_ep_open(uuid, &opts, &ep, &epid);

        TRACE("ep open");
        //std::this_thread::sleep_for(std::chrono::seconds(20));
        find_connections2(epid);
        TRACE("connections found");
        vector<psm_epid_t> epids;
        for(int i = 0; i < num_nodes; i++){
            if(i != node_rank){
                epids.push_back((psm_epid_t)connections[i]);
//            cout << i << " " << connections[i] << endl;
            }
        }

        vector<psm_error_t> errors((epids.size()));
        epaddrs = vector<psm_epaddr_t> ((epids.size()));

        //boost::this_thread::sleep(boost::posix_time::milliseconds(30000));
        TRACE("ep connecting");
        psm_ep_connect(ep, epids.size(), &epids[0], NULL, 
                       &errors[0], &epaddrs[0], 0);

        //add placeholder for this node to epids array
        epaddrs.insert(epaddrs.begin()+node_rank, 0);

        TRACE("ep connectioned");
        psm_mq_init(ep, PSM_MQ_ORDERMASK_ALL, NULL, 0, &mq);
        TRACE("mp initialized");

        uint32_t devunits = 0;
        psm_ep_num_devunits(&devunits);
        if(devunits != 1){
            printf("num_devunits = %d\n", devunits);
            fflush(stdout);
        }
//    put_flush("Doing ping pong test");
//    ping_pong();    
//    print_stats();
//    sbandwidth_test();
//    sbandwidth_test2();
//    bandwidth_test();
//    memtest();

        //std::this_thread::sleep_for(std::chrono::seconds(1));

        // for(int i = 0; i < 2; i++){
        //     do_barrier();
        //     ping_pong();

        //     do_barrier();
        //     latency_test();

        //     do_barrier();
        //     blocking_send_latency_test();
        // }
        do_barrier();
        bandwidth_test();
        do_barrier();

        TRACE("posting receives");

        psm_mq_req_t req;
        request_context* context = &shutdown_request_context;
        context->data = &shutdown_receive_buffer;
        context->tag = tag_t(tag_t::type::SHUTDOWN);
        context->is_send = false;
        psm_mq_irecv(mq, context->tag.raw(), tag_t::TYPE_MASK, 0, context->data,
                     sizeof(shutdown_msg), context, &req);

        for(unsigned int i = 0; i < NUM_SMALL_SENDS; i++){
            small_send_buffers[i] = new char[SMALL_SEND_SIZE];
            memset(small_send_buffers[i], 0, SMALL_SEND_SIZE);
            request_context* context = new request_context;
            context->data = small_send_buffers[i];
            context->tag = tag_t(tag_t::type::SMALL_SEND);
            context->is_send = false;
            psm_mq_irecv(mq, context->tag.raw(), tag_t::TYPE_MASK, 0,
                         context->data, SMALL_SEND_SIZE, context, &req);
        }

        thread t(main_loop);
        t.detach();
        //boost::this_thread::sleep(boost::posix_time::milliseconds(1000));
        //say_hello();
        //    sequential_send();
        //tree_send(2);
        //tree_send(1);

//    const size_t buffer_size = 1u << 30;
//    char* buffer = (char*)malloc(buffer_size);
//    assert(buffer);
//    tree_multicast(0, buffer, buffer_size);
//    chain_multicast(0, buffer, buffer_size);
//    tree_multicast(0, buffer, block_size);
//    chain_multicast(0, buffer, block_size);
//    free(buffer);
    }

    void initialize(){
        shutdown_flag = false;
        queued_operation_flag = false;

        init_environment();
        TRACE("env initialized");

        psm_initialize();
    }

    void create_group(uint16_t group_number, std::vector<uint16_t> members,
                      size_t block_size, send_algorithm algorithm, 
                      completion_callback_t callback,
                      completion_callback_t ss_callback){
        std::mutex m;
        std::condition_variable cv;
        volatile atomic<bool> done{false};

        std::unique_lock<std::mutex> lock(queued_operations_lock);
        queued_operations.push([&]() {
            if(algorithm == BINOMIAL_SEND){
                auto group = 
                    make_unique<binomial_group>(group_number, block_size,
                                                members, callback, ss_callback);
                group->init();
                groups.emplace(group_number, std::move(group));
            }else if(algorithm == CHAIN_SEND){
                auto group = 
                    make_unique<chain_group>(group_number, block_size,
                                             members, callback, ss_callback);
                group->init();
                groups.emplace(group_number, std::move(group));
            }else if(algorithm == SEQUENTIAL_SEND){
                auto group = 
                    make_unique<sequential_group>(group_number, block_size,
                                               members, callback, ss_callback);
                group->init();
                groups.emplace(group_number, std::move(group));
            }else if(algorithm == TREE_SEND){
                auto group = 
                    make_unique<tree_group>(group_number, block_size,
                                            members, callback, ss_callback);
                group->init();
                groups.emplace(group_number, std::move(group));
            }else{
                puts("Unsupported send type?!");
                fflush(stdout);
            }
            done = true;
            cv.notify_one();
        });
        queued_operation_flag = true;
        queued_operations_cv.notify_all();
        lock.unlock();

        std::unique_lock<std::mutex> lk(m);
        cv.wait(lk, [&done]()->bool{return done;});
    }
    void destroy_group(uint16_t group_number){
        std::unique_lock<std::mutex> lock(queued_operations_lock);
        queued_operations.emplace([group_number]() {
            LOG_EVENT(group_number, -1, -1, "destroy_group");
            groups.erase(group_number);
        });
        queued_operation_flag = true;
        queued_operations_cv.notify_all();
    }
    void shutdown(){
        for(uint16_t i = 0; i < num_nodes; i++){
            if(i != node_rank){
                shutdown_msg s;
                tag_t tag = tag_t(tag_t::type::SHUTDOWN);
                psm_mq_send(mq, epaddrs[i], PSM_MQ_FLAG_SENDSYNC,
                            tag.raw(), &s, sizeof(s));
            }
        }
  
        shutdown_flag = true;
        while(1){
            /* do nothing */;
        }
    }
    void send(uint16_t group_number, char* buffer, size_t size){
        std::unique_lock<std::mutex> lock(queued_operations_lock);
        queued_operations.emplace([group_number, buffer, size]() {
            LOG_EVENT(group_number, -1,-1, "preparing_to_send_message");
            auto it = groups.find(group_number);
            assert(it != groups.end());
            it->second->send_message(buffer, size);
            LOG_EVENT(group_number, -1,-1, "started_sending_message");
        });
        queued_operation_flag = true;
        queued_operations_cv.notify_all();
        LOG_EVENT(group_number, -1, -1, "queued_send_message");
    }
    void small_send(uint16_t group_number, char* buffer, uint16_t size){
        std::unique_lock<std::mutex> lock(queued_operations_lock);
        queued_operations.emplace([group_number, buffer, size]() {
            LOG_EVENT(group_number, -1, -1, "small_send_lambda");
            auto it = groups.find(group_number);
            assert(it != groups.end());
            it->second->small_send(buffer, size);
        });
        queued_operation_flag = true;
        queued_operations_cv.notify_all();
        LOG_EVENT(group_number, -1, -1, "queued_small_send");
    }
    void post_receive_buffer(uint16_t group_number, char* buffer, size_t size){
        std::unique_lock<std::mutex> lock(queued_operations_lock);
        queued_operations.emplace([group_number, buffer, size]() {
            auto it = groups.find(group_number);
            assert(it != groups.end());
            it->second->post_buffer(buffer, size);
            LOG_EVENT(group_number, -1, -1, "buffer_posted");
        });
        queued_operation_flag = true;
        queued_operations_cv.notify_all();
        LOG_EVENT(group_number, -1, -1, "queued_post_receive_buffer");
    }
    void barrier(){
        std::mutex m;
        std::condition_variable cv;
        volatile atomic<bool> done{false};

        std::unique_lock<std::mutex> lock(queued_operations_lock);

        queued_operations.emplace([&done, &cv](){
            LOG_EVENT(-1,-1,-1, "start_barrier");
            do_barrier();
            LOG_EVENT(-1,-1,-1, "end_barrier");
            done = true;
            cv.notify_one();
        });
        queued_operation_flag = true;
        queued_operations_cv.notify_all();
        lock.unlock();

        std::unique_lock<std::mutex> lk(m);
        cv.wait(lk, [&done]()->bool{return done;});
        LOG_EVENT(-1,-1,-1, "resume_from_barrier");
    }
}
