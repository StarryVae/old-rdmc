
#include "util.h"
#include "message.h"
#include "psm_helper.h"
#include "rdmc.h"
#include "microbenchmarks.h"

#include <atomic>
#include <algorithm>
#include <cassert>
#include <condition_variable>
#include <cinttypes>
#include <cstdlib>
#include <cstring>
#include <chrono>
#include <iostream>
#include <memory>
#include <mutex>
#include <string>
#include <sys/mman.h>
#include <sys/resource.h> 
#include <thread>
#include <vector>

using namespace std;
using rdmc::node_rank;
using rdmc::num_nodes;

template <class T>
struct stat{
    T mean;
    T stddev;
};

struct send_stats{
    stat<double> time; // in ms

    stat<double> bandwidth; // in Gb/s

    size_t size;
    size_t block_size;
    size_t chunks_per_block;
    size_t group_size;
    size_t iterations;
};

std::mutex send_mutex;
std::condition_variable send_done_cv;
atomic<uint64_t> send_completion_time;

std::mutex small_send_mutex;
std::condition_variable small_send_done_cv;
atomic<size_t> small_sends_left;


// Map from (group_size, block_size, send_type) to group_number.
map<std::tuple<uint16_t, size_t, rdmc::send_algorithm>, uint16_t> send_groups;
uint16_t next_group_number;

send_stats measure_multicast(size_t size, size_t block_size, 
                             uint16_t group_size, size_t iterations, 
                             rdmc::send_algorithm type = rdmc::BINOMIAL_SEND){

    std::tuple<uint16_t, size_t, rdmc::send_algorithm> 
        send_params(group_size, block_size, type);
    if(send_groups.count(send_params) == 0){
        vector<uint16_t> members;
        for(uint16_t i = 0; i < group_size; i++){
            members.push_back(i);
        }
        LOG_EVENT(-1,-1,-1, "create_group");
        rdmc::create_group(next_group_number, members, block_size, type,
            [&](uint16_t group_number, rdmc::completion_status,char* data){
                LOG_EVENT(-1,-1,-1, "completion_callback");
                unique_lock<mutex> lk(send_mutex);
                send_completion_time = get_time();
                LOG_EVENT(-1,-1,-1, "stop_send_timer");
                send_done_cv.notify_all();
            },[&](uint16_t, rdmc::completion_status, char*){});
        LOG_EVENT(-1,-1,-1, "group_created");
        send_groups.emplace(send_params, next_group_number++);
    }

    uint16_t group_number = send_groups[send_params];
    

    vector<double> rates;
    vector<double> times;

    if(node_rank >= group_size){
        // Each iteration involves three barriers: one at the start and two at
        // the end.
        for(size_t i = 0; i < iterations*3; i++){
            rdmc::barrier();
        }

        return send_stats();
    }
    else if(node_rank > 0){
        auto buffer = (char*)mmap(NULL, size, PROT_READ|PROT_WRITE, 
                                  MAP_ANON|MAP_PRIVATE, -1, 0);
        memset(buffer, 1, size);
        memset(buffer, 0, size);

        for(size_t i = 0; i < iterations; i++){
            send_completion_time = 0;
            rdmc::post_receive_buffer(group_number, buffer, size);
            rdmc::barrier();

            LOG_EVENT(-1,-1,-1, "start_send_timer");
            uint64_t start_time = get_time();

            unique_lock<mutex> lk(send_mutex);
            send_done_cv.wait(lk, [&]{return send_completion_time != 0;});

            // uint64_t t1 = get_time();
            rdmc::barrier();
            uint64_t t2 = get_time();
            rdmc::barrier();
            uint64_t t3 = get_time();

            // Compute time taken by send, but subtract out time spend in
            // barrier and transfering control from callback thread.
            uint64_t dt = t2 - start_time - (t3 - t2) /*- (send_completion_time - t1)*/;

            rates.push_back(8.0 * size / dt);
            times.push_back(1.0e-6 * dt);
        }

        send_stats s;
        s.size = size;
        s.block_size = block_size;
        s.chunks_per_block = 1;
        s.group_size = group_size;
        s.iterations = iterations;

        s.time.mean = compute_mean(times);
        s.time.stddev = compute_stddev(times);
        s.bandwidth.mean = compute_mean(rates);
        s.bandwidth.stddev = compute_stddev(rates);
        return s;
    }
    else{
        for(size_t i = 0; i < iterations; i++){
            // uint16_t cpb = 1;
            // if(block_size <= 128<<10) cpb = 1;
            // else if(block_size == 256<<10) cpb = 4;
            // else if(block_size == 1<<20) cpb = 1;
            // else if(block_size == 2<<20) cpb = 8;
            // else if(block_size == 4<<20) cpb = 8;
            // else if(block_size == 8<<20) cpb = 8;
            // else if(block_size == 16<<20) cpb = 16;

            unique_ptr<char[]> buffer(new char[size]);
            // (char*)mmap(NULL,size,PROT_READ|PROT_WRITE,
            //             MAP_ANON|MAP_PRIVATE, -1, 0);

            for(size_t i = 0; i < size; i++)
                buffer[i] = (rand() >> 5) % 256;

            rdmc::barrier();

            LOG_EVENT(-1,-1,-1, "start_send_timer");
            uint64_t start_time = get_time();
            send_completion_time = 0;
            rdmc::send(group_number, buffer.get(), size);
            
            unique_lock<mutex> lk(send_mutex);
            send_done_cv.wait(lk, [&]{return send_completion_time != 0;});
            LOG_EVENT(-1,-1,-1, "completion_reported");

            // uint64_t t1 = get_time();
            rdmc::barrier();
            uint64_t t2 = get_time();
            rdmc::barrier();
            uint64_t t3 = get_time();

            // Compute time taken by send, but subtract out time spend in
            // barrier and transfering control from callback thread.
            uint64_t dt = t2 - start_time - (t3 - t2) /*- (send_completion_time - t1)*/;
            rates.push_back(8.0 * size / dt);
            times.push_back(1.0e-6 * dt);
        }

        send_stats s;
        s.size = size;
        s.block_size = block_size;
        s.chunks_per_block = 1;
        s.group_size = group_size;
        s.iterations = iterations;
        s.time.mean = compute_mean(times);
        s.time.stddev = compute_stddev(times);
        s.bandwidth.mean = compute_mean(rates);
        s.bandwidth.stddev = compute_stddev(rates);

        return s;
    }
}

send_stats measure_small_multicast(size_t size, uint16_t group_size, size_t iterations,
                                   size_t concurrent_sends){
    std::tuple<uint16_t, size_t, rdmc::send_algorithm> 
        send_params(group_size, 1<<20, rdmc::BINOMIAL_SEND);
    if(send_groups.count(send_params) == 0){
        vector<uint16_t> members;
        for(uint16_t i = 0; i < group_size; i++){
            members.push_back(i);
        }
        LOG_EVENT(-1,-1,-1, "create_group");
        rdmc::create_group(next_group_number, members, 1<<20, rdmc::BINOMIAL_SEND, 
            [&](uint16_t, rdmc::completion_status, char*){},
            [&](uint16_t group_number, rdmc::completion_status, char* data){
                if(--small_sends_left == 0){
                    unique_lock<mutex> lk(small_send_mutex);
                    small_send_done_cv.notify_all();
                }
            });
        LOG_EVENT(-1,-1,-1, "group_created");
        send_groups.emplace(send_params, next_group_number++);
    }

    uint16_t group_number = send_groups[send_params];

    vector<double> rates;
    vector<double> times;

    if(node_rank >= group_size){
        for(size_t i = 0; i < iterations; i++){
            rdmc::barrier();
        }

        rdmc::barrier();
        return send_stats();
    } else if(node_rank > 0){
        for(size_t i = 0; i < iterations; i++){
            small_sends_left = concurrent_sends;
            rdmc::barrier();

            LOG_EVENT(-1,-1,-1, "start_small_send_timer");
            uint64_t start_time = get_time();
            
            unique_lock<mutex> lk(small_send_mutex);
            small_send_done_cv.wait(lk, [&]{return small_sends_left == 0;});

//            rdmc::barrier();
            uint64_t end_time = get_time();
            LOG_EVENT(-1,-1,-1, "stop_small_send_timer");

            rates.push_back(8.0 * size * concurrent_sends
                            / (end_time - start_time));
            times.push_back(1.0e-6 * (end_time - start_time)
                            / concurrent_sends);
        }

        send_stats s;
        s.size = size;
        s.block_size = 1<<20;
        s.chunks_per_block = 1;
        s.group_size = group_size;
        s.iterations = iterations;

        s.time.mean = compute_mean(times);
        s.time.stddev = compute_stddev(times);
        s.bandwidth.mean = compute_mean(rates);
        s.bandwidth.stddev = compute_stddev(rates);

        rdmc::barrier();
        return s;
    }
    else{
        for(size_t i = 0; i < iterations; i++){
            unique_ptr<char[]> buffer(new char[size*concurrent_sends]);

            for(size_t i = 0; i < size*concurrent_sends; i++)
                buffer[i] = (rand() >> 5) % 256;

            small_sends_left = concurrent_sends * group_size;
            rdmc::barrier();

            LOG_EVENT(-1,-1,-1, "start_small_send_timer");
            uint64_t start_time = get_time();
            
            for(size_t i = 0; i < concurrent_sends; i++)
                rdmc::small_send(group_number, buffer.get()+i*size, size);
            
            // unique_lock<mutex> lk(small_send_mutex);
            // small_send_done_cv.wait(lk, [&]{return small_sends_left == 0;});

//            rdmc::barrier();
            uint64_t end_time = get_time();
            LOG_EVENT(-1,-1,-1, "stop_small_send_timer");

            LOG_EVENT(-1,-1,-1, "completion_reported");

            rates.push_back(8.0 * size * concurrent_sends
                            / (end_time - start_time));
            times.push_back(1.0e-6 * (end_time - start_time)
                            / concurrent_sends);
        }

        send_stats s;
        s.size = size;
        s.block_size = 1<<20;
        s.chunks_per_block = 1;
        s.group_size = group_size;
        s.iterations = iterations;
        s.time.mean = compute_mean(times);
        s.time.stddev = compute_stddev(times);
        s.bandwidth.mean = compute_mean(rates);
        s.bandwidth.stddev = compute_stddev(rates);

        rdmc::barrier();
        return s;
    }
}

void blocksize_v_bandwidth(uint16_t gsize){
    const size_t min_block_size = 16ull<<10;
    const size_t max_block_size = 16ull<<20;

    puts("=========================================================");
    puts("=             Block Size vs. Bandwdith (Gb/s)           =");
    puts("=========================================================");
    printf("Group Size = %d\n", (int)gsize);
    printf("Send Size, ");
    for(auto block_size = min_block_size; block_size <= max_block_size; block_size *= 2){
        if(block_size >= 1<<20)  printf("%d MB, ", (int)(block_size>>20));
        else                     printf("%d KB, ", (int)(block_size>>10));
    }
    for(auto block_size = min_block_size; block_size <= max_block_size; block_size *= 2){
        if(block_size >= 1<<20)  printf("%d MB stddev, ", (int)(block_size>>20));
        else                     printf("%d KB stddev, ", (int)(block_size>>10));
    }
    puts("");
    fflush(stdout);
    for(auto size : {256ull << 20, 64ull << 20, 16ull << 20, 4ull << 20, 1ull << 20, 256ull << 10, 64ull << 10, 16ull << 10}){
        if(size >= 1<<20)       printf("%d MB, ", (int)(size>>20));
        else if(size >= 1<<10)  printf("%d KB, ", (int)(size>>10));
        else                    printf("%d B, ", (int)(size));

        vector<double> stddevs;
        for(auto block_size = min_block_size; block_size <= max_block_size; block_size *= 2){
            if(block_size > size){
                printf(", ");
                continue;
            }
            auto s = measure_multicast(size, block_size, gsize, 8);
            printf("%f, ", s.bandwidth.mean);
            fflush(stdout);

            stddevs.push_back(s.bandwidth.stddev);
        }
        for(auto s : stddevs){
            printf("%f, ", s);
        }
        puts("");
        fflush(stdout);
    }
    puts("");
    fflush(stdout);
}
void compare_send_types(){
    puts("=========================================================");
    puts("=         Compare Send Types - Bandwidth (Gb/s)         =");
    puts("=========================================================");
    puts("Group Size,Binomial Pipeline (256 MB),Chain Send (256 MB),Sequential Send (256 MB),Tree Send (256),"
         "Binomial Pipeline (64 MB),Chain Send (64 MB),Sequential Send (64 MB),Tree Send (64 MB)"
         "BP256 stddev,CS256 stddev,SS256 stddev,BP64 stddev,CS64 stddev,SS64 stddev");
    fflush(stdout);

    const size_t block_size = 1 << 20;

    for(int gsize = num_nodes; gsize >= 2; gsize /= 2){
        auto bp64 = measure_multicast(64<<20, block_size, gsize, 8, rdmc::BINOMIAL_SEND);
        auto bp256 = measure_multicast(256<<20, block_size, gsize, 8, rdmc::BINOMIAL_SEND);
        auto cs64 = measure_multicast(64<<20, block_size, gsize, 8, rdmc::CHAIN_SEND);
        auto cs256 = measure_multicast(256<<20, block_size, gsize, 8, rdmc::CHAIN_SEND);
        auto ss64 = measure_multicast(64<<20, block_size, gsize, 8, rdmc::SEQUENTIAL_SEND);
        auto ss256 = measure_multicast(256<<20, block_size, gsize, 8, rdmc::SEQUENTIAL_SEND);
        auto ts64 = measure_multicast(64<<20, block_size, gsize, 8, rdmc::TREE_SEND);
        auto ts256 = measure_multicast(256<<20, block_size, gsize, 8, rdmc::TREE_SEND);
        printf("%d, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f\n", gsize,
               bp256.bandwidth.mean, cs256.bandwidth.mean, ss256.bandwidth.mean, ts256.bandwidth.mean,
               bp64.bandwidth.mean, cs64.bandwidth.mean, ss64.bandwidth.mean, ts64.bandwidth.mean,
               bp256.bandwidth.stddev, cs256.bandwidth.stddev, ss256.bandwidth.stddev, ts256.bandwidth.stddev, 
               bp64.bandwidth.stddev, cs64.bandwidth.stddev, ss64.bandwidth.stddev, ts64.bandwidth.stddev);
        fflush(stdout);
    }
    puts("");
    fflush(stdout);    
}
void bandwidth_group_size(){
    puts("=========================================================");
    puts("=              Bandwidth vs. Group Size                 =");
    puts("=========================================================");
    puts("Group Size, 256 MB, 64 MB, 16 MB, 4 MB, "
         "256stddev, 64stddev, 16stddev, 4stddev");
    fflush(stdout);

    for(int gsize = num_nodes; gsize >= 2; gsize /= 2){
        auto bp256 = measure_multicast(256<<20, 1<<20, gsize, 8, rdmc::BINOMIAL_SEND);
        auto bp64 = measure_multicast(64<<20, 1<20, gsize, 8, rdmc::BINOMIAL_SEND);
        auto bp16 = measure_multicast(16<<20, 1<<20, gsize, 16, rdmc::BINOMIAL_SEND);
        auto bp4 = measure_multicast(4<<20, 512<<10, gsize, 16, rdmc::BINOMIAL_SEND);
        printf("%d, %f, %f, %f, %f, %f, %f, %f, %f\n", gsize,
               bp256.bandwidth.mean, bp64.bandwidth.mean, 
               bp16.bandwidth.mean, bp4.bandwidth.mean, 
               bp256.bandwidth.stddev, bp64.bandwidth.stddev, 
               bp16.bandwidth.stddev, bp4.bandwidth.stddev);
        fflush(stdout);
    }
    puts("");
    fflush(stdout);    
}
void latency_group_size(){
    puts("=========================================================");
    puts("=               Latency vs. Group Size                  =");
    puts("=========================================================");
    puts("Group Size,64 KB,16 KB,4 KB,1 KB,256 B,"
         "64stddev,16stddev,4stddev,1stddev,256stddev");
    fflush(stdout);

    for(int gsize = num_nodes; gsize >= 2; gsize /= 2){
        auto bp64 = measure_multicast(64<<10, 32<<10, gsize, 16, rdmc::BINOMIAL_SEND);
        auto bp16 = measure_multicast(16<<10, 8<<10, gsize, 16, rdmc::BINOMIAL_SEND);
        auto bp4 = measure_multicast(4<<10, 4<<10, gsize, 16, rdmc::BINOMIAL_SEND);
        auto bp1 = measure_multicast(1<<10, 1<<10, gsize, 16, rdmc::BINOMIAL_SEND);
        auto bp256 = measure_multicast(256, 256, gsize, 16, rdmc::BINOMIAL_SEND);
        printf("%d, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f\n", gsize,
               bp64.time.mean, bp16.time.mean, 
               bp4.time.mean, bp1.time.mean, bp256.time.mean,
               bp64.time.stddev, bp16.time.stddev, 
               bp4.time.stddev, bp1.time.stddev, bp256.time.stddev);
        fflush(stdout);
    }
    puts("");
    fflush(stdout);    

}
void small_send_latency_group_size(){
    puts("=========================================================");
    puts("=               Latency vs. Group Size                  =");
    puts("=========================================================");
    puts("Group Size, 16 KB, 4 KB, 1 KB, 256 Bytes, "
         "64stddev, 16stddev, 4stddev, 1stddev");
    fflush(stdout);

    for(int gsize = num_nodes; gsize >= 2; gsize /= 2){
        auto bp16 = measure_small_multicast(16<<10, gsize, 16, 512);
        auto bp4 = measure_small_multicast(4<<10, gsize, 16, 512);
        auto bp1 = measure_small_multicast(1<<10, gsize, 16, 512);
        auto bp256 = measure_small_multicast(256, gsize, 16, 512);
        printf("%d, %f, %f, %f, %f, %f, %f, %f, %f\n", gsize,
               bp16.time.mean, bp4.time.mean, 
               bp1.time.mean, bp256.time.mean, 
               bp16.time.stddev, bp4.time.stddev, 
               bp1.time.stddev, bp256.time.stddev);
        fflush(stdout);
    }
    puts("");
    fflush(stdout);    
}
void large_send(){
    LOG_EVENT(-1, -1, -1, "start_large_send");
    auto s = measure_multicast(32<<20, 8<<20, num_nodes, 1);
    printf("Bandwidth = %f(%f) Gb/s\n", s.bandwidth.mean, s.bandwidth.stddev);
    printf("Latency = %f(%f) ms\n", s.time.mean, s.time.stddev);
    // uint64_t eTime = get_time();
    // double diff = 1.e-6 * (eTime - sTime);
    // printf("Percent time sending: %f%%", 100.0 * s.time.mean * 16 / diff);
    fflush(stdout);
}
void small_send(){
    auto s = measure_small_multicast(1024, num_nodes, 4, 128);
    printf("Latency = %.2f(%.2f) us\n", s.time.mean*1000.0, s.time.stddev * 1000.0);
    fflush(stdout);
}
int main(int argc, char* argv[]){
    if(argc <= 1){
        puts("Must specify which experiment to run");
        return -1;
    }
    // rlimit rlim;
    // rlim.rlim_cur = RLIM_INFINITY;
    // rlim.rlim_max = RLIM_INFINITY;
    // setrlimit(RLIMIT_CORE, &rlim);

    LOG_EVENT(-1,-1,-1, "calling_init");
    rdmc::initialize();
    LOG_EVENT(-1,-1,-1, "init_done");
    TRACE("Finished initializing.");

    printf("Experiment Name: %s\n", argv[1]);
    if(strcmp(argv[1], "blocksize4") == 0){
        blocksize_v_bandwidth(4);
    } else if(strcmp(argv[1], "blocksize16") == 0){
        blocksize_v_bandwidth(16);
    } else if(strcmp(argv[1], "sendtypes") == 0){
        compare_send_types();
    } else if(strcmp(argv[1], "bandwidth") == 0){
        bandwidth_group_size();        
    } else if(strcmp(argv[1], "overhead") == 0){
        latency_group_size();
    } else if(strcmp(argv[1], "smallsend") == 0){
        small_send_latency_group_size();
    } else if(strcmp(argv[1], "concurrent") == 0){
        // TODO: Group Size vs. Bandwidth (concurrent)
    } else if(strcmp(argv[1], "custom") == 0){
        large_send();
        rdmc::print_stats();
        flush_events();
    } else {
        puts("Unrecognized experiment name.");
        fflush(stdout);
    }

    // flush_events();

    if(node_rank == 0){
        TRACE("About to trigger shutdown");
        rdmc::shutdown();
    }

    this_thread::sleep_for(chrono::minutes(30));
}
