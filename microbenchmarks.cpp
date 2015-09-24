
#include "group_send.h"
#include "psm_helper.h"
#include "microbenchmarks.h"

#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <malloc.h>
#include <sys/mman.h>

using namespace std;
using namespace rdmc;

uint32_t ping_pong(){
    const uint64_t tag =  0xbbbbbbbbbbbbbbbb;
    const uint64_t mask = 0xffffffffffffffff;
    const int NUM_BOUNCES = 100000;
    const int WARM_UP = 10000;
//    put_flush("Starting Ping-pong");

//    volatile uint32_t buf = 12345;
//    volatile uint32_t buf2 = 54321;
    char* buffer = (char*)memalign(4096, 4096);//&buf;
    char* buffer2 = (char*)memalign(4096, 4096);//&buf2;
    psm_mq_req_t req;

    memset(buffer, 0, 4096);
    memset(buffer2, 0, 4096);

//    psm_mq_req_t req2;
//    for(int m =0; m<1024;m++)
//    psm_mq_irecv(mq, 1, 1, 0, new char[1024*1024], 1024*1024, NULL, &req2);

    auto wait = [&req,tag](){
//        psm_mq_wait(&req, NULL);
//        psm_mq_status_t status;
//        while(psm_mq_test(&req, &status) != PSM_OK)
//            ;
       while(psm_mq_ipeek(mq, &req, NULL) != PSM_OK)
           ;
       psm_mq_status_t status;
       auto ret = psm_mq_test(&req, &status);
       assert(ret == PSM_OK);
       assert(status.msg_tag == tag);
    };

    uint32_t n = 0;
    if(node_rank % 2 == 0){
        psm_mq_irecv(mq, tag, mask, 0, buffer, 4, NULL, &req);
        wait();

        uint64_t t = get_time();
        for(int i = 0; i < NUM_BOUNCES; i++)
            psm_mq_irecv(mq, tag, mask, 0, buffer2, 4, NULL, &req);

        for(int i = 0; i < NUM_BOUNCES; i++){
            if(i == WARM_UP - 1)
                t = get_time();
                
            psm_mq_send(mq, epaddrs[(node_rank ^ 1)], 
                        PSM_MQ_FLAG_SENDSYNC, tag, buffer, 4);
//            psm_mq_isend(mq, epaddrs[(node_rank ^ 1)], 
//                         PSM_MQ_FLAG_SENDSYNC, 0, buffer, 4, NULL, &req);

//            wait();
            wait();
        }
        printf("ping pong time = %f us\n", 
               1.e-3 * 0.5 * (get_time() - t)/(NUM_BOUNCES - WARM_UP));
        fflush(stdout);
    }else{
        psm_mq_send(mq, epaddrs[(node_rank ^ 1)],
                     PSM_MQ_FLAG_SENDSYNC, tag, buffer, 4);
        
        for(int i = 0; i < NUM_BOUNCES; i++)
            psm_mq_irecv(mq, tag, mask, 0, buffer, 4, NULL, &req);

        for(int i = 0; i < NUM_BOUNCES; i++){
            wait();

            psm_mq_send(mq, epaddrs[(node_rank ^ 1)], 
                         PSM_MQ_FLAG_SENDSYNC, tag, buffer2, 4);
        }
    }
    return n;
}
uint32_t latency_test(){
    const int NUM_MESSAGES = 10;
    const uint64_t tag =  0xaaaaaaaaaaaaaaaa;
    const uint64_t mask = 0xffffffffffffffff;
//    put_flush("Starting Ping-pong");

//    volatile uint32_t buf = 12345;
//    volatile uint32_t buf2 = 54321;
    char* buffer = (char*)memalign(4096, 4096);//&buf;
    char* buffer2 = (char*)memalign(4096, 4096);//&buf2;
    psm_mq_req_t req;

    memset(buffer, 0, 4096);
    memset(buffer2, 0, 4096);

//    psm_mq_req_t req2;
//    for(int m =0; m<1024;m++)
//    psm_mq_irecv(mq, 1, 1, 0, new char[1024*1024], 1024*1024, NULL, &req2);

    auto wait = [&req](){
//        psm_mq_wait(&req, NULL);
//        psm_mq_status_t status;
//        while(psm_mq_test(&req, &status) != PSM_OK)
//            ;
       while(psm_mq_ipeek(mq, &req, NULL) != PSM_OK)
           ;
       psm_mq_status_t status;
       auto ret = psm_mq_test(&req, &status);
       assert(ret == PSM_OK);
       assert(status.msg_tag == tag || status.msg_tag == tag+1 || 
              status.msg_tag == tag+2);
    };

    uint32_t n = 0;
    if(node_rank % 2 == 0){
        psm_mq_irecv(mq, tag+2, mask, 0, buffer, 4, NULL, &req);
        wait();

        uint64_t t = get_time();
        for(int i = 0; i < NUM_MESSAGES; i++){
            // psm_mq_send(mq, epaddrs[(node_rank ^ 1)], 
            //             PSM_MQ_FLAG_SENDSYNC, 0, buffer, 4);
            psm_mq_isend(mq, epaddrs[(node_rank ^ 1)], 
                         PSM_MQ_FLAG_SENDSYNC, tag, buffer, 4, NULL, &req);
        }
        psm_mq_irecv(mq, tag+1, mask, 0, buffer2, 4, NULL, &req);

        psm_mq_status_t status;
        auto ret = psm_mq_wait(&req, &status);
        assert(ret == PSM_OK);
        assert(status.msg_tag == tag+1);
        printf("latency test time = %f us\n", 
               1.e-3 * 0.5 * (get_time() - t)/(NUM_MESSAGES+1));
        fflush(stdout);

        for(int i = 0; i < NUM_MESSAGES; i++){
            wait();
        }
    }else{
        psm_mq_send(mq, epaddrs[(node_rank ^ 1)],
                     PSM_MQ_FLAG_SENDSYNC, tag+2, buffer, 4);

        for(int i = 0; i < NUM_MESSAGES; i++){
            psm_mq_irecv(mq, tag, mask, 0, buffer, 4, NULL, &req);
        }

        for(int i = 0; i < NUM_MESSAGES; i++){
            wait();
        }

        psm_mq_send(mq, epaddrs[(node_rank ^ 1)], 
                    PSM_MQ_FLAG_SENDSYNC, tag+1, buffer2, 4);
    }
    return n;
}
uint32_t blocking_send_latency_test(){
    const int NUM_MESSAGES = 10;
    const uint64_t tag =  0xaaaaaaaaaaaaaaaa;
    const uint64_t mask = 0xffffffffffffffff;

    char* buffer = (char*)memalign(4096, 4096);//&buf;
    char* buffer2 = (char*)memalign(4096, 4096);//&buf2;
    psm_mq_req_t req;

    memset(buffer, 0, 4096);
    memset(buffer2, 0, 4096);

    auto wait = [&req](){
       while(psm_mq_ipeek(mq, &req, NULL) != PSM_OK)
           ;
       psm_mq_status_t status;
       auto ret = psm_mq_test(&req, &status);
       assert(ret == PSM_OK);
       assert(status.msg_tag == tag || status.msg_tag == tag+1 || 
              status.msg_tag == tag+2);
    };

    uint32_t n = 0;
    if(node_rank % 2 == 0){
        psm_mq_irecv(mq, tag+2, mask, 0, buffer, 4, NULL, &req);
        wait();

        uint64_t t = get_time();
        for(int i = 0; i < NUM_MESSAGES; i++){
            psm_mq_send(mq, epaddrs[(node_rank ^ 1)],
                        PSM_MQ_FLAG_SENDSYNC, tag, buffer, 4);
        }
        psm_mq_irecv(mq, tag+1, mask, 0, buffer2, 4, NULL, &req);

        psm_mq_status_t status;
        auto ret = psm_mq_wait(&req, &status);
        assert(ret == PSM_OK);
        assert(status.msg_tag == tag+1);
        printf("blocking send latency = %f us\n", 
               1.e-3 * 0.5 * (get_time() - t)/(NUM_MESSAGES+1));
        fflush(stdout);
    }else{
        psm_mq_send(mq, epaddrs[(node_rank ^ 1)],
                     PSM_MQ_FLAG_SENDSYNC, tag+2, buffer, 4);

        for(int i = 0; i < NUM_MESSAGES; i++){
            psm_mq_irecv(mq, tag, mask, 0, buffer, 4, NULL, &req);
        }

        for(int i = 0; i < NUM_MESSAGES; i++){
            wait();
        }

        psm_mq_send(mq, epaddrs[(node_rank ^ 1)], 
                    PSM_MQ_FLAG_SENDSYNC, tag+1, buffer2, 4);
    }
    return n;
}
void wait(){
    psm_mq_req_t req;
    while(psm_mq_ipeek(mq, &req, NULL) != PSM_OK)
        /* do nothing */;
    
    psm_mq_status_t status;
    assert(psm_mq_test(&req, &status) == PSM_OK);
}
void sbandwidth_test(){
    for(size_t num = 1; num <= 1024; num *= 2){
        vector<double> means;
        vector<double> stddevs;
        vector<size_t> totals;
        for(size_t total = 16 << 20; total >= 16 << 10 && total >= num*4096; total /= 2){
            totals.push_back(total);
            size_t size = total / num;
//#define SBANDWIDTH_USE_MMAP
#ifdef SBANDWIDTH_USE_MMAP
            char* buffer = (char*)mmap(NULL,size*num,PROT_READ|PROT_WRITE, 
                                       MAP_ANON|MAP_PRIVATE, -1, 0);
            char* buffer2 = (char*)mmap(NULL,size*num,PROT_READ|PROT_WRITE, 
                                        MAP_ANON|MAP_PRIVATE, -1, 0);
#else
            char* buffer = (char*)malloc(size*num);
            char* buffer2 = (char*)malloc(size*num);
#endif
            vector<double> rates;
            for(int iteration = 0; iteration < 32; iteration++){
                memset(buffer, 'a', size*num);
                memset(buffer, 'b', size*num);

                memset(buffer2, 'c', size*num);
                memset(buffer2, 'd', size*num);

                psm_mq_req_t req;

                if(node_rank % 2 == 0){
                    // Wait for an initial send, so we know that the other node is ready.
                    psm_mq_irecv(mq, 0, 0, 0, buffer, 4, NULL, &req);
                    psm_mq_wait(&req, NULL);

                    // Time returns the number of nanoseconds since some fixed start time.
                    uint64_t t = get_time();

                    // Start concurrent send and receive.
                    for(unsigned int i = 0; i < num; i++){
                        psm_mq_irecv(mq, i, ~0ull, 0, buffer+size*i, size, NULL, &req);
                        psm_mq_isend(mq, epaddrs[node_rank + 1], PSM_MQ_FLAG_SENDSYNC, i, 
                                     buffer2+size*i, size, NULL, &req);

                    }
                    // Wait for them to complete.
                    for(unsigned int i = 0; i < num*2; i++)
                        wait();

                    // Compute how long these sends took
                    uint64_t diff = get_time() - t;
                    rates.push_back(8.0 * total / diff);
                }else{
                    // Post an initial send, and wait for it to complete before we initiate
                    // the concurrent send and receive.
                    psm_mq_isend(mq, epaddrs[node_rank - 1], PSM_MQ_FLAG_SENDSYNC, 
                                 0, buffer, 4, NULL, &req);
                    psm_mq_wait(&req, NULL);

                    // Post simultaneous sends and receives
                    for(unsigned int i = 0; i < num; i++){
                        psm_mq_irecv(mq, i, ~0ull, 0, buffer+size*i, size, NULL, &req);
                        psm_mq_isend(mq, epaddrs[node_rank - 1], PSM_MQ_FLAG_SENDSYNC, i, 
                                     buffer2+size*i, size, NULL, &req);
                    }
                    // Wait for them to complete before returning.
                    for(unsigned int i = 0; i < num*2; i++)
                        wait();
//        wait();
                }
            }

            if(node_rank == 0){
                means.push_back(compute_mean(rates));
                stddevs.push_back(compute_stddev(rates));
            }

            free(buffer);
            free(buffer2);
        }

        if(node_rank == 0){
            if(num == 1){
                printf("Chunks/Block, ");
                for(auto t : totals){
                    if(t >= 1 << 20)
                        printf("%d MB, , ", (int)(t >> 20));
                    else
                        printf("%d KB, , ", (int)(t >> 10));
                }
                printf("\n");
            }
            assert(means.size() == stddevs.size());
            for(unsigned int i = 0; i < means.size(); i++){
                printf("%f, %f, ", means[i], stddevs[i]);
            }
            printf("\n");
            fflush(stdout);
        }
    }
    printf("\n");
    fflush(stdout);
}
void sbandwidth_test2(){
    const size_t total = 16 << 20;
    for(size_t size = total; size >= 4096; size /= 2){
        char* buffer = (char*)malloc(size);
        char* buffer2 = (char*)malloc(size);

        memset(buffer, 'a', size);
//        memset(buffer, 'b', size);

        memset(buffer2, 'c', size);
//        memset(buffer2, 'd', size);

        psm_mq_req_t req;

        if(node_rank % 2 == 0){
            // Wait for an initial send, so we know that the other node is ready.
            psm_mq_irecv(mq, 0, 0, 0, buffer, 4, NULL, &req);
            psm_mq_wait(&req, NULL);

            // Time returns the number of nanoseconds since some fixed start time.
            uint64_t t = get_time();

            // Start concurrent send and receive.
            psm_mq_irecv(mq, 0, ~0ull, 0, buffer, size, NULL, &req);
            psm_mq_isend(mq, epaddrs[node_rank + 1], PSM_MQ_FLAG_SENDSYNC, 0, 
                         buffer2, size, NULL, &req);

            wait();
            wait();

            // Compute how long these sends took
            uint64_t diff = get_time() - t;

            printf("Bandwidth (size = %d KB): %f Gb/s\n", (int)(size>>10), 8.0 * size / diff);
            fflush(stdout);
        }else{
            // Post an initial send, and wait for it to complete before we initiate
            // the concurrent send and receive.
            psm_mq_isend(mq, epaddrs[node_rank - 1], PSM_MQ_FLAG_SENDSYNC, 
                         0, buffer, 4, NULL, &req);
            psm_mq_wait(&req, NULL);

            // Post simultaneous sends and receives
            psm_mq_irecv(mq, 0, ~0ull, 0, buffer, size, NULL, &req);
            psm_mq_isend(mq, epaddrs[node_rank - 1], PSM_MQ_FLAG_SENDSYNC, 0, 
                         buffer2, size, NULL, &req);
            // Wait for them to complete before returning.
            wait();
            wait();
        }
        free(buffer);
        free(buffer2);
    }
}

void bandwidth_test(){
    const int size = 16 << 20;

    TRACE("Starting bandwidth test");

    // char* buffer = new char[size];
    // char* buffer2 = new char[size];
    char* buffer = //(char*)malloc(size);
        (char*)mmap(NULL,size,PROT_READ|PROT_WRITE, 
                    MAP_ANON|MAP_PRIVATE, -1, 0);

    char* buffer2 = //(char*)malloc(size);
        (char*)mmap(NULL,size,PROT_READ|PROT_WRITE, 
                    MAP_ANON|MAP_PRIVATE, -1, 0);

    memset(buffer, 'a', size);
    memset(buffer, 'b', size);

    memset(buffer2, 'c', size);
    memset(buffer2, 'd', size);


    psm_mq_req_t req;

    auto wait = [&req](){
//        psm_mq_wait(&req, NULL);
//        psm_mq_status_t status;
//        while(psm_mq_test(&req, &status) != PSM_OK)
//            ;
       while(psm_mq_ipeek(mq, &req, NULL) != PSM_OK)
           ;
       psm_mq_status_t status;
       assert(psm_mq_test(&req, &status) == PSM_OK);
    };

    if(node_rank == 0){
        vector<uint64_t> times;

        for(int i = 1; i < num_nodes; i++){
            psm_mq_irecv(mq, i, ~0ull, 0, buffer, 4, NULL, &req);
            psm_mq_wait(&req, NULL);
        
            uint64_t t = get_time();
            psm_mq_irecv(mq, i, ~0ull, 0, buffer, size, NULL, &req);
            psm_mq_isend(mq, epaddrs[i], PSM_MQ_FLAG_SENDSYNC, 0, 
                         buffer2, size, NULL, &req);

            wait();
            wait();

            t = get_time() - t;
            times.push_back(t);
        }

        sort(times.begin(), times.end());
        printf("Min bandwidth: %f Gb/s\n", 8.0 * size / times.back());
        printf("Max bandwidth: %f Gb/s\n", 8.0 * size / times.front());
        printf("Median bandwidth: %f Gb/s\n", 8.0 * size / times[times.size()/2]);
        fflush(stdout);
    }else{
        psm_mq_isend(mq, epaddrs[0], PSM_MQ_FLAG_SENDSYNC, 
                     node_rank, buffer2, 4, NULL, &req);
        psm_mq_wait(&req, NULL);

        psm_mq_irecv(mq, 0, ~0ull, 0, buffer, size, NULL, &req);
        psm_mq_isend(mq, epaddrs[0], PSM_MQ_FLAG_SENDSYNC, node_rank, 
                     buffer2, size, NULL, &req);
        
        wait();
        wait();
    }
//    free(buffer);
//    free(buffer2);
    munmap(buffer, size);
    munmap(buffer2, size);
}
void memcpy_test(){
    const size_t size = 1 << 30;
    char* buf1 = new char[size];
    char* buf2 = new char[size];

    for(size_t i = 0; i < size; i++){
        buf1[i] = (i*i - i) % 256;
    }

    uint64_t t = get_time();

    memcpy(buf2, buf1, size);

    uint64_t diff = get_time() - t;

    printf("Memcpy bandwidth: %f Gb/s\n", 8.0 * size / diff);
    fflush(stdout);
}
void memtest(){
    const auto size = 1ull << 30;  
    char* buffer = new char[size];
    for(auto i = 0ull; i < size; i++){
        buffer[i] = rand();
    }
    
    char sum = 0;

    uint64_t t = get_time();

    for(int i = 0; i < 1024; i++){
        sum += buffer[(327234ull * i + 42354ull) % size];
    }

    uint64_t diff = get_time() - t;

    printf("random read latency: %f ns (sum = %d)\n", diff / 1024.0, (int)sum);
    fflush(stdout);
    delete [] buffer;
}
void memtest2(){
    for(size_t size = 4096; size < (1<<30); size *= 2){

        uint64_t t0 = get_time();
        char* buffer = (char*)mmap(NULL,size,PROT_READ|PROT_WRITE, 
                                   MAP_ANON|MAP_PRIVATE, -1, 0);
        char* buffer2 = (char*)mmap(NULL,size,PROT_READ|PROT_WRITE, 
                                    MAP_ANON|MAP_PRIVATE, -1, 0);
        uint64_t t1 = get_time();
        memset(buffer, 1, size);
        memset(buffer2, 2, size);
        uint64_t t2 = get_time();
        memcpy(buffer, buffer2, size);
        uint64_t t3 = get_time();

        mremap(buffer2, size, size, MREMAP_FIXED | MREMAP_MAYMOVE,
               buffer);

        uint64_t t4 = get_time();

        munmap(buffer, size);


        printf("Buffer size = %d KiB\n", (int)(size >> 10));
        printf("Buffer creation time = %f us\n", 0.001 * (t1 - t0));
        printf("memset time = %f us\n", 0.001 * (t2 - t1) / 2);
        printf("memcpy time = %f us\n", 0.001 * (t3 - t2));
        printf("mremap time = %f us\n", 0.001 * (t4 - t3));
        puts("");
        fflush(stdout);
    }
}
