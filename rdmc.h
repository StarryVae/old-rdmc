
#ifndef MCAST_H
#define MCAST_H

#include <functional>
#include <utility>
#include <vector>

namespace rdmc{
    enum completion_status {SUCCESS, FAILURE};
    enum send_algorithm {BINOMIAL_SEND=1, CHAIN_SEND=2, SEQUENTIAL_SEND=3, TREE_SEND=4};
    
    typedef std::function<void(uint16_t, completion_status, char*)>
        completion_callback_t;

    void initialize();

    void create_group(uint16_t group_number, std::vector<uint16_t> members,
                      size_t block_size, send_algorithm algorithm,
                      completion_callback_t send_callback,
                      completion_callback_t small_send_callback);
    void destroy_group(uint16_t group_number);

    void send(uint16_t group_number, char* buffer, size_t size);
    void small_send(uint16_t group_number, char* buffer, uint16_t size);

    void barrier();
    void shutdown();
    void print_stats();
};

#endif /* MCAST_H */
