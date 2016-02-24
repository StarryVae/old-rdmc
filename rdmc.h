
#ifndef RDMC_H
#define RDMC_H

#include "verbs_helper.h"

#include <functional>
#include <utility>
#include <vector>
#include <memory>

namespace rdmc {

enum completion_status { SUCCESS, FAILURE };
enum send_algorithm {
    BINOMIAL_SEND = 1,
    CHAIN_SEND = 2,
    SEQUENTIAL_SEND = 3,
    TREE_SEND = 4
};

struct receive_destination {
    std::shared_ptr<memory_region> mr;
    size_t offset;
};

typedef std::function<receive_destination(size_t)> incoming_message_callback_t;
typedef std::function<void(uint16_t, completion_status, char *, size_t)>
    completion_callback_t;

void initialize();
void shutdown();

void create_group(uint16_t group_number, std::vector<uint32_t> members,
                  size_t block_size, send_algorithm algorithm,
                  incoming_message_callback_t incoming_receive,
                  completion_callback_t send_callback);
void destroy_group(uint16_t group_number);

void send(uint16_t group_number, std::shared_ptr<memory_region> mr,
          size_t offset, size_t length);

void barrier();
};

#endif /* RDMC_H */
