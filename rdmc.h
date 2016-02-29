
#ifndef RDMC_H
#define RDMC_H

#include "verbs_helper.h"

#include <boost/optional.hpp>
#include <functional>
#include <memory>
#include <utility>
#include <string>
#include <vector>

namespace rdmc {

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

typedef std::function<receive_destination(size_t size)>
	incoming_message_callback_t;
typedef std::function<void(char* buffer, size_t size)>
    completion_callback_t;
typedef std::function<void(boost::optional<uint32_t> suspected_victim)>
    failure_callback_t;

void initialize(const std::vector<std::string>& addresses, uint32_t node_rank);
void shutdown() __attribute__ ((noreturn));

void create_group(uint16_t group_number, std::vector<uint32_t> members,
                  size_t block_size, send_algorithm algorithm,
                  incoming_message_callback_t incoming_receive,
                  completion_callback_t send_callback,
                  failure_callback_t failure_callback);
void destroy_group(uint16_t group_number);

void send(uint16_t group_number, std::shared_ptr<memory_region> mr,
          size_t offset, size_t length);

void barrier();
};

#endif /* RDMC_H */
