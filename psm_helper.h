
#ifndef PSM_HELPER_H
#define PSM_HELPER_H

#include "message.h"

extern "C" {
#include <psm.h>
#include <psm_mq.h>
}

#include <map>
#include <vector>
#include <cstdint>

namespace rdmc{
    extern unsigned int job_number;
    extern unsigned int job_step;
    extern uint16_t node_rank;
    extern uint16_t num_nodes;

//used to initialize the mq
    extern std::map<unsigned int, uint64_t> connections;
//used to send/receive data
    extern psm_mq_t mq;
    extern std::vector<psm_epaddr_t> epaddrs;

    struct request_context {
        void* data;
        tag_t tag;
        bool is_send;
    };
}

#endif /* PSM_HELPER_H */
