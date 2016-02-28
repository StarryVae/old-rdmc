
#ifndef VERBS_HELPER_H
#define VERBS_HELPER_H

#include "message.h"

#include <atomic>
#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <vector>

extern "C" {
#include <infiniband/verbs.h>
}

namespace rdmc {
extern unsigned int job_number;
extern unsigned int job_step;
extern uint32_t node_rank;
extern uint32_t num_nodes;
extern std::vector<std::string> node_addresses;
};

// structure of system resources
// struct resources {
//     ibv_device_attr device_attr;  // Device attributes
//     ibv_port_attr port_attr;      // IB port attributes
//     cm_con_data_t remote_props;   // values to connect to remote side
//     ibv_context *ib_ctx;          // device handle
//     ibv_pd *pd;                   // PD handle
//     ibv_cq *cq;                   // CQ handle
//     ibv_qp *qp;                   // QP handle
//     ibv_mr *mr;                   // MR handle for buf
//     int sock;                     // TCP socket file descriptor
//     char *buf;                    // memory buffer pointer, used for
//                                   // RDMA and send ops
// };

class completion_queue {};

class memory_region {
    std::unique_ptr<ibv_mr, std::function<void(ibv_mr*)>> mr;
    friend class queue_pair;

public:
    memory_region(char* buffer, size_t size);
	uint32_t get_rkey() const;
		
    char* const buffer;
    const size_t size;
};
class remote_memory_region {
public:
    remote_memory_region(uint64_t remote_address, size_t length,
                         uint32_t remote_key)
        : buffer(remote_address), size(length), rkey(remote_key) {}

    const uint64_t buffer;
    const size_t size;
	const uint32_t rkey;
};

class queue_pair {
    std::unique_ptr<ibv_qp, std::function<void(ibv_qp*)>> qp;

public:
	~queue_pair();
    explicit queue_pair(size_t remote_index);
	queue_pair(queue_pair&&) = default;
    bool post_send(const memory_region& mr, size_t offset, size_t length,
                   uint64_t wr_id, uint32_t immediate);
    bool post_recv(const memory_region& mr, size_t offset, size_t length,
                   uint64_t wr_id);

    bool post_empty_send(uint64_t wr_id, uint32_t immediate);
    bool post_empty_recv(uint64_t wr_id);

    bool post_write(const memory_region& mr, size_t offset, size_t length,
                    uint64_t wr_id, remote_memory_region remote_mr,
                    size_t remote_offset, bool signaled = false,
                    bool send_inline = false);
};

// int get_sockets(int rank);

// int prepare_send(struct resources *res, char *msg, int size);
// int prepare_recv(struct resources *res, char *msg, int size);
// int post_send(struct resources *res);
// int post_receive(struct resources *res);

// void resources_init(struct resources *res);
// int resources_create(struct resources *res);
bool verbs_initialize();
void verbs_destroy();
int poll_for_completions(int num, ibv_wc* wcs,
                         std::atomic<bool>& shutdown_flag);

// This function exchanges memory regions with all other connected nodes which
// enables us to do one-sided RDMA operations between them. Due to its nature,
// the function requires that it is called simultaneously on all nodes and that
// only one execution is active at any time.
std::map<uint32_t, remote_memory_region> verbs_exchange_memory_regions(
    const memory_region& mr);

#endif /* VERBS_HELPER_H */
