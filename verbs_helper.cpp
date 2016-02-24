
#include "connection.h"
#include "util.h"
#include "verbs_helper.h"

#include <arpa/inet.h>
#include <byteswap.h>
#include <cassert>
#include <cstring>
#include <endian.h>
#include <getopt.h>
#include <iostream>
#include <netdb.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <unistd.h>
#include <vector>

using namespace std;
using namespace rdmc;

struct config_t {
    const char *dev_name;        // IB device name
    string ip_addr;              // server host name
    u_int32_t tcp_port = 19875;  // server TCP port
    int ib_port = 1;             // local IB port to work with
    int gid_idx = -1;            // gid index to use
};

// structure to exchange data which is needed to connect the QPs
struct cm_con_data_t {
    uint32_t qp_num;  // QP number
    uint16_t lid;     // LID of the IB port
    uint8_t gid[16];  // gid
} __attribute__((packed));

// config for tcp connections
static vector<config_t> config_vec;
// sockets for each connection
static map<uint32_t, tcp::socket> sockets;

// structure of system resources
struct ibv_resources {
    ibv_device_attr device_attr;  // Device attributes
    ibv_port_attr port_attr;      // IB port attributes
    ibv_context *ib_ctx;          // device handle
    ibv_pd *pd;                   // PD handle
    ibv_cq *cq;                   // CQ handle
} verbs_resources;

// int get_sockets(int rank) { return sockets[rank]; }

// static int exchange_node_rank(int sock) {
//     char msg[10];
//     memset(msg, 0, 10);
//     sprintf(msg, "%d", node_rank);
//     write(sock, msg, sizeof(msg));
//     read(sock, msg, sizeof(msg));
//     int rank;
//     sscanf(msg, "%d", &rank);
//     return rank;
// }

// int tcp_listen(int port) {
//     int listenfd;
//     struct sockaddr_in serv_addr;

//     listenfd = socket(AF_INET, SOCK_STREAM, 0);
//     if(listenfd < 0) fprintf(stderr, "ERROR opening socket\n");

//     int reuse_addr = 1;
//     setsockopt(listenfd, SOL_SOCKET, SO_REUSEADDR, (char *)&reuse_addr,
//                sizeof(reuse_addr));

//     bzero((char *)&serv_addr, sizeof(serv_addr));
//     serv_addr.sin_family = AF_INET;
//     serv_addr.sin_addr.s_addr = INADDR_ANY;
//     serv_addr.sin_port = htons(port);
//     if(bind(listenfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0)
//         fprintf(stderr, "ERROR on binding\n");
//     listen(listenfd, 5);
//     return listenfd;
// }

// pair<int, int> tcp_accept(int listenfd) {
//     int sock = accept(listenfd, NULL, 0);
//     int rank = exchange_node_rank(sock);
//     return pair<int, int>(sock, rank);
// }

// pair<int, int> tcp_connect(const char *servername, int port) {
//     int sock;
//     struct sockaddr_in serv_addr;
//     struct hostent *server;

//     sock = socket(AF_INET, SOCK_STREAM, 0);
//     if(sock < 0) {
//         fprintf(stderr, "ERROR opening socket\n");
//     }
//     server = gethostbyname(servername);
//     if(server == NULL) {
//         fprintf(stderr, "ERROR, no such host\n");
//         exit(0);
//     }
//     bzero((char *)&serv_addr, sizeof(serv_addr));
//     serv_addr.sin_family = AF_INET;
//     bcopy((char *)server->h_addr, (char *)&serv_addr.sin_addr.s_addr,
//           server->h_length);
//     serv_addr.sin_port = htons(port);

//     while(connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) <
//     0) {
//     }

//     int rank = exchange_node_rank(sock);
//     return pair<int, int>(sock, rank);
// }

// int sock_sync_data(int sock, int xfer_size, char *local_data,
//                    char *remote_data) {
//     int rc;
//     int read_bytes = 0;
//     int total_read_bytes = 0;
//     rc = write(sock, local_data, xfer_size);
//     if(rc < xfer_size)
//         fprintf(stderr, "Failed writing data during sock_sync_data\n");
//     else
//         rc = 0;
//     while(!rc && total_read_bytes < xfer_size) {
//         read_bytes = read(sock, remote_data, xfer_size);
//         if(read_bytes > 0)
//             total_read_bytes += read_bytes;
//         else
//             rc = read_bytes;
//     }
//     return rc;
// }

// void resources_init(struct resources *res) {
//     memset(res, 0, sizeof *res);
//     res->sock = -1;
// }

// before calling resources_create, its socket field must be set
// int resources_create(struct resources *res) {
//     struct config_t config = config_vec[node_rank];

//     struct ibv_device **dev_list = NULL;
//     struct ibv_qp_init_attr qp_init_attr;
//     struct ibv_device *ib_dev = NULL;
//     int i;
//     int cq_size = 0;
//     int num_devices;
//     int rc = 0;

//     fprintf(stdout, "searching for IB devices in host\n");
//     /* get device names in the system */
//     dev_list = ibv_get_device_list(&num_devices);
//     if(!dev_list) {
//         fprintf(stderr, "failed to get IB devices list\n");
//         rc = 1;
//         goto resources_create_exit;
//     }
//     /* if there isn't any IB device in host */
//     if(!num_devices) {
//         fprintf(stderr, "found %d device(s)\n", num_devices);
//         rc = 1;
//         goto resources_create_exit;
//     }
//     fprintf(stdout, "found %d device(s)\n", num_devices);
//     /* search for the specific device we want to work with */
//     for(i = 0; i < num_devices; i++) {
//         if(!config.dev_name) {
//             config.dev_name = strdup(ibv_get_device_name(dev_list[i]));
//             fprintf(stdout, "device not specified, using first one found:
//             %s\n",
//                     config.dev_name);
//         }
//         if(!strcmp(ibv_get_device_name(dev_list[i]), config.dev_name)) {
//             ib_dev = dev_list[i];
//             break;
//         }
//     }
//     /* if the device wasn't found in host */
//     if(!ib_dev) {
//         fprintf(stderr, "IB device %s wasn't found\n", config.dev_name);
//         rc = 1;
//         goto resources_create_exit;
//     }
//     /* get device handle */
//     res->ib_ctx = ibv_open_device(ib_dev);
//     if(!res->ib_ctx) {
//         fprintf(stderr, "failed to open device %s\n", config.dev_name);
//         rc = 1;
//         goto resources_create_exit;
//     }
//     /* We are now done with device list, free it */
//     ibv_free_device_list(dev_list);
//     dev_list = NULL;
//     ib_dev = NULL;
//     /* query port properties  */
//     if(ibv_query_port(res->ib_ctx, config.ib_port, &res->port_attr)) {
//         fprintf(stderr, "ibv_query_port on port %u failed\n",
//         config.ib_port);
//         rc = 1;
//         goto resources_create_exit;
//     }
//     /* allocate Protection Domain */
//     res->pd = ibv_alloc_pd(res->ib_ctx);
//     if(!res->pd) {
//         fprintf(stderr, "ibv_alloc_pd failed\n");
//         rc = 1;
//         goto resources_create_exit;
//     }
//     /* each side will send only one WR, so Completion Queue with 1 entry is
//      * enough
//    */
//     cq_size = 1;
//     res->cq = ibv_create_cq(res->ib_ctx, cq_size, NULL, NULL, 0);
//     if(!res->cq) {
//         fprintf(stderr, "failed to create CQ with %u entries\n", cq_size);
//         rc = 1;
//         goto resources_create_exit;
//     }

//     /* create the Queue Pair */
//     memset(&qp_init_attr, 0, sizeof(qp_init_attr));
//     qp_init_attr.qp_type = IBV_QPT_RC;
//     qp_init_attr.sq_sig_all = 1;
//     qp_init_attr.send_cq = res->cq;
//     qp_init_attr.recv_cq = res->cq;
//     qp_init_attr.cap.max_send_wr = 1;
//     qp_init_attr.cap.max_recv_wr = 1;
//     qp_init_attr.cap.max_send_sge = 1;
//     qp_init_attr.cap.max_recv_sge = 1;
//     res->qp = ibv_create_qp(res->pd, &qp_init_attr);
//     if(!res->qp) {
//         fprintf(stderr, "failed to create QP\n");
//         rc = 1;
//         goto resources_create_exit;
//     }
//     fprintf(stdout, "QP was created, QP number=0x%x\n", res->qp->qp_num);
// resources_create_exit:
//     if(rc) {
//         /* Error encountered, cleanup */
//         if(res->qp) {
//             ibv_destroy_qp(res->qp);
//             res->qp = NULL;
//         }
//         if(res->mr) {
//             ibv_dereg_mr(res->mr);
//             res->mr = NULL;
//         }
//         if(res->buf) {
//             free(res->buf);
//             res->buf = NULL;
//         }
//         if(res->cq) {
//             ibv_destroy_cq(res->cq);
//             res->cq = NULL;
//         }
//         if(res->pd) {
//             ibv_dealloc_pd(res->pd);
//             res->pd = NULL;
//         }
//         if(res->ib_ctx) {
//             ibv_close_device(res->ib_ctx);
//             res->ib_ctx = NULL;
//         }
//         if(dev_list) {
//             ibv_free_device_list(dev_list);
//             dev_list = NULL;
//         }
//         if(res->sock >= 0) {
//             if(close(res->sock)) fprintf(stderr, "failed to close socket\n");
//             res->sock = -1;
//         }
//     }
//     return rc;
// }

// int prepare_recv(struct resources *res, char *msg, int size) {
//     int rc = 0;
//     int mr_flags = 0;
//     res->buf = msg;
//     memset(res->buf, 0, size);

//     /* register the memory buffer */
//     mr_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ |
//                IBV_ACCESS_REMOTE_WRITE;
//     res->mr = ibv_reg_mr(res->pd, res->buf, size, mr_flags);
//     if(!res->mr) {
//         fprintf(stderr, "ibv_reg_mr failed with mr_flags=0x%x\n", mr_flags);
//         rc = 1;
//         return rc;
//     }
//     fprintf(
//         stdout,
//         "MR was registered with addr=%p, lkey=0x%x, rkey=0x%x, flags=0x%x\n",
//         res->buf, res->mr->lkey, res->mr->rkey, mr_flags);

//     return rc;
// }

// int prepare_send(struct resources *res, char *msg, int size) {
//     int rc = 0;
//     int mr_flags = 0;
//     /* allocate the memory buffer that will hold the data */
//     res->buf = msg;
//     if(!res->buf) {
//         fprintf(stderr, "empty message buffer\n");
//         rc = 1;
//         return rc;
//     }

//     /* register the memory buffer */
//     mr_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ |
//                IBV_ACCESS_REMOTE_WRITE;
//     res->mr = ibv_reg_mr(res->pd, res->buf, size, mr_flags);
//     if(!res->mr) {
//         fprintf(stderr, "ibv_reg_mr failed with mr_flags=0x%x\n", mr_flags);
//         rc = 1;
//         return rc;
//     }
//     fprintf(
//         stdout,
//         "MR was registered with addr=%p, lkey=0x%x, rkey=0x%x, flags=0x%x\n",
//         res->buf, res->mr->lkey, res->mr->rkey, mr_flags);

//     return rc;
// }

static int modify_qp_to_init(struct ibv_qp *qp) {
    struct config_t config = config_vec[node_rank];

    struct ibv_qp_attr attr;
    int flags;
    int rc;
    memset(&attr, 0, sizeof(attr));
    attr.qp_state = IBV_QPS_INIT;
    attr.port_num = config.ib_port;
    attr.pkey_index = 0;
    attr.qp_access_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ |
                           IBV_ACCESS_REMOTE_WRITE;
    flags =
        IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS;
    rc = ibv_modify_qp(qp, &attr, flags);
    if(rc) fprintf(stderr, "failed to modify QP state to INIT\n");
    return rc;
}

static int modify_qp_to_rtr(struct ibv_qp *qp, uint32_t remote_qpn,
                            uint16_t dlid, uint8_t *dgid) {
    struct config_t config = config_vec[node_rank];

    struct ibv_qp_attr attr;
    int flags;
    int rc;
    memset(&attr, 0, sizeof(attr));
    attr.qp_state = IBV_QPS_RTR;
    attr.path_mtu = verbs_resources.port_attr.max_mtu;
    attr.dest_qp_num = remote_qpn;
    attr.rq_psn = 0;
    attr.max_dest_rd_atomic = 1;
    attr.min_rnr_timer = 0x12;
    attr.ah_attr.is_global = 0;
    attr.ah_attr.dlid = dlid;
    attr.ah_attr.sl = 0;
    attr.ah_attr.src_path_bits = 0;
    attr.ah_attr.port_num = config.ib_port;
    if(config.gid_idx >= 0) {
        attr.ah_attr.is_global = 1;
        attr.ah_attr.port_num = 1;
        memcpy(&attr.ah_attr.grh.dgid, dgid, 16);
        attr.ah_attr.grh.flow_label = 0;
        attr.ah_attr.grh.hop_limit = 1;
        attr.ah_attr.grh.sgid_index = config.gid_idx;
        attr.ah_attr.grh.traffic_class = 0;
    }
    flags = IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU | IBV_QP_DEST_QPN |
            IBV_QP_RQ_PSN | IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER;
    rc = ibv_modify_qp(qp, &attr, flags);
    if(rc) fprintf(stderr, "failed to modify QP state to RTR\n");
    return rc;
}

static int modify_qp_to_rts(struct ibv_qp *qp) {
    struct ibv_qp_attr attr;
    int flags;
    int rc;
    memset(&attr, 0, sizeof(attr));
    attr.qp_state = IBV_QPS_RTS;
    attr.timeout = 4;
    attr.retry_cnt = 6;
    attr.rnr_retry = 7;
    attr.sq_psn = 0;
    attr.max_rd_atomic = 1;
    flags = IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT |
            IBV_QP_RNR_RETRY | IBV_QP_SQ_PSN | IBV_QP_MAX_QP_RD_ATOMIC;
    rc = ibv_modify_qp(qp, &attr, flags);
    if(rc) fprintf(stderr, "failed to modify QP state to RTS. ERRNO=%d\n", rc);
    return rc;
}

void verbs_destroy() {
    if(verbs_resources.cq && ibv_destroy_cq(verbs_resources.cq)) {
        fprintf(stderr, "failed to destroy CQ\n");
    }
    if(verbs_resources.pd && ibv_dealloc_pd(verbs_resources.pd)) {
        fprintf(stderr, "failed to deallocate PD\n");
    }
    if(verbs_resources.ib_ctx && ibv_close_device(verbs_resources.ib_ctx)) {
        fprintf(stderr, "failed to close device context\n");
    }
}

bool verbs_initialize() {
    memset(&verbs_resources, 0, sizeof(verbs_resources));

    config_vec.resize(num_nodes);
    for(uint32_t i = 0; i < num_nodes; ++i) {
        config_vec[i].ip_addr = node_addresses[i];
    }

    TRACE("Starting connection phase");
    // try to connect to nodes greater than node_rank
    for(uint32_t i = num_nodes - 1; i > node_rank; --i) {
        cout << "trying to connect to node rank " << i << endl;
        sockets[i] = std::move(
            tcp::socket(config_vec[i].ip_addr, config_vec[i].tcp_port));

        uint32_t remote_rank = sockets[i].exchange(node_rank);
        printf("remote_rank = %d\n", (int)remote_rank);
        fflush(stdout);
        assert(remote_rank == i);
        TRACE("Connected to node");
    }

    TRACE("starting listen phase");

    // set up listen on the port
    tcp::connection_listener listener(config_vec[node_rank].tcp_port);

    // now accept connections
    // make sure that the caller is correctly identified with its id!
    for(int i = node_rank - 1; i >= 0; --i) {
        cout << "waiting for nodes with lesser rank" << endl;

        tcp::socket s = listener.accept();
        uint32_t remote_rank = s.exchange(node_rank);
        sockets[remote_rank] = std::move(s);

        cout << "connected to node rank " << remote_rank << endl << endl;
    }

    TRACE("Done connecting");

    auto res = &verbs_resources;
    config_t config = config_vec[node_rank];

    ibv_device **dev_list = NULL;
    ibv_device *ib_dev = NULL;
    int i;
    int cq_size = 0;
    int num_devices = 0;
    int rc = 0;

    fprintf(stdout, "searching for IB devices in host\n");
    /* get device names in the system */
    dev_list = ibv_get_device_list(&num_devices);
    if(!dev_list) {
        fprintf(stderr, "failed to get IB devices list\n");
        rc = 1;
        goto resources_create_exit;
    }
    /* if there isn't any IB device in host */
    if(!num_devices) {
        fprintf(stderr, "found %d device(s)\n", num_devices);
        rc = 1;
        goto resources_create_exit;
    }

    fprintf(stdout, "found %d device(s)\n", num_devices);
    /* search for the specific device we want to work with */
    for(i = 0; i < num_devices; i++) {
        if(!config.dev_name) {
            config.dev_name = strdup(ibv_get_device_name(dev_list[i]));
            fprintf(stdout, "device not specified, using first one found: %s\n",
                    config.dev_name);
        }
        if(!strcmp(ibv_get_device_name(dev_list[i]), config.dev_name)) {
            ib_dev = dev_list[i];
            break;
        }
    }
    /* if the device wasn't found in host */
    if(!ib_dev) {
        fprintf(stderr, "IB device %s wasn't found\n", config.dev_name);
        rc = 1;
        goto resources_create_exit;
    }
    /* get device handle */
    res->ib_ctx = ibv_open_device(ib_dev);
    if(!res->ib_ctx) {
        fprintf(stderr, "failed to open device %s\n", config.dev_name);
        rc = 1;
        goto resources_create_exit;
    }
    /* We are now done with device list, free it */
    ibv_free_device_list(dev_list);
    dev_list = NULL;
    ib_dev = NULL;
    /* query port properties  */
    if(ibv_query_port(res->ib_ctx, config.ib_port, &res->port_attr)) {
        fprintf(stderr, "ibv_query_port on port %u failed\n", config.ib_port);
        rc = 1;
        goto resources_create_exit;
    }
    /* allocate Protection Domain */
    res->pd = ibv_alloc_pd(res->ib_ctx);
    if(!res->pd) {
        fprintf(stderr, "ibv_alloc_pd failed\n");
        rc = 1;
        goto resources_create_exit;
    }
    /* each side will send only one WR, so Completion Queue with 1 entry is
     * enough
     */
    cq_size = 16;
    res->cq = ibv_create_cq(res->ib_ctx, cq_size, NULL, NULL, 0);
    if(!res->cq) {
        fprintf(stderr, "failed to create CQ with %u entries\n", cq_size);
        rc = 1;
        goto resources_create_exit;
    }

    /* create the Queue Pair */
    // memset(&qp_init_attr, 0, sizeof(qp_init_attr));
    // qp_init_attr.qp_type = IBV_QPT_RC;
    // qp_init_attr.sq_sig_all = 1;
    // qp_init_attr.send_cq = res->cq;
    // qp_init_attr.recv_cq = res->cq;
    // qp_init_attr.cap.max_send_wr = 1;
    // qp_init_attr.cap.max_recv_wr = 1;
    // qp_init_attr.cap.max_send_sge = 1;
    // qp_init_attr.cap.max_recv_sge = 1;
    // res->qp = ibv_create_qp(res->pd, &qp_init_attr);
    // if(!res->qp) {
    //     fprintf(stderr, "failed to create QP\n");
    //     rc = 1;
    //     goto resources_create_exit;
    // }
    // fprintf(stdout, "QP was created, QP number=0x%x\n", res->qp->qp_num);
    TRACE("verbs_initialize() - SUCCESS");
    return true;
resources_create_exit:
    TRACE("verbs_initialize() - ERROR!!!!!!!!!!!!!!");
    if(rc) {
        /* Error encountered, cleanup */
        // if(res->qp) {
        //     ibv_destroy_qp(res->qp);
        //     res->qp = NULL;
        // }
        // if(res->mr) {
        //     ibv_dereg_mr(res->mr);
        //     res->mr = NULL;
        // }
        // if(res->buf) {
        //     free(res->buf);
        //     res->buf = NULL;
        // }
        if(res->cq) {
            ibv_destroy_cq(res->cq);
            res->cq = NULL;
        }
        if(res->pd) {
            ibv_dealloc_pd(res->pd);
            res->pd = NULL;
        }
        if(res->ib_ctx) {
            ibv_close_device(res->ib_ctx);
            res->ib_ctx = NULL;
        }
        if(dev_list) {
            ibv_free_device_list(dev_list);
            dev_list = NULL;
        }
        // if(res->sock >= 0) {
        //     if(close(res->sock)) fprintf(stderr, "failed to close socket\n");
        //     res->sock = -1;
        // }
    }
    return false;
}
memory_region::memory_region(char *buf, size_t s) : buffer(buf), size(s) {
    int mr_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ |
                   IBV_ACCESS_REMOTE_WRITE;

    // printf("MR Protection Domain = %p\n", verbs_resources.pd);

    mr = unique_ptr<ibv_mr, std::function<void(ibv_mr *)>>(
        ibv_reg_mr(verbs_resources.pd, const_cast<void *>((const void *)buffer),
                   size, mr_flags),
        [](ibv_mr *m) { ibv_dereg_mr(m); });

    if(!mr) {
        fprintf(stderr, "ibv_reg_mr failed with mr_flags=0x%x\n", mr_flags);
        assert(false);
    }

    // fprintf(
    //     stdout,
    //     "MR was registered with addr=%p, lkey=0x%x, rkey=0x%x, flags=0x%x\n",
    //     buffer, mr->lkey, mr->rkey, mr_flags);
}
uint32_t memory_region::get_rkey() const{
	return mr->rkey;
}
queue_pair::~queue_pair() {
	//    if(qp) cout << "Destroying Queue Pair..." << endl;
}
queue_pair::queue_pair(size_t remote_index) {
    auto &sock = sockets[remote_index];

    ibv_qp_init_attr qp_init_attr;
    memset(&qp_init_attr, 0, sizeof(qp_init_attr));
    qp_init_attr.qp_type = IBV_QPT_RC;
    qp_init_attr.sq_sig_all = 1;
    qp_init_attr.send_cq = verbs_resources.cq;
    qp_init_attr.recv_cq = verbs_resources.cq;
    qp_init_attr.cap.max_send_wr = 16;
    qp_init_attr.cap.max_recv_wr = 16;
    qp_init_attr.cap.max_send_sge = 1;
    qp_init_attr.cap.max_recv_sge = 1;

    qp = unique_ptr<ibv_qp, std::function<void(ibv_qp *)>>(
        ibv_create_qp(verbs_resources.pd, &qp_init_attr),
        [](ibv_qp *q) { ibv_destroy_qp(q); });

    
    if(!qp) {
        fprintf(stderr, "failed to create QP\n");
        assert(false);
    }

    struct config_t config = config_vec[node_rank];

    struct cm_con_data_t local_con_data;
    struct cm_con_data_t remote_con_data;
    memset(&local_con_data, 0, sizeof(local_con_data));
    memset(&remote_con_data, 0, sizeof(remote_con_data));
    int rc = 0;
    union ibv_gid my_gid;

    if(config.gid_idx >= 0) {
        rc = ibv_query_gid(verbs_resources.ib_ctx, config.ib_port,
                           config.gid_idx, &my_gid);
        if(rc) {
            fprintf(stderr, "could not get gid for port %d, index %d\n",
                    config.ib_port, config.gid_idx);
            return;
        }
    } else {
        memset(&my_gid, 0, sizeof my_gid);
    }

    /* exchange using TCP sockets info required to connect QPs */
    local_con_data.qp_num = qp->qp_num;
    local_con_data.lid = verbs_resources.port_attr.lid;
    memcpy(local_con_data.gid, &my_gid, 16);
    // fprintf(stdout, "Local QP number  = 0x%x\n", qp->qp_num);
    // fprintf(stdout, "Local LID        = 0x%x\n", verbs_resources.port_attr.lid);

    remote_con_data = sock.exchange(local_con_data);

    /* save the remote side attributes, we will need it for the post SR */
    //    verbs_resources.remote_props = remote_con_data;
    // fprintf(stdout, "Remote QP number = 0x%x\n", remote_con_data.qp_num);
    // fprintf(stdout, "Remote LID       = 0x%x\n", remote_con_data.lid);
    // if(config.gid_idx >= 0) {
    //     uint8_t *p = remote_con_data.gid;
    //     fprintf(stdout,
    //             "Remote GID = "
    //             "%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%"
    //             "02x:%02x:%02x:%02x:%02x\n",
    //             p[0], p[1], p[2], p[3], p[4], p[5], p[6], p[7], p[8], p[9],
    //             p[10], p[11], p[12], p[13], p[14], p[15]);
    // }
    /* modify the QP to init */

    bool success =
        !modify_qp_to_init(qp.get()) &&
        !modify_qp_to_rtr(qp.get(), remote_con_data.qp_num, remote_con_data.lid,
                          remote_con_data.gid) &&
        !modify_qp_to_rts(qp.get());

    if(!success)
        printf("Failed to initialize QP\n");

    /* sync to make sure that both sides are in states that they can connect to
   * prevent packet loss */
    /* just send a dummy char back and forth */
    sock.exchange(0);
}
bool queue_pair::post_send(const memory_region &mr, size_t offset,
                           size_t length, uint64_t wr_id, uint32_t immediate) {
    if(mr.size < offset + length) return false;

    ibv_send_wr sr;
    ibv_sge sge;
    ibv_send_wr *bad_wr = NULL;

    // prepare the scatter/gather entry
    memset(&sge, 0, sizeof(sge));
    sge.addr = (uintptr_t)(mr.buffer + offset);
    sge.length = length;
    sge.lkey = mr.mr->lkey;

    // prepare the send work request
    memset(&sr, 0, sizeof(sr));
    sr.next = NULL;
    sr.wr_id = wr_id;
    sr.imm_data = immediate;
    sr.sg_list = &sge;
    sr.num_sge = 1;
    sr.opcode = IBV_WR_SEND_WITH_IMM;
    sr.send_flags = IBV_SEND_SIGNALED;  // | IBV_SEND_INLINE;

    if(ibv_post_send(qp.get(), &sr, &bad_wr)) {
        fprintf(stderr, "failed to post SR\n");
        return false;
    }
    return true;
}
bool queue_pair::post_empty_send(uint64_t wr_id, uint32_t immediate) {
    ibv_send_wr sr;
    ibv_send_wr *bad_wr = NULL;

    // prepare the send work request
    memset(&sr, 0, sizeof(sr));
    sr.next = NULL;
    sr.wr_id = wr_id;
    sr.imm_data = immediate;
    sr.sg_list = NULL;
    sr.num_sge = 0;
    sr.opcode = IBV_WR_SEND_WITH_IMM;
    sr.send_flags = IBV_SEND_SIGNALED;  // | IBV_SEND_INLINE;

    if(ibv_post_send(qp.get(), &sr, &bad_wr)) {
        fprintf(stderr, "failed to post SR\n");
        return false;
    }
    return true;
}

bool queue_pair::post_recv(const memory_region &mr, size_t offset,
                           size_t length, uint64_t wr_id) {
    if(mr.size < offset + length) return false;

    ibv_recv_wr rr;
    ibv_sge sge;
    ibv_recv_wr *bad_wr;

    // prepare the scatter/gather entry
    memset(&sge, 0, sizeof(sge));
    sge.addr = (uintptr_t)(mr.buffer + offset);
    sge.length = length;
    sge.lkey = mr.mr->lkey;

    // prepare the receive work request
    memset(&rr, 0, sizeof(rr));
    rr.next = NULL;
    rr.wr_id = wr_id;
    rr.sg_list = &sge;
    rr.num_sge = 1;

    if(ibv_post_recv(qp.get(), &rr, &bad_wr)) {
        fprintf(stderr, "failed to post RR\n");
        fflush(stdout);
        return false;
    }
    return true;
}
bool queue_pair::post_empty_recv(uint64_t wr_id){
    ibv_recv_wr rr;
    ibv_recv_wr *bad_wr;

    // prepare the receive work request
    memset(&rr, 0, sizeof(rr));
    rr.next = NULL;
    rr.wr_id = wr_id;
    rr.sg_list = NULL;
    rr.num_sge = 0;

    if(ibv_post_recv(qp.get(), &rr, &bad_wr)) {
        fprintf(stderr, "failed to post RR\n");
        fflush(stdout);
        return false;
    }
    return true;
}
bool queue_pair::post_write(const memory_region &mr, size_t offset,
                            size_t length, uint64_t wr_id,
                            remote_memory_region remote_mr,
                            size_t remote_offset, bool signaled,
                            bool send_inline) {
    if(mr.size < offset + length || remote_mr.size < remote_offset + length){
        cout << "mr.size = " << mr.size << " offset = " << offset
             << " length = " << length << " remote_mr.size = " << remote_mr.size
             << " remote_offset = " << remote_offset;
        return false;
	}
	
    ibv_send_wr sr;
    ibv_sge sge;
    ibv_send_wr *bad_wr = NULL;

    // prepare the scatter/gather entry
    memset(&sge, 0, sizeof(sge));
    sge.addr = (uintptr_t)(mr.buffer + offset);
    sge.length = length;
    sge.lkey = mr.mr->lkey;

    // prepare the send work request
    memset(&sr, 0, sizeof(sr));
    sr.next = NULL;
    sr.wr_id = wr_id;
    sr.sg_list = &sge;
    sr.num_sge = 1;
    sr.opcode = IBV_WR_RDMA_WRITE;
    sr.send_flags = (signaled ? IBV_SEND_SIGNALED : 0) |
                    (send_inline ? IBV_SEND_INLINE : 0);
    sr.wr.rdma.remote_addr = remote_mr.buffer + remote_offset;
	sr.wr.rdma.rkey = remote_mr.rkey;
	
    if(ibv_post_send(qp.get(), &sr, &bad_wr)) {
        fprintf(stderr, "failed to post SR\n");
        return false;
    }
    return true;
}

int poll_for_completions(int num, ibv_wc *wcs, atomic<bool> &shutdown_flag) {
    while(true) {
        int poll_result = ibv_poll_cq(verbs_resources.cq, num, wcs);
        if(poll_result != 0 || shutdown_flag) {
            return poll_result;
        }
    }

    // if(poll_result < 0) {
    //     /* poll CQ failed */
    //     fprintf(stderr, "poll CQ failed\n");
    // } else {
    //     return
    //     /* CQE found */
    //     fprintf(stdout, "completion was found in CQ with status 0x%x\n",
    //             wc.status);
    //     /* check the completion status (here we don't care about the
    //     completion
    //      * opcode */
    //     if(wc.status != IBV_WC_SUCCESS) {
    //         fprintf(
    //             stderr,
    //             "got bad completion with status: 0x%x, vendor syndrome:
    //             0x%x\n",
    //             wc.status, wc.vendor_err);
    //     }
    // }
}
map<uint32_t, remote_memory_region> verbs_exchange_memory_regions(
    const memory_region& mr) {
    map<uint32_t, remote_memory_region> remote_mrs;
	for(uint32_t i = 0; i < num_nodes; i++){
		if(i != node_rank){
			uint64_t buffer;
			size_t size;
			uint32_t rkey;
			
			buffer = sockets[i].exchange<uint64_t>((uintptr_t)mr.buffer);
			size = sockets[i].exchange<size_t>(mr.size);
			rkey = sockets[i].exchange<uint64_t>(mr.get_rkey());
            remote_mrs.emplace((uint32_t)i,
                               remote_memory_region(buffer, size, rkey));

			cout << buffer << " " << size << " " << rkey << endl;
        }
	}
	return remote_mrs;
}
