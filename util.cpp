
#include "util.h"
#include "psm_helper.h"

#include <boost/array.hpp>
#include <boost/asio.hpp>
#include <fstream>
#include <iostream>
#include <thread>
#include <slurm/slurm.h>
#include <sstream>
#include <sys/stat.h>

using namespace std;


// This holds the REALTIME timestamp corresponding to the value on the
// master node once all other nodes have connected. Until this takes
// place, it is initialized to zero.
static uint64_t epoch_start = 0;

template<class T, class U>
T lexical_cast(U u) {
    stringstream s{};
    s << u;
    T t{};
    s >> t;
    return t;
}

bool file_exists (const string& name) {
    struct stat buffer;   
    return (stat (name.c_str(), &buffer) == 0); 
}

void create_directory(const string& name) {
    mkdir("/home/cnd/mod1", S_IRWXU | S_IRWXG | S_IRWXO);
}

//return bits / nanosecond = Gigabits/second
double compute_data_rate(size_t num_bytes, uint64_t sTime, uint64_t eTime){
    return ((double)num_bytes) * 8.0 / (eTime - sTime);
}
void put_flush(const char* str){
    printf("[%6.3f]%s\n", 1.0e-6 * (get_time()-epoch_start), str);
    fflush(stdout);
}

void init_environment(){
    rdmc::job_number = lexical_cast<unsigned int, string>(getenv("SLURM_JOBID"));
    rdmc::job_step = lexical_cast<unsigned int, string>(getenv("SLURM_STEPID"));
    rdmc::node_rank = lexical_cast<unsigned int, string>(getenv("SLURM_NODEID"));
    rdmc::num_nodes = lexical_cast<unsigned int, string>(getenv("SLURM_NNODES"));
}

void find_connections(uint64_t addr){
    using namespace rdmc;

    put_flush("starting find_connections");
    //create directory to exchange connection info
    create_directory("connection");

    //form filename prefix
    string filename_prefix = "connection/" + lexical_cast<string>(job_number) +
        "." + lexical_cast<string>(job_step) + "-";

    //output our connection info
    string filename = filename_prefix + lexical_cast<string>(node_rank);    
    ofstream outfile(filename.c_str());
    outfile << addr;
    outfile.close();

    //find other nodes
    for(unsigned int i = 0; i < num_nodes;){
        if(i != node_rank){
            string fname = filename_prefix + lexical_cast<string>(i);
            if(!file_exists(fname)){
                std::this_thread::sleep_for(std::chrono::milliseconds(500));
                continue;
            }
            ifstream fin(fname.c_str());

            uint64_t remote_addr = 0;
            fin >> remote_addr;
            connections[i] = remote_addr;
        }
        ++i;
    }

    put_flush("find_connections done");
}
void find_connections2(uint64_t addr){
    using boost::asio::ip::tcp;
    using namespace rdmc;

    if(node_rank == 0){
        try{
            boost::asio::io_service io_service;
            tcp::acceptor acceptor(io_service, tcp::endpoint(tcp::v4(), 1036));

            vector<unique_ptr<tcp::socket>> sockets;
            for(int i = 0; i < num_nodes - 1; i++){
                sockets.emplace_back(new tcp::socket(io_service));
                acceptor.accept(*sockets.back());

                boost::array<uint64_t, 2> recv_msg;
                sockets.back()->receive(boost::asio::buffer(recv_msg));
                connections[recv_msg[0]] = recv_msg[1];
            }
            put_flush("Modifying epoch");
            epoch_start = get_time();

            vector<uint64_t> addresses;
            addresses.push_back(addr);
            for(int i = 1; i < num_nodes; i++){
                addresses.push_back(connections[i]);
            }
            for(auto& socket : sockets){
                socket->send(boost::asio::buffer(addresses));
                socket->send(boost::asio::buffer(&epoch_start,
                                                 sizeof(epoch_start)));
            }
        }
        catch (std::exception& e){
            put_flush("TCP EXCEPTION");
            std::cerr << e.what() << std::endl;
            exit(0);
        }
    }
    else{
        char* hostnames = getenv("SLURM_STEP_NODELIST");
        hostlist_t step_hostlist = slurm_hostlist_create(hostnames);
        assert(step_hostlist != NULL);

        char* master_hostname = slurm_hostlist_shift(step_hostlist);

        for(int i = 0; i < 32; i++){
            try{
                boost::asio::io_service io_service;
                tcp::resolver resolver(io_service);

                tcp::resolver::query query(master_hostname, "1036");

                tcp::resolver::iterator endpoint_iterator = resolver.resolve(query);
                tcp::resolver::iterator end;
                tcp::socket socket(io_service);
                boost::system::error_code error = boost::asio::error::host_not_found;

                const int MAX_RETRIES = 64;
                for(int i = 0; i < MAX_RETRIES && error; i++){
                    while (error && endpoint_iterator != end)
                    {
                        socket.close();
                        socket.connect(*endpoint_iterator++, error);
                    }

                    if(error){
                        this_thread::sleep_for(chrono::milliseconds(5));
                    }
                }

                boost::array<uint64_t, 2> msg;
                msg[0] = node_rank;
                msg[1] = addr;
                socket.send(boost::asio::buffer(msg));
                vector<uint64_t> addresses(rdmc::num_nodes);
                socket.receive(boost::asio::buffer(addresses));
                socket.receive(boost::asio::buffer(&epoch_start, 
                                                   sizeof(epoch_start)));

                for(int i = 0; i < num_nodes; i++){
                    if(i != node_rank){
                        connections[i] = addresses[i];
                    }
                }
                return;
            }
            catch (std::exception& e)
            {
                put_flush("TCP EXCEPTION");
                std::cerr << "**" << e.what() << "**" << std::endl;
                std::this_thread::sleep_for(std::chrono::milliseconds(500));
            }
        }
    }
}
void do_barrier(){
    using namespace rdmc;

    int64_t tag = ~((int64_t)0);
    int64_t mask = ~((int64_t)0);
    

    if(node_rank == 0){
        uint32_t message;

        for(int i = 1; i < num_nodes; i++){
            psm_mq_req_t req;
            psm_mq_irecv(mq, tag, mask, 0, &message, sizeof(message), NULL, &req);
            psm_mq_wait(&req, NULL);
        }
        for(int i = num_nodes-1; i >= 1; i--){
            psm_mq_send(mq, epaddrs[i], PSM_MQ_FLAG_SENDSYNC, tag, &message,
                        sizeof(message));
        }
    }
    else{
        uint32_t message = 0xFF1234FF;
        psm_mq_send(mq, epaddrs[0], PSM_MQ_FLAG_SENDSYNC, tag, &message,
                    sizeof(message));

        psm_mq_req_t req;
        psm_mq_irecv(mq, tag, mask, 0, &message, sizeof(message), NULL, &req);
        psm_mq_wait(&req, NULL);
    }
}
double compute_mean(std::vector<double> v){
    double sum = std::accumulate(v.begin(), v.end(), 0.0);
    return sum / v.size();
}
double compute_stddev(std::vector<double> v){
    double mean = compute_mean(v);
    double sq_sum = std::inner_product(v.begin(), v.end(), v.begin(), 0.0);
    return std::sqrt(sq_sum / v.size() - mean * mean);
}

vector<event> events;
std::mutex events_mutex;
void flush_events(){
    std::unique_lock<std::mutex> lock(events_mutex);

    static bool print_header = true;
    if(print_header){
        printf("time, file:line, event_name, group_number, message_number, "
               "block_number\n");
        print_header = false;
    }
    for(const auto& e : events){
        printf("%5.3f, %s:%d, %s, %d, %d, %d\n", 1.0e-6 * (e.time - epoch_start),
               e.file, e.line, e.event_name, e.group_number, e.message_number,
               e.block_number);
    }
    fflush(stdout);
    events.clear();
}
