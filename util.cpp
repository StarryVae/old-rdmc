
#include "util.h"
#include "verbs_helper.h"

#include <boost/array.hpp>
#include <boost/asio.hpp>
#include <cinttypes>
#include <cstdint>
#include <fstream>
#include <iostream>
#include <thread>
#include <sstream>
#include <sys/stat.h>

#ifdef USE_SLURM
#include <slurm/slurm.h>
#endif

using namespace std;

// This holds the REALTIME timestamp corresponding to the value on the
// master node once all other nodes have connected. Until this takes
// place, it is initialized to zero.
static uint64_t epoch_start = 0;

template <class T, class U>
T lexical_cast(U u) {
    stringstream s{};
    s << u;
    T t{};
    s >> t;
    return t;
}

bool file_exists(const string &name) {
    struct stat buffer;
    return (stat(name.c_str(), &buffer) == 0);
}

void create_directory(const string &name) {
    mkdir(name.c_str(), S_IRWXU | S_IRWXG | S_IRWXO);
}

// return bits / nanosecond = Gigabits/second
double compute_data_rate(size_t num_bytes, uint64_t sTime, uint64_t eTime) {
    return ((double)num_bytes) * 8.0 / (eTime - sTime);
}
void put_flush(const char *str) {
    //    printf("[%6.3f]%s\n", 1.0e-6 * (get_time() - epoch_start), str);
    puts(str);
    fflush(stdout);
}

// Attempts to init environment using slurm and returns whether it was
// successful.
bool slurm_init_environment()
{
#ifdef USE_SLURM
    char *nodeid_ptr = getenv("SLURM_NODEID");
    char *nnodes_ptr = getenv("SLURM_NNODES");
    char* hostnames = getenv("SLURM_JOB_NODELIST");
	if(!nodeid_ptr || !nnodes_ptr || !hostnames)
		return false;
	
	hostlist_t hostlist = slurm_hostlist_create(hostnames);
	if(!hostlist)
	    return false;

	char* host;
	while((host = slurm_hostlist_shift(hostlist))){
	    rdmc::node_addresses.push_back(host);
	}

    slurm_hostlist_destroy(hostlist);

	rdmc::node_rank = lexical_cast<unsigned int>(string(nodeid_ptr));
	rdmc::num_nodes = lexical_cast<unsigned int>(string(nnodes_ptr));

	assert(rdmc::node_addresses.size() == rdmc::num_nodes);
	assert(rdmc::node_rank < rdmc::num_nodes);
	return true;
#else
	return false;
#endif
}

void query_addresses(map<uint32_t, string> &addresses, uint32_t &node_rank) {
	uint32_t num_nodes;
	cout << "Please enter '[node_rank] [num_nodes]': ";
	cin >> node_rank >> num_nodes;

	string addr;
	for(uint32_t i = 0; i < num_nodes; ++i) {
		// input the connection information here
		cout << "Please enter IP Address for node " << i << ": ";
		cin >> addr;
		addresses.emplace(i, addr);
	}
}

void init_environment() {
	epoch_start = get_time();
}

void do_barrier() {}
double compute_mean(std::vector<double> v) {
    double sum = std::accumulate(v.begin(), v.end(), 0.0);
    return sum / v.size();
}
double compute_stddev(std::vector<double> v) {
    double mean = compute_mean(v);
    double sq_sum = std::inner_product(v.begin(), v.end(), v.begin(), 0.0);
    return std::sqrt(sq_sum / v.size() - mean * mean);
}

vector<event> events;
std::mutex events_mutex;
void flush_events() {
    std::unique_lock<std::mutex> lock(events_mutex);

	auto basename = [](const char *path) {
		const char *base = strrchr(path, '/');
		return base ? base + 1 : path;
	};

	
    static bool print_header = true;
    if(print_header) {
        printf(
            "time, file:line, event_name, group_number, message_number, "
            "block_number\n");
        print_header = false;
    }
    for(const auto &e : events) {
        if(e.group_number == (uint32_t)(-1)) {
            printf("%5.3f, %s:%d, %s\n", 1.0e-6 * (e.time - epoch_start),
                   basename(e.file), e.line, e.event_name);

        } else if(e.message_number == (size_t)(-1)) {
			printf("%5.3f, %s:%d, %s, %" PRIu32 "\n",
				   1.0e-6 * (e.time - epoch_start), basename(e.file), e.line,
				   e.event_name, e.group_number);

        } else if(e.block_number == (size_t)(-1)) {
			printf("%5.3f, %s:%d, %s, %" PRIu32 ", %zu\n",
				   1.0e-6 * (e.time - epoch_start), basename(e.file),
				   e.line, e.event_name, e.group_number, e.message_number);

        } else {
            printf("%5.3f, %s:%d, %s, %" PRIu32 ", %zu, %zu\n",
				   1.0e-6 * (e.time - epoch_start), basename(e.file),
				   e.line, e.event_name, e.group_number, e.message_number,
				   e.block_number);
        }
    }
    fflush(stdout);
    events.clear();
}
