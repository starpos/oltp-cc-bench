#include <map>
#include "cpuid.hpp"
#include "util.hpp"


void printAffinityModeResult(const std::string& amodeStr)
{                                                                    
    std::map<uint, CpuTopology> topoM;
    for (const CpuTopology& topo : getCpuTopologies()) {
        topoM[topo.id] = topo;
    }
    const CpuAffinityMode amode = parseCpuAffinityMode(amodeStr);    
    std::vector<uint> cpuIdV = getCpuIdList(amode);                  
    for (size_t i = 0; i < cpuIdV.size(); i++) {                     
        const uint cid = cpuIdV[i];
        const CpuTopology& topo = topoM[cid];
        ::printf("worker %4zu\tcpuId %4u\tcore %4u\tsocket %4u\tnode %4u\tthread %4u\n"
            , i, cid, topo.core, topo.socket, topo.node, topo.thread);
    }                                                                
}                                                                    


int main(int argc, char *argv[])
try {
    if (argc < 2) {
        ::printf("specify amode in %s\n", cybozu::util::concat(getAffinityModeStrVec(), ",").c_str());
        return 1;
    }
    printAffinityModeResult(argv[1]);
    return 0;
} catch (std::exception& e) {
    ::fprintf(::stderr, "error: %s\n", e.what());
    return 1;
}
