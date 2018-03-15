#pragma once

#include <vector>
#include <mutex>
#include <cstdlib>
#include "vector_payload.hpp"
#include "arch.hpp"


template <typename T>
class PartitionedVectorWithPayload
{
    using Vec = VectorWithPayload<T>;
    using VecPtr = std::unique_ptr<Vec>;
    using Elem = DataWithPayload<T>;
    std::vector<VecPtr> vv_;
    mutable std::mutex mu_;
    size_t nrNode_;
    size_t sizePerNode_;
    size_t payloadSize_;
    size_t alignmentSize_;
    size_t totalSize_;
public:
    PartitionedVectorWithPayload() = default;
    void setSizes(size_t nrNode, size_t sizePerNode, size_t payloadSize, size_t alignmentSize = sizeof(uintptr_t)) {
        vv_.resize(nrNode);
        nrNode_ = nrNode;
        sizePerNode_ = sizePerNode;
        payloadSize_ = payloadSize;
        alignmentSize_ = alignmentSize;
        totalSize_ = nrNode * sizePerNode;
    }
    /*
     * Each worker thread must call this to allocate memory
     * at its appropriate numa node.
     */
    void allocate(size_t nodeId) {
        std::lock_guard<std::mutex> lk(mu_);
        assert(nodeId < vv_.size());
        VecPtr& v = vv_[nodeId];
        // This will be reused.
        if (!v) {
            v.reset(new Vec());
            v->setPayloadSize(payloadSize_, alignmentSize_);
            v->resize(sizePerNode_);
        }
    }
    DataWithPayload<T>& operator[](size_t pos) {
        size_t nodeId, posInNode;
        getRealPos(pos, nodeId, posInNode);
        VecPtr& v = vv_[nodeId];
        assert(bool(v));
        return (*v)[posInNode];
    }
    const DataWithPayload<T>& operator[](size_t pos) const {
        size_t nodeId, posInNode;
        getRealPos(pos, nodeId, posInNode);
        VecPtr& v = vv_[nodeId];
        assert(bool(v));
        return (*v)[posInNode];
    }
    size_t size() const {
        assert(nrNode_ = vv_.size());
        assert(nrNode * sizePerNode_ == totalSize_);
        return totalSize_;
    }

    bool isReady() const {
        std::lock_guard<std::mutex> lk(mu_);
        for (size_t i = 0; i < nrNode_; i++) {
            if (!vv_[i]) return false;
        }
        return true;
    }
    void checkAndWait() {
        while (!isReady()) {
            _mm_pause();
        }
    }
private:
    void getRealPos(size_t pos, size_t& nodeId, size_t& posInNode) const {
        assert(pos < totalSize_);
#if 0
        nodeId = pos / sizePerNode_;
        posInNode = pos % sizePerNode_;
#else
        const ldiv_t d = ::ldiv(pos, sizePerNode_);
        nodeId = d.quot;
        posInNode = d.rem;
#endif
        assert(nodeId < nrNode_);
    }
};
