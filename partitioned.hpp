#pragma once

#include <vector>
#include <mutex>
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
public:
    PartitionedVectorWithPayload() = default;
    void setSizes(size_t nrNode, size_t sizePerNode, size_t payloadSize, size_t alignmentSize = sizeof(uintptr_t)) {
        vv_.resize(nrNode);
        nrNode_ = nrNode;
        sizePerNode_ = sizePerNode;
        payloadSize_ = payloadSize;
        alignmentSize_ = alignmentSize;
    }
    /*
     * Each worker thread must call this to allocate memory
     * at its appropriate numa node.
     */
    void allocate(size_t nodeId) {
        std::lock_guard<std::mutex> lk(mu_);
        assert(nodeId < vv_.size());
        VecPtr& v = vv_[nodeId];
        v.reset(new Vec());
        v->setPayloadSize(payloadSize_, alignmentSize_);
        v->resize(sizePerNode_);
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
        return vv_.size() * sizePerNode_;
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
        nodeId = pos / sizePerNode_;
        assert(nodeId < nrNode_);
        posInNode = pos % sizePerNode_;
    }
};
