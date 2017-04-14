#pragma once

#include "cybozu/option.hpp"
#include "cybozu/exception.hpp"
#include <string>

struct CmdLineOption : cybozu::Option
{
    using base = cybozu::Option;

    size_t nrTh; // Number of worker threads (concurrency).
    size_t runSec; // Running period [sec].
    size_t nrLoop; // Number of run.
    size_t nrMuPerTh; // number of mutexes per thread
    size_t nrMu;  // total number of mutexes. (used if nrMuPerTh is 0)
    std::string workload; // workload name.
    size_t longTxSize; // long transaction size. (0 means no long tx exists.)
    size_t nrOp; // Number of total operations of short transactions.
    size_t nrWr; // Number of write operations of short transactions.
    int shortTxMode; // Short transaction mode. See enum TxMode.
    int longTxMode; // Long transaction mode. See enum TxMode.
    bool verbose; // verbose mode.

    constexpr static const char *NAME = "CmdLineOption";

    explicit CmdLineOption(const std::string& description) {
        setDescription(description);
        appendMust(&nrTh, "th", "[num]: number of worker threads.");
        appendOpt(&runSec, 10, "p", "[second]: running period (default: 10).");
        appendOpt(&nrLoop, 1, "loop", "[num]: number of run (default: 1).");
        appendOpt(&nrMuPerTh, 0, "mupt", "[num]: number of mutexes per thread (use this for shortlong workload).");
        appendOpt(&nrMu, 0, "mu", "[num]: total number of mutexes (use this for other workloads).");
        appendOpt(&workload, "custom", "w", "[workload]: workload type in 'custom', 'custom-t' etc.");
        appendOpt(&longTxSize, 0, "long-tx-size", "[size]: long tx size for shortlong workload. 0 means no long tx.");
        appendOpt(&nrOp, 4, "nrop", "[num]: number of operations of short transactions (default:4).");
        appendOpt(&nrWr, 2, "nrwr", "[num]: number of write operations of short transactions (default:2).");
        appendOpt(&shortTxMode, 0, "sm", "[id]: short Tx mode (0:last-writes, 1:first-writes, 2:read-only, 3:write-only, 4:half-and-half, 5:mix)");
        appendOpt(&longTxMode, 0, "lm", "[id]: long Tx mode (0:last-writes, 1:first-writes, 2:read-only, 4:half-and-half)");
        appendBoolOpt(&verbose, "v", ": puts verbose messages.");
        appendHelp("h", ": put this message.");
    }
    void parse(int argc, char *argv[]) {
        if (!base::parse(argc, argv)) {
            usage();
            ::exit(1);
        }
        if (nrTh == 0) {
            throw cybozu::Exception(NAME) << "nrTh must not be 0.";
        }
        if (runSec == 0) {
            throw cybozu::Exception(NAME) << "runSec must not be 0.";
        }
        if (nrLoop == 0) {
            throw cybozu::Exception(NAME) << "nrLoop must not be 0.";
        }
        if (nrMuPerTh == 0 && nrMu == 0) {
            throw cybozu::Exception(NAME) << "nrMuPerTh or nrMu must not be 0.";
        }
        if (longTxSize > getNrMu()) {
            throw cybozu::Exception(NAME) << "longTxSize is too large: up to nrMuPerTh * nrTh.";
        }
        if (nrOp < nrWr) {
            throw cybozu::Exception(NAME) << "nrOp must be >= nrWr.";
        }
    }
    size_t getNrMuPerTh() const {
        return nrMuPerTh > 0 ? nrMuPerTh : nrMu / nrTh;
    }
    size_t getNrMu() const {
        return nrMuPerTh > 0 ? nrMuPerTh * nrTh : nrMu;
    }
    virtual std::string str() const {
        return cybozu::util::formatString(
            "concurrency:%zu workload:%s nrMutex:%zu nrMuPerTh:%zu "
            "sec:%zu longTxSize:%zu nrOp:%zu nrWr:%zu shortTxMode:%d longTxMode:%d"
            , nrTh, workload.c_str(), getNrMu(), getNrMuPerTh()
            , runSec, longTxSize, nrOp, nrWr, shortTxMode, longTxMode);
    }
};
