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
    bool verbose; // verbose mode.

    constexpr static const char *NAME = "CmdLineOption";

    explicit CmdLineOption(const std::string& description) {
        setDescription(description);
        appendMust(&nrTh, "th", "[num]: number of worker threads.");
        appendOpt(&runSec, 10, "p", "[second]: running period (default: 10).");
        appendOpt(&nrLoop, 1, "loop", "[num]: number of run (default: 1).");
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
    }
};
