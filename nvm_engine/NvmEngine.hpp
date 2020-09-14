/*
 * @author: shenke
 * @date: 2020/9/7
 * @project: tair-contest
 * @desp:
 */

#ifndef TAIR_CONTEST_KV_CONTEST_NVM_ENGINE_H_
#define TAIR_CONTEST_KV_CONTEST_NVM_ENGINE_H_

#include "Statement.hpp"

#ifndef LOCAL
const size_t MAP_SIZE = 72ull << 30ull;  //  72G（77309411328）
const uint64_t DISPLAY_NUM = 100000000;  //  1亿
#else
const size_t MAP_SIZE = 960ull << 20ull;  //  960M
const uint64_t DISPLAY_NUM = 10000;
#endif

const uint16_t KEY_SIZE = 16;
const uint16_t VALUE_SIZE = 80;
const uint16_t PAIR_SIZE = KEY_SIZE + VALUE_SIZE;
const uint64_t PAIR_NUM = MAP_SIZE / PAIR_SIZE;  //  键值对数量（805306368，不是素数）
//const uint64_t MOD_NUM = 805306457u; //  在 805306368 周围的两个素数分别是 805306357 和 805306457
const uint64_t MOD_NUM = PAIR_NUM;
const uint16_t CONFLICT_THRESHOLD = 1 << 13; //  冲突阈值 （8192）


class NvmEngine : DB {
public:
    /**
     * @param 
     * name: file in AEP(exist)
     * dbptr: pointer of db object
     */
    NvmEngine(const std::string &name, FILE *log_file = nullptr);

    static Status CreateOrOpen(const std::string &name, DB **dbptr, FILE *log_file = nullptr);

    Status Get(const Slice &key, std::string *value) override;

    Status Set(const Slice &key, const Slice &value) override;

    ~NvmEngine() override;

private:
    void BuildMapping(const std::string &name, size_t size);

    inline uint64_t Hash(const std::string &key);

private:
    char *pmem_base_;
    size_t mapped_size_;
#ifdef USE_LIBPMEM
    int is_pmem_;
#endif
    std::hash<std::string> str_hash_;
    std::bitset<MOD_NUM> bit_set_;
    std::unordered_map<std::string, std::string> hash_map_;
    uint16_t conflict_count_[MOD_NUM];
    uint64_t get_count_;
    uint64_t set_count_;
    FILE *log_file_;
};

#endif
