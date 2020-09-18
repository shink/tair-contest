/*
 * @author: shenke
 * @date: 2020/9/7
 * @project: tair-contest
 * @desp:
 */

#ifndef TAIR_CONTEST_KV_CONTEST_NVM_ENGINE_H_
#define TAIR_CONTEST_KV_CONTEST_NVM_ENGINE_H_

#include "Statement.hpp"


struct bucket {
    char *ptr;
    uint64_t end_off;
    uint64_t next_loc;
};


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
    inline void BuildMapping(const std::string &name, size_t size);

    inline void InitBucket();

    inline uint16_t Hash(const std::string &key);

    inline void Append(const Slice &key, const Slice &value, uint16_t index);

private:
    char *pmem_base_;
    size_t mapped_size_;

#ifdef USE_LIBPMEM
    int is_pmem_;
#endif

#ifndef LOCAL
    const static size_t MAP_SIZE = 72ull << 30ull;  //  72G（77309411328）
    const static uint64_t DISPLAY_NUM = 100000000;  //  1亿
#else
    const static size_t MAP_SIZE = 960ull << 20ull;  //  960M
    const static uint64_t DISPLAY_NUM = 100000;
#endif

    const static uint64_t KEY_SIZE = 16;
    const static uint64_t VALUE_SIZE = 80;
    const static uint64_t PAIR_SIZE = KEY_SIZE + VALUE_SIZE;
    const static uint64_t PAIR_NUM = MAP_SIZE / PAIR_SIZE;  //  键值对数量（805306368，不是素数，805306457是素数）
    const static uint16_t BUCKET_NUM = 1ull << 10ull;    //  1024 个桶
    const static uint64_t BUCKET_SIZE = MAP_SIZE / BUCKET_NUM; //  72M（75497472）
    const static uint16_t MIN_MAP_SIZE = 2u << 10u; //  每个 fast_map 至少存 2048 条 kv 对
    const static uint8_t MAX_CONFLICT_NUM = 5; //  每次搜索最多试探 5 个桶

    std::hash<std::string> str_hash_;
    std::mutex mut_[BUCKET_NUM];
    std::unordered_map<std::string, std::string> fast_map_[BUCKET_NUM];
    bucket buckets_[BUCKET_NUM];
    uint64_t get_count_;
    uint64_t set_count_;
    FILE *log_file_;
};

#endif
