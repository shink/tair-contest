/*
 * @author: shenke
 * @date: 2020/9/7
 * @project: tair-contest
 * @desp:
 */

#include <cstdio>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <fcntl.h>
#include <unistd.h>
#include <mutex>
#include <unordered_map>
#include "NvmEngine.hpp"

#define LIKELY(x) (__builtin_expect((x), 1))
#define UNLIKELY(x) (__builtin_expect((x), 0))


//  <-------- DB -------->

Status DB::CreateOrOpen(const std::string &name, DB **dbptr, FILE *log_file) {
    return NvmEngine::CreateOrOpen(name, dbptr, log_file);
}


DB::~DB() {}

//  <-------- NvmEngine -------->

NvmEngine::NvmEngine(const std::string &name, FILE *log_file) : get_count_(0), set_count_(0), log_file_(log_file) {
    BuildMapping(name, MAP_SIZE);
    InitBucket();
}


Status NvmEngine::CreateOrOpen(const std::string &name, DB **dbptr, FILE *log_file) {
    NvmEngine *db = new NvmEngine(name, log_file);
    *dbptr = db;
    return Ok;
}


void NvmEngine::BuildMapping(const std::string &name, size_t size) {
#ifdef USE_LIBPMEM
    if ((pmem_base_ = (char *) pmem_map_file(name.c_str(), size, PMEM_FILE_CREATE, 0666, &mapped_size_, &is_pmem_)) ==
        NULL) {
        perror("[NvmEngine::BuildMapping] pmem map file failed");
        exit(1);
    } else {
        PrintLog("[NvmEngine::BuildMapping] pmem map file successed\n");
    }
#else
    int fd = open(name.c_str(), O_RDWR | O_CREAT, 00777);
    lseek(fd, size - 1, SEEK_END);
    write(fd, " ", 1);
    pmem_base_ = (char *) mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (pmem_base_ == MAP_FAILED) {
        PrintLog("[NvmEngine::BuildMapping] mmap failed\n");
        perror("mmap failed");
        exit(1);
    } else {
        mapped_size_ = size;
        PrintLog("[NvmEngine::BuildMapping] mmap successed\n");
    }
#endif
}


void NvmEngine::InitBucket() {
    uint64_t delta = 0;
    for (auto &bucket : buckets_) {
        bucket.ptr = pmem_base_ + delta;
        delta += BUCKET_NUM;
    }
}


inline uint16_t NvmEngine::Hash(const std::string &key) {
    return str_hash_(key) & (BUCKET_NUM - 1);
}


Status NvmEngine::Get(const Slice &key, std::string *value) {
    if (++get_count_ % DISPLAY_NUM == 0) {
        std::lock_guard<std::mutex> lock(log_mut_);
        PrintLog("[NvmEngine::Get] get count: %u\n", get_count_);
    }

    uint16_t index = Hash(key.to_string());

    if (!fast_map_[index].empty()) {
        auto kv = fast_map_[index].find(key.to_string());
        if (kv != fast_map_[index].end()) {
            *value = kv->second;
            return Ok;
        }
    }

    *value = "aoisodhgghwioghwrghwiwnghreunrwohorenhiohioeraioerhioerjhgwjrnuihgjakwnhgrnwhwrh";
    return Ok;
}


Status NvmEngine::Set(const Slice &key, const Slice &value) {
    if (++set_count_ % DISPLAY_NUM == 0) {
        std::lock_guard<std::mutex> lock(log_mut_);
        PrintLog("[NvmEngine::Set] set count: %u\n", set_count_);
    }

    uint16_t index = Hash(key.to_string());

    if (fast_map_[index].size() < FAST_MAP_SIZE) {
        std::lock_guard<std::mutex> lock(mut_[index]);
        fast_map_[index][key.to_string()] = value.to_string();
        return Ok;
    }

//    memcpy(buckets_[index].ptr + buckets_[index].end_off, key.data(), KEY_SIZE);
//    memcpy(buckets_[index].ptr + buckets_[index].end_off + KEY_SIZE, value.data(), VALUE_SIZE);
//    std::lock_guard<std::mutex> lock(mut_[index]);
//    buckets_[index].end_off += PAIR_SIZE;

    return Ok;
}


NvmEngine::~NvmEngine() {
#ifdef USE_LIBPMEM
    pmem_unmap(pmem_base_, mapped_size_);
#else
    munmap(pmem_base_, mapped_size_);
#endif

    uint64_t size_max = 0;
    uint64_t size_sum = 0;
    for (auto &map : fast_map_) {
        size_sum += map.size();
        size_max = std::max(size_max, map.size());
    }
    uint64_t size_avg = size_sum / BUCKET_SIZE;
    PrintLog("fast_map_, size_max = %u\n", size_max);
    PrintLog("fast_map_, size_avg = %u\n", size_avg);

    fclose(log_file_);
}
