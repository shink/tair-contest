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
    return str_hash_(key) % BUCKET_NUM;
}


inline void NvmEngine::Append(const Slice &key, const Slice &value, uint16_t index) {
    char *p = buckets_[index].ptr + buckets_[index].end_off;
    memcpy(p, key.data(), KEY_SIZE);
    memcpy(p + KEY_SIZE, value.data(), VALUE_SIZE);
    buckets_[index].end_off += PAIR_SIZE;
}


Status NvmEngine::Get(const Slice &key, std::string *value) {
    if (++get_count_ % DISPLAY_NUM == 0) {
        PrintLog("[NvmEngine::Get] get count: %u\n", get_count_);
    }

    uint16_t index = Hash(key.to_string());

    //  若桶已满，则先查找 fast_map_
    if (buckets_[index].end_off == BUCKET_SIZE) {
        auto kv = fast_map_[index].find(key.to_string());
        if (kv != fast_map_[index].end()) {
            *value = kv->second;
            return Ok;
        }
    }

    //  从 aep 中查找
    char *p = buckets_[index].ptr;
    uint64_t right = buckets_[index].end_off;
    uint64_t left = 0;

    while (left < right) {
        if (memcmp(p + left, key.data(), KEY_SIZE) == 0) {
            value->assign(p + left + KEY_SIZE, VALUE_SIZE);
            return Ok;
        }
        left += PAIR_SIZE;
    }

    return NotFound;
}


Status NvmEngine::Set(const Slice &key, const Slice &value) {
    if (++set_count_ % DISPLAY_NUM == 0) {
        PrintLog("[NvmEngine::Set] set count: %u\n", set_count_);
    }

    uint16_t index = Hash(key.to_string());

    char *p = buckets_[index].ptr;
    uint64_t right = buckets_[index].end_off;
    uint64_t left = 0;

    while (left < right) {
        if (memcmp(p + left, key.data(), KEY_SIZE) == 0) {
            memcpy(p + left + KEY_SIZE, value.data(), VALUE_SIZE);
            return Ok;
        }
        left += PAIR_SIZE;
    }

    //  如果桶已满，则直接放 fast_map_
    if (right == BUCKET_SIZE) {
        std::lock_guard<std::mutex> lock(mut_[index]);
        fast_map_[index][key.to_string()] = value.to_string();
    } else {
        memcpy(p + right, key.data(), KEY_SIZE);
        memcpy(p + right + KEY_SIZE, value.data(), VALUE_SIZE);
        std::lock_guard<std::mutex> lock(mut_[index]);
        buckets_[index].end_off += PAIR_SIZE;
    }

    return Ok;
}


NvmEngine::~NvmEngine() {
#ifdef USE_LIBPMEM
    pmem_unmap(pmem_base_, mapped_size_);
#else
    munmap(pmem_base_, mapped_size_);
#endif

    fclose(log_file_);
}
