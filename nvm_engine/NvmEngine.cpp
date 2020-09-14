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
#include <bitset>
#include <unordered_map>
#include "NvmEngine.hpp"

//  <-------- DB -------->

Status DB::CreateOrOpen(const std::string &name, DB **dbptr, FILE *log_file) {
    return NvmEngine::CreateOrOpen(name, dbptr, log_file);
}


DB::~DB() {}

//  <-------- NvmEngine -------->

NvmEngine::NvmEngine(const std::string &name, FILE *log_file) : get_count_(0), set_count_(0), log_file_(log_file) {
    memset(conflict_count_, 0, sizeof(uint16_t) * MOD_NUM);
    BuildMapping(name, MAP_SIZE);
}


Status NvmEngine::CreateOrOpen(const std::string &name, DB **dbptr, FILE *log_file) {
    NvmEngine *db = new NvmEngine(name, log_file);
    *dbptr = db;
    return Ok;
}


void NvmEngine::BuildMapping(const std::string &name, size_t size) {
#ifdef USE_LIBPMEM
    if ((pmem_base_ = (char *) pmem_map_file(name.c_str(), size, PMEM_FILE_CREATE, 0666, &mapped_size_, &is_pmem_)) == NULL) {
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


inline uint64_t NvmEngine::Hash(const std::string &key) {
    return str_hash_(key) % MOD_NUM;
}


Status NvmEngine::Get(const Slice &key, std::string *value) {
    if (++get_count_ % DISPLAY_NUM == 0) {
        PrintLog("[NvmEngine::Get] get count: %u\n", get_count_);
    }

    uint64_t index = Hash(key.to_string());
    uint16_t count = conflict_count_[index];

    //  若 hash_map_ 中存在 key，则快速读取 hash_map_
    if (count == CONFLICT_THRESHOLD) {
        auto kv = hash_map_.find(key.to_string());
        if (kv != hash_map_.end()) {
            *value = kv->second;
            return Ok;
        }
    }

    //  顺序探测
    uint64_t i = index;
    for (uint16_t j = 0; j <= count; ++j) {
        if (!bit_set_.test(i)) {
            return NotFound;
        }

        if (memcmp(pmem_base_ + i * PAIR_SIZE, key.data(), KEY_SIZE) == 0) {
            value->assign(pmem_base_ + i * PAIR_SIZE + KEY_SIZE, VALUE_SIZE);
            return Ok;
        }

        if (++i == MOD_NUM) {
            i = 0ull;
        }
    }

    return NotFound;
}


Status NvmEngine::Set(const Slice &key, const Slice &value) {
    if (++set_count_ % DISPLAY_NUM == 0) {
        PrintLog("[NvmEngine::Set] set count: %u\n", set_count_);
    }

    //  若该处冲突次数已经大于阈值，并且 hash_map_ 中存在，则快速更新 hash_map_
    uint64_t index = Hash(key.to_string());
    if (conflict_count_[index] >= CONFLICT_THRESHOLD) {
        auto kv = hash_map_.find(key.to_string());
        if (kv != hash_map_.end()) {
            hash_map_[key.to_string()] = value.to_string();
            return Ok;
        }
    }

    //  根据冲突次数判断，若冲突次数大于阈值，则写入 hash_map_，若小于阈值，则写入 AEP
    uint64_t i = index;
    uint16_t count = 0;

    while (true) {
        if (!bit_set_.test(i)) {
            bit_set_.set(i);
            conflict_count_[index] = std::max(conflict_count_[index], count);
            memcpy(pmem_base_ + i * PAIR_SIZE, key.data(), KEY_SIZE);
            memcpy(pmem_base_ + i * PAIR_SIZE + KEY_SIZE, value.data(), VALUE_SIZE);
            return Ok;
        }
        if (memcmp(pmem_base_ + i * PAIR_SIZE, key.data(), KEY_SIZE) == 0) {
            conflict_count_[index] = std::max(conflict_count_[index], count);
            memcpy(pmem_base_ + i * PAIR_SIZE + KEY_SIZE, value.data(), VALUE_SIZE);
            return Ok;
        }
        ++count;

        if (count == CONFLICT_THRESHOLD) {
            conflict_count_[index] = CONFLICT_THRESHOLD;
            hash_map_[key.to_string()] = value.to_string();
            return Ok;
        }

        if (++i == MOD_NUM) {
            i = 0ull;
        }
    }
}


NvmEngine::~NvmEngine() {
#ifdef USE_LIBPMEM
    pmem_unmap(pmem_base_, mapped_size_);
#else
    munmap(pmem_base_, mapped_size_);
#endif

    fclose(log_file_);
}
