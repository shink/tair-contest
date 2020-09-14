/*
 * @author: shenke
 * @date: 2020/9/10
 * @project: tair-contest
 * @desp: 
 */

#ifndef TAIR_CONTEST_KV_CONTEST_LOG_H_
#define TAIR_CONTEST_KV_CONTEST_LOG_H_

//#define CLION   //  Windows Clion CMake 本地调试
//#define LOCAL   //  Centos 本地调试

#ifndef LOCAL
#define PrintLog(...)                                 \
    if (log_file_) {                                  \
        fprintf(log_file_, __VA_ARGS__);              \
        fflush(log_file_);                            \
    }
#else
#define PrintLog(...)                                 \
    printf(__VA_ARGS__);
#endif

#ifndef CLION
#include "include/db.hpp"
#else
#include "db.hpp"
#endif

#endif
