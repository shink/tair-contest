#include <iostream>
#include <pthread.h>
#include <sys/time.h>
#include <getopt.h>
#include <string>
#include <random>
#include <atomic>
#include <immintrin.h>
#include "random.h"
#include "db.hpp"

using namespace std;

static const int KEY_SIZE = 16;
static const int VALUE_SIZE = 80;

static const int NUM_THREADS = 16;          /* 16T */
static int PER_SET = 48000000;
static int PER_GET = 48000000;
static const uint64_t BASE = 199997;
static struct timeval TIME_START, TIME_END;
static const int MAX_KEY_POOL_SIZE = 1e4 * 10;
static const int MAX_VAL_POOL_SIZE = 1e4 * 10 * 5;
static int KEY_POOL_TOP = 0;
static uint64_t key_pool[MAX_KEY_POOL_SIZE];    /* All generated key */
static int VAL_POOL_TOP = 0;
static uint64_t val_pool[MAX_VAL_POOL_SIZE];    /* All Generated value */
static int MODE = 1;

static DB* db = nullptr;
static vector<uint16_t> pool_seed[16];

static uint64_t seed[] = {
        19, 31, 277, 131, 97, 2333, 19997, 22221,
        217, 89, 73, 31, 17,
        255, 103, 207
};

#define PUT_KEY_TO_POOL(addr) \
    memcpy(key_pool + KEY_POOL_TOP, addr, KEY_SIZE); \
    KEY_POOL_TOP += 2;

#define PUT_VAL_TO_POOL(addr) \
    memcpy(val_pool + VAL_POOL_TOP, addr, VALUE_SIZE); \
    VAL_POOL_TOP += 10;

static void init_pool_seed() {
    for(int i = 0; i < KEY_SIZE; i++) {
        pool_seed[i].resize(16);
        pool_seed[i][0] = seed[i];
        for(int j = 1; j < 16; j++) {
            pool_seed[i][j] = pool_seed[i][j-1] * pool_seed[i][j-1];
        }
    }
}

static void* set_pure(void * id) {
    Random rnd;
    int cnt = PER_SET;
    while (cnt--) {
        unsigned int *start = rnd.nextUnsignedInt();
        Slice data_key((char *) start, 16);
        Slice data_value((char *) (start + 4), 80);

        if (((cnt & 0x7777) ^ 0x7777) == 0) {
            PUT_KEY_TO_POOL(start);
            PUT_VAL_TO_POOL(start + 4);
        }
        db->Set(data_key, data_value);
    }
    return nullptr;
}

static void* get_pure(void *id) {
    Random rnd;
    mt19937 mt(23333);
    double u = KEY_POOL_TOP / 2.0;
    double o = KEY_POOL_TOP * 0.01;
    int edge = (int) (KEY_POOL_TOP * 0.0196);
    normal_distribution<double> n(u, o);
    string value;
    char *ans;
    int cnt = PER_GET;
    while(cnt --) {
        int key_idx = ((int)n(mt) | 1) ^1;
        key_idx %= KEY_POOL_TOP - 2;
        int val_idx = (key_idx + (8 * (key_idx / 2)));
        if(key_idx - u > edge || u - key_idx > edge) {
            // 写
            unsigned int* start = rnd.nextUnsignedInt();
            Slice data_key((char*)(key_pool + key_idx), 16);
            Slice data_value((char*)start, 80);
            /* Put new value of key(key_idx) */
            memcpy(val_pool + val_idx, start, VALUE_SIZE);
            db->Set(data_key, data_value);
        } else {
            // 读
            Slice data_key((char*)(key_pool + key_idx), 16);
            db->Get(data_key, &value);

            /* Consistency check */
            if(strncmp(&value[0], (ans = (char*)(val_pool + val_idx)), VALUE_SIZE) != 0 ) {
                std::cout << "Check result failed. " << endl;
//                std::cout << "Key: " << data_key.data() << endl;
//                std::cout << "Result: " << value << endl;
//                std::cout << "Answer: " << ans << endl;
//                EXIT("Check result failed.", ERR_CHECKFAILED);
                exit(1);
            }
        }
    }
    return nullptr;
}

/**
 * Configuration input args
 */
static void config_parse(int argc, char *argv[]) {
    int opt = 0;
    while((opt = getopt(argc, argv, "hs:g:")) != -1) {
        switch(opt) {
            case 'h':
                printf("Usage: ./judge -s <set-size-per-Thread> -g <get-size-per-Thread>\n");
                return ;
            case 'm':
                MODE = atoi(optarg);
                break;
            case 's':
                PER_SET = atoi(optarg);
                break;
            case 'g':
                PER_GET = atoi(optarg);
                break;
            default:
                break;
        }
    }
}

/**
 * Start N thread to test DB::Set
 */
static void test_set_pure(pthread_t * tids) {
    for(int i = 0; i < NUM_THREADS; ++i) {
        if(pthread_create(&tids[i], nullptr, set_pure, seed + i) != 0) {
            printf("create thread failed.\n");
            exit(1);
        }
    }
    for(int i = 0; i < NUM_THREADS; i++){
        pthread_join(tids[i], nullptr);
    }
}


/**
 * Start N thread to test DB::Set & DB::Get
 */
static void test_set_get(pthread_t * tids) {
    for(int i = 0; i < NUM_THREADS; ++i) {
        if(pthread_create(&tids[i], nullptr, get_pure, seed + i) != 0) {
            printf("create thread failed.\n");
            exit(1);
        }
    }
    for(int i = 0; i < NUM_THREADS; i++){
        pthread_join(tids[i], nullptr);
    }
}

int main(int argc, char *argv[]) {

    printf("start judge...\n");

    config_parse(argc, argv);
    init_pool_seed();
    FILE * log_file =  fopen("./performance.log", "w");
    pthread_t tids[NUM_THREADS];

    gettimeofday(&TIME_START,nullptr);
    DB::CreateOrOpen("./DB", &db, log_file);
    test_set_pure(tids);    /* Test Set */
    gettimeofday(&TIME_END,nullptr);
    uint64_t sec_set = 1000000 * (TIME_END.tv_sec-TIME_START.tv_sec)+ (TIME_END.tv_usec-TIME_START.tv_usec);
    test_set_get(tids);     /* Test Set & Get */
    gettimeofday(&TIME_END,nullptr);

    uint64_t sec_total = 1000000 * (TIME_END.tv_sec-TIME_START.tv_sec)+ (TIME_END.tv_usec-TIME_START.tv_usec);
    uint64_t sec_set_get = sec_total - sec_set;
    printf("Set: %.2lfms\n"
           "Get & Set: %.2lfms\n", sec_set/1000.0, sec_set_get/1000.0);

    delete db;  /* Release */
    return 0;
}

/* END */
