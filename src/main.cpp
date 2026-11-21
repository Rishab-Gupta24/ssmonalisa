// Shopping Sessions pipeline: one static C++ binary.
// Reads $DATA_DIR, writes $OUTPUT_DIR/user_daily and $OUTPUT_DIR/campaign_daily as Parquet.
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <vector>

#include <dirent.h>
#include <fcntl.h>
#include <immintrin.h>
#include <sys/mman.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <unistd.h>

typedef uint64_t u64;
typedef int64_t i64;
typedef uint32_t u32;
typedef int32_t i32;
typedef uint16_t u16;
typedef uint8_t u8;

static const i64 SEC = 1000, DAYMS = 86400000LL, WEEK = 7 * DAYMS;
static const i64 TS_BAD = INT64_MIN;
static const u32 TS_INVALID = 0xFFFFFFFFu;

static int NT = 8;

static void die(const char* m) {
    fprintf(stderr, "fatal: %s\n", m);
    _exit(1);
}

static double T0 = 0;
static bool TIMING = false;
static double now_s() {
    return std::chrono::duration<double>(std::chrono::steady_clock::now().time_since_epoch()).count();
}
// user-space instruction counter for this process and threads created later (debug only)
static int PERF_FD = -1;
static void perf_init() {
    struct {
        u32 type, size;
        u64 config, sample_period, sample_type, read_format, flags;
        u32 wakeup, bp_type;
        u64 bp_addr, bp_len, branch_sample_type, sample_regs_user;
        u32 sample_stack_user;
        i32 clockid;
        u64 sample_regs_intr;
        u32 aux_watermark;
        u16 sample_max_stack, reserved;
        u32 aux_sample_size, reserved3;
        u64 sig_data, config3;
    } a;
    memset(&a, 0, sizeof a);
    a.type = 0;    // PERF_TYPE_HARDWARE
    a.size = 128;  // PERF_ATTR_SIZE_VER7 or so; kernel accepts smaller known sizes
    a.config = 1;  // PERF_COUNT_HW_INSTRUCTIONS
    // flags: disabled=0, inherit=bit1, exclude_kernel=bit5, exclude_hv=bit6
    a.flags = (1ULL << 1) | (1ULL << 5) | (1ULL << 6);
    PERF_FD = (int)syscall(298 /* perf_event_open */, &a, 0, -1, -1, 0);
}
static u64 perf_read() {
    u64 v = 0;
    if (PERF_FD >= 0 && read(PERF_FD, &v, 8) != 8) v = 0;
    return v;
}
static void mark(const char* what) {
    if (!TIMING) return;
    struct rusage ru;
    getrusage(RUSAGE_SELF, &ru);
    double us = ru.ru_utime.tv_sec + ru.ru_utime.tv_usec / 1e6, sy = ru.ru_stime.tv_sec + ru.ru_stime.tv_usec / 1e6;
    fprintf(stderr, "%8.1f ms  user %7.1f  sys %7.1f  Minstr %8.1f  %s\n", (now_s() - T0) * 1000, us * 1000,
            sy * 1000, perf_read() / 1e6, what);
}

// ---------------------------------------------------------------- threads
template <class F>
static void parallel(int n, F f) {
    std::vector<std::thread> th;
    th.reserve(n);
    for (int t = 0; t < n; t++) th.emplace_back(f, t);
    for (auto& x : th) x.join();
}

// dynamic task loop: f(t, task)
template <class F>
static void run_tasks(size_t ntasks, F f) {
    std::atomic<size_t> next{0};
    parallel(NT, [&](int t) {
        size_t i;
        while ((i = next.fetch_add(1, std::memory_order_relaxed)) < ntasks) f(t, i);
    });
}

// ---------------------------------------------------------------- arena + block lists
struct Arena {
    char* base = nullptr;
    size_t cap = 0;
    std::atomic<size_t> off{0};
    static const size_t HUGE = 2 << 20, SLAB = 4 << 20;
    void init(size_t c) {
        c = (c + HUGE - 1) & ~(HUGE - 1);
        void* p = mmap(nullptr, c + HUGE, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE, -1, 0);
        if (p == MAP_FAILED) die("arena mmap");
        base = (char*)(((uintptr_t)p + HUGE - 1) & ~(uintptr_t)(HUGE - 1));
        cap = c;
        madvise(base, c, MADV_HUGEPAGE);
    }
    // raw, globally shared bump allocation (fresh memory, zero-filled)
    void* raw(size_t n, bool populate) {
        n = (n + HUGE - 1) & ~(HUGE - 1);  // keeps every offset 2 MB aligned
        size_t o = off.fetch_add(n, std::memory_order_relaxed);
        if (o + n > cap) die("arena exhausted");
        if (populate) madvise(base + o, n, 23 /* MADV_POPULATE_WRITE */);
        return base + o;
    }
    // per-thread slabs, prefaulted in one syscall each
    void* get(size_t n) {
        static thread_local char *sp = nullptr, *se = nullptr;
        n = (n + 63) & ~(size_t)63;
        if (n > SLAB / 4) return raw(n, true);
        if (!sp || sp + n > se) {
            sp = (char*)raw(SLAB, true);
            se = sp + SLAB;
        }
        void* r = sp;
        sp += n;
        return r;
    }
};
static Arena ARENA;

template <class T>
struct BList {
    struct Blk {
        Blk* next;
        u32 n, cap;
        T* items() { return (T*)(this + 1); }
    };
    Blk* head = nullptr;
    Blk* cur = nullptr;
    size_t done = 0;  // items in full blocks before cur
    inline void push(const T& v, u32 bcap) {
        if (__builtin_expect(!cur || cur->n == cur->cap, 0)) grow(bcap);
        cur->items()[cur->n++] = v;
    }
    void grow(u32 bcap) {
        Blk* b = (Blk*)ARENA.get(sizeof(Blk) + (size_t)bcap * sizeof(T));
        b->next = nullptr;
        b->n = 0;
        b->cap = bcap;
        if (cur) {
            done += cur->n;
            cur->next = b;
        } else
            head = b;
        cur = b;
    }
    size_t size() const { return done + (cur ? cur->n : 0); }
    template <class F>
    void each(F f) const {
        for (Blk* b = head; b; b = b->next) {
            T* it = b->items();
            for (u32 i = 0; i < b->n; i++) f(it[i]);
        }
    }
};

// ---------------------------------------------------------------- files
struct InFile {
    int fd = -1;
    size_t n = 0;
    const char* map = nullptr;  // whole-file read-only mapping (zero-copy path)
};

static std::vector<std::string> list_csv(const std::string& dir) {
    std::vector<std::string> out;
    DIR* d = opendir(dir.c_str());
    if (!d) return out;
    while (dirent* e = readdir(d)) {
        std::string name = e->d_name;
        if (name.empty() || name[0] == '.') continue;
        if (name.size() < 4 || name.compare(name.size() - 4, 4, ".csv") != 0) continue;
        std::string full = dir + "/" + name;
        struct stat st;
        if (stat(full.c_str(), &st) != 0 || !S_ISREG(st.st_mode)) continue;
        out.push_back(full);
    }
    closedir(d);
    std::sort(out.begin(), out.end());
    return out;
}

static InFile open_in(const std::string& path) {
    InFile f;
    f.fd = open(path.c_str(), O_RDONLY);
    if (f.fd < 0) return f;
    struct stat st;
    if (fstat(f.fd, &st) == 0) f.n = st.st_size;
    if (f.n > (1u << 20)) {
        void* p = mmap(nullptr, f.n, PROT_READ, MAP_PRIVATE, f.fd, 0);
        if (p != MAP_FAILED) f.map = (const char*)p;
    }
    return f;
}

static void pread_all(int fd, char* dst, size_t n, size_t off) {
    while (n) {
        ssize_t r = pread(fd, dst, n, off);
        if (r <= 0) die("read failed");
        dst += r;
        n -= r;
        off += r;
    }
}

static std::string read_small(const std::string& path) {
    InFile f = open_in(path);
    std::string s(f.n, '\0');
    if (f.n) pread_all(f.fd, s.data(), f.n, 0);
    if (f.fd >= 0) close(f.fd);
    return s;
}

// ---------------------------------------------------------------- time
static inline bool dig(char c) { return (unsigned)(c - '0') < 10; }
static inline int d2(const char* s) { return (s[0] - '0') * 10 + (s[1] - '0'); }

static inline i64 days_from_civil(i64 y, i64 m, i64 d) {
    y -= m <= 2;
    i64 era = (y >= 0 ? y : y - 399) / 400;
    i64 yoe = y - era * 400;
    i64 doy = (153 * (m + (m > 2 ? -3 : 9)) + 2) / 5 + d - 1;
    i64 doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    return era * 146097 + doe - 719468;
}

static inline bool leap(i64 y) { return (y % 4 == 0 && y % 100 != 0) || y % 400 == 0; }

// Parses F1 / F2 / F3; returns epoch ms or TS_BAD.
static inline i64 parse_ts(const char* s, size_t n) {
    if (n == 0) return TS_BAD;
    if ((n == 20 || n == 25) && s[4] == '-') {
        if (!(dig(s[0]) && dig(s[1]) && dig(s[2]) && dig(s[3]) && dig(s[5]) && dig(s[6]) && s[7] == '-' &&
              dig(s[8]) && dig(s[9]) && s[10] == 'T' && dig(s[11]) && dig(s[12]) && s[13] == ':' &&
              dig(s[14]) && dig(s[15]) && s[16] == ':' && dig(s[17]) && dig(s[18])))
            return TS_BAD;
        i64 y = d2(s) * 100 + d2(s + 2);
        int mo = d2(s + 5), d = d2(s + 8), h = d2(s + 11), mi = d2(s + 14), se = d2(s + 17);
        if (mo < 1 || mo > 12 || d < 1 || h > 23 || mi > 59 || se > 59) return TS_BAD;
        static const int dim[13] = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
        int maxd = dim[mo] + (mo == 2 && leap(y));
        if (d > maxd) return TS_BAD;
        i64 off = 0;
        if (n == 20) {
            if (s[19] != 'Z') return TS_BAD;
        } else {
            char sg = s[19];
            if (sg != '+' && sg != '-') return TS_BAD;
            if (!(dig(s[20]) && dig(s[21]) && s[22] == ':' && dig(s[23]) && dig(s[24]))) return TS_BAD;
            int oh = d2(s + 20), om = d2(s + 23);
            if (oh > 23 || om > 59) return TS_BAD;
            off = (i64)(oh * 60 + om) * 60000;
            if (sg == '-') off = -off;
        }
        i64 days = days_from_civil(y, mo, d);
        return ((days * 24 + h) * 60 + mi) * 60000 + se * SEC - off;
    }
    // F3: digits only
    for (size_t k = 0; k < n; k++)
        if (!dig(s[k])) return TS_BAD;
    size_t i = 0;
    while (i < n && s[i] == '0') i++;
    if (n - i > 18) return TS_BAD;  // far outside any window
    i64 v = 0;
    for (; i < n; i++) v = v * 10 + (s[i] - '0');
    return v;
}

// Dates near the window, keyed by month*32+day: valid calendar dates only.
static i32 DT_YEAR[13 * 32];
static i64 DT_DAYS[13 * 32];
static void build_date_table(i64 firstDay, i64 lastDay) {
    for (int i = 0; i < 13 * 32; i++) DT_YEAR[i] = -1;
    for (i64 d = firstDay; d <= lastDay; d++) {
        // civil_from_days (Howard Hinnant)
        i64 z = d + 719468;
        i64 era = (z >= 0 ? z : z - 146096) / 146097;
        i64 doe = z - era * 146097;
        i64 yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
        i64 y = yoe + era * 400;
        i64 doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
        i64 mp = (5 * doy + 2) / 153;
        i64 dd = doy - (153 * mp + 2) / 5 + 1;
        i64 m = mp < 10 ? mp + 3 : mp - 9;
        y += m <= 2;
        DT_YEAR[m * 32 + dd] = (i32)y;
        DT_DAYS[m * 32 + dd] = d;
    }
}

static const u64 DIG_A = 0x00FFFF00FFFFFFFFULL;  // "YYYY-MM-": digit bytes
static const u64 DIG_B = 0xFFFF00FFFF00FFFFULL;  // "DDTHH:MM"
static inline bool swar_digits(u64 x, u64 m) {
    const u64 H = 0xF0F0F0F0F0F0F0F0ULL, T = 0x3030303030303030ULL, S = 0x0606060606060606ULL;
    x &= m;
    return (x & H & m) == (T & m) && (((x + (S & m)) & H & m) == (T & m));
}

// Same result as parse_ts, faster for F1/F2 dates near the window.
static inline i64 parse_ts_fast(const char* s, size_t n) {
    if (n == 20 || n == 25) {
        u64 a, b;
        memcpy(&a, s, 8);
        memcpy(&b, s + 8, 8);
        const u64 SEPA = ((u64)'-' << 32) | ((u64)'-' << 56), SEPB = ((u64)'T' << 16) | ((u64)':' << 40);
        if ((a & ~DIG_A) == SEPA && (b & ~DIG_B) == SEPB && swar_digits(a, DIG_A) && swar_digits(b, DIG_B) &&
            s[16] == ':' && dig(s[17]) && dig(s[18])) {
            // all date/time digit pairs at once: byte i of t = 10*digit_i + digit_{i+1}
            const u64 Z = 0x3030303030303030ULL;
            u64 va = (a - (Z & DIG_A)) & DIG_A, vb = (b - (Z & DIG_B)) & DIG_B;
            u64 ta = va * 10 + (va >> 8), tb = vb * 10 + (vb >> 8);
            int mo = (int)((ta >> 40) & 0xFF), d = (int)(tb & 0xFF);
            int idx = mo * 32 + d;
            i32 y = (i32)(ta & 0xFF) * 100 + (i32)((ta >> 16) & 0xFF);
            if (mo <= 12 && d <= 31 && DT_YEAR[idx] == y) {
                int h = (int)((tb >> 24) & 0xFF), mi = (int)((tb >> 48) & 0xFF), se = d2(s + 17);
                if (h > 23 || mi > 59 || se > 59) return TS_BAD;
                i64 off = 0;
                if (n == 20) {
                    if (s[19] != 'Z') return TS_BAD;
                } else {
                    char sg = s[19];
                    if ((sg != '+' && sg != '-') || !dig(s[20]) || !dig(s[21]) || s[22] != ':' || !dig(s[23]) ||
                        !dig(s[24]))
                        return TS_BAD;
                    int oh = d2(s + 20), om = d2(s + 23);
                    if (oh > 23 || om > 59) return TS_BAD;
                    off = (i64)(oh * 60 + om) * 60000;
                    if (sg == '-') off = -off;
                }
                return ((DT_DAYS[idx] * 24 + h) * 60 + mi) * 60000 + se * SEC - off;
            }
        }
    } else if (n >= 10 && n <= 16) {
        // F3 fast path: last 8 digits + first n-8 digits (left-padded with '0'), SWAR-parsed
        const u64 Z = 0x3030303030303030ULL;
        u64 lo, hi;
        memcpy(&lo, s + n - 8, 8);
        memcpy(&hi, s, 8);
        unsigned pad = (unsigned)(16 - n) * 8;  // 0..48 bits of leading '0' bytes
        hi = (hi << pad) | (Z & ((1ULL << pad) - 1));
        if (!swar_digits(lo, ~0ULL) || !swar_digits(hi, ~0ULL)) return TS_BAD;
        auto p8 = [](u64 x) -> u64 {
            x -= 0x3030303030303030ULL;
            x = ((x * 10) + (x >> 8)) & 0x00FF00FF00FF00FFULL;
            x = ((x * 100) + (x >> 16)) & 0x0000FFFF0000FFFFULL;
            x = ((x * 10000) + (x >> 32)) & 0x00000000FFFFFFFFULL;
            return x;
        };
        return (i64)(p8(hi) * 100000000ULL + p8(lo));
    }
    return parse_ts(s, n);
}

// ---------------------------------------------------------------- keys
// Canonical strings ([a-z] then up to 8 of [0-9_a-z]) pack into 53 bits, order-preserving.
// Everything else becomes a 63-bit hash with the top bit set and is kept in a registry.
static u8 CH[256];
static const u64 NONCANON = 1ULL << 63;

static void init_ch() {
    memset(CH, 0, sizeof CH);
    for (int c = '0'; c <= '9'; c++) CH[c] = 1 + (c - '0');
    CH[(u8)'_'] = 11;
    for (int c = 'a'; c <= 'z'; c++) CH[c] = 12 + (c - 'a');
}
static const char DECH[] = "?0123456789_abcdefghijklmnopqrstuvwxyz";

static inline u64 mix64(u64 k) {
    k ^= k >> 33;
    k *= 0xff51afd7ed558ccdULL;
    k ^= k >> 33;
    k *= 0xc4ceb9fe1a85ec53ULL;
    k ^= k >> 33;
    return k;
}

static inline u64 hash_bytes(const char* s, size_t n) {
    u64 h = 0x9E3779B97F4A7C15ULL ^ (n * 0xff51afd7ed558ccdULL);
    for (size_t i = 0; i < n; i++) {
        h ^= (u8)s[i];
        h *= 0x100000001b3ULL;
        h ^= h >> 29;
    }
    return mix64(h);
}

// SWAR + pext version of enc_key_ref. Reads up to 8 bytes past s+1 (buffers are padded).
static inline u64 enc_key(const char* s, size_t n, bool& canon) {
    if (n >= 1 && n <= 9 && (unsigned)(s[0] - 'a') < 26) {
        u64 x;
        memcpy(&x, s + 1, 8);
        size_t r = n - 1;
        u64 keep = r == 8 ? ~0ULL : ((1ULL << (8 * r)) - 1);
        x &= keep;
        const u64 H = 0x8080808080808080ULL, L = 0x0101010101010101ULL;
        if (!(x & H)) {
            u64 xh = x | H;
            auto ge = [&](u64 c) { return (xh - L * c) & H; };  // high bit set where byte >= c
            u64 isd = ge('0') & ~ge('9' + 1);
            u64 isl = ge('a') & ~ge('z' + 1);
            u64 isu = ge('_') & ~ge('_' + 1);
            if (!(keep & H & ~(isd | isl | isu))) {
                u64 dm = (isd >> 7) * 0xFF, lm = (isl >> 7) * 0xFF, um = (isu >> 7) * 0xFF;
                u64 code = (((xh - L * 0x2F) & (L * 0x7F)) & dm) | (((xh - L * 0x55) & (L * 0x7F)) & lm) |
                           ((L * 11) & um);
                canon = true;
                return ((u64)(s[0] - 'a' + 1) << 48) | _pext_u64(__builtin_bswap64(code), L * 0x3F);
            }
        }
    }
    canon = false;
    return hash_bytes(s, n) | NONCANON;
}

static inline u64 enc_key_ref(const char* s, size_t n, bool& canon) {
    if (n >= 1 && n <= 9 && (unsigned)(s[0] - 'a') < 26) {
        u64 k = (u64)(s[0] - 'a' + 1) << 48;
        int sh = 42;
        size_t i = 1;
        for (; i < n; i++) {
            u8 c = CH[(u8)s[i]];
            if (!c) break;
            k |= (u64)c << sh;
            sh -= 6;
        }
        if (i == n) {
            canon = true;
            return k;
        }
    }
    canon = false;
    return hash_bytes(s, n) | NONCANON;
}

static std::unordered_map<u64, std::string_view> REG;  // noncanonical key -> string

static void key_append(u64 k, std::string& out) {
    if (k & NONCANON) {
        auto it = REG.find(k);
        if (it == REG.end()) die("unregistered key");
        out.append(it->second.data(), it->second.size());
        return;
    }
    out.push_back((char)('a' + ((k >> 48) & 31) - 1));
    for (int sh = 42; sh >= 0; sh -= 6) {
        int c = (k >> sh) & 63;
        if (!c) break;
        out.push_back(DECH[c]);
    }
}
static std::string key_str(u64 k) {
    std::string s;
    key_append(k, s);
    return s;
}

// byte-wise compare of the strings behind two keys
static inline bool key_less(u64 a, u64 b) {
    if (!((a | b) & NONCANON)) return a < b;
    if (a == b) return false;
    return key_str(a) < key_str(b);
}

// ---------------------------------------------------------------- hash map u64 -> u64 (key 0 = empty)
struct HMap {
    struct KV {
        u64 k, v;
    };
    std::vector<KV> s;
    u64 mask = 0;
    int shift = 64;
    size_t cap() const { return s.size(); }
    void init(size_t n) {
        size_t c = 16;
        while (c < n * 2) c <<= 1;
        if (s.size() == c)
            memset((void*)s.data(), 0, c * sizeof(KV));
        else
            s.assign(c, KV{0, 0});
        mask = c - 1;
        shift = 64 - __builtin_ctzll(c);
    }
    inline size_t slot(u64 key) const { return (size_t)((key * 0x9E3779B97F4A7C15ULL) >> shift); }
    inline u64* upsert(u64 key, u64 def, bool& inserted) {
        size_t i = slot(key);
        while (true) {
            KV& e = s[i];
            if (e.k == key) {
                inserted = false;
                return &e.v;
            }
            if (e.k == 0) {
                e.k = key;
                e.v = def;
                inserted = true;
                return &e.v;
            }
            i = (i + 1) & mask;
        }
    }
    inline const u64* find(u64 key) const {
        if (s.empty()) return nullptr;
        size_t i = slot(key);
        while (true) {
            const KV& e = s[i];
            if (e.k == key) return &e.v;
            if (e.k == 0) return nullptr;
            i = (i + 1) & mask;
        }
    }
};

// ---------------------------------------------------------------- region-partitioned table (links)
// 256 regions, each a contiguous run of slots; probing wraps inside the region, so regions
// can be filled in parallel. Region of a key = top 8 bits of mix64(key).
struct Slot {
    u64 k, v;
};
struct RTable {
    Slot* s = nullptr;
    int bits = 16;
    size_t rmask = 0;
    void init(size_t n) {
        bits = 16;
        while (((size_t)1 << bits) < n) bits++;
        size_t size = (size_t)1 << bits;
        s = (Slot*)ARENA.get(size * sizeof(Slot));  // arena memory is fresh, hence zero
        rmask = (size >> 8) - 1;
    }
    inline size_t start(u64 key) const { return (size_t)(mix64(key) >> (64 - bits)); }
    inline size_t next(size_t i) const { return (i & ~rmask) | ((i + 1) & rmask); }
    inline const Slot* find(u64 key) const {
        size_t i = start(key);
        while (true) {
            if (s[i].k == key) return &s[i];
            if (!s[i].k) return nullptr;
            i = next(i);
        }
    }
    inline void prefetch(u64 key) const { __builtin_prefetch(&s[start(key)]); }
};
static inline int region_of(u64 key) { return (int)(mix64(key) >> 56); }

// ---------------------------------------------------------------- CSV line scanning
// Structural bitmasks for [b, b+N): bit i of word i/64 marks byte i. sep = ',' or '\n'.
// A virtual newline is set at N. Requires 64 readable bytes past b+N.
struct Masks {
    std::vector<u64> nl, sep, q;
};
static void build_masks(const char* b, size_t N, Masks& M) {
    size_t W = (N >> 6) + 1;
    if (M.nl.size() < W + 1) {
        M.nl.resize(W + 64);
        M.sep.resize(W + 64);
        M.q.resize(W + 64);
    }
    const __m256i NL = _mm256_set1_epi8('\n'), CM = _mm256_set1_epi8(','), QT = _mm256_set1_epi8('"');
    u64* nl = M.nl.data();
    u64* sep = M.sep.data();
    u64* q = M.q.data();
    for (size_t w = 0; w < W; w++) {
        __m256i v0 = _mm256_loadu_si256((const __m256i*)(b + (w << 6)));
        __m256i v1 = _mm256_loadu_si256((const __m256i*)(b + (w << 6) + 32));
        u64 n = (u32)_mm256_movemask_epi8(_mm256_cmpeq_epi8(v0, NL)) |
                ((u64)(u32)_mm256_movemask_epi8(_mm256_cmpeq_epi8(v1, NL)) << 32);
        u64 c = (u32)_mm256_movemask_epi8(_mm256_cmpeq_epi8(v0, CM)) |
                ((u64)(u32)_mm256_movemask_epi8(_mm256_cmpeq_epi8(v1, CM)) << 32);
        u64 qq = (u32)_mm256_movemask_epi8(_mm256_cmpeq_epi8(v0, QT)) |
                 ((u64)(u32)_mm256_movemask_epi8(_mm256_cmpeq_epi8(v1, QT)) << 32);
        nl[w] = n;
        sep[w] = n | c;
        q[w] = qq;
    }
    u64 bit = 1ULL << (N & 63);
    nl[N >> 6] |= bit;
    sep[N >> 6] |= bit;
    nl[W] = sep[W] = ~0ULL;  // guard
    q[W] = 0;
}

static inline u32 range_pop(const u64* q, size_t a, size_t b) {  // popcount of bits [a, b)
    if (a >= b) return 0;
    size_t wa = a >> 6, wb = (b - 1) >> 6;
    u64 lo = ~0ULL << (a & 63), hi = ~0ULL >> (63 - ((b - 1) & 63));
    if (wa == wb) return (u32)__builtin_popcountll(q[wa] & lo & hi);
    u32 c = (u32)__builtin_popcountll(q[wa] & lo) + (u32)__builtin_popcountll(q[wb] & hi);
    for (size_t w = wa + 1; w < wb; w++) c += (u32)__builtin_popcountll(q[w]);
    return c;
}
static inline bool bit_at(const u64* m, size_t i) { return (m[i >> 6] >> (i & 63)) & 1; }

// Calls fn(fp, fl) for every well-formed row (rules.md dialect) in [b, lim).
static std::atomic<u64> DBG_LINES[8], DBG_BYTES[8];
template <int NF, class F>
static inline void for_rows(const char* b, const char* lim, F fn) {
    static thread_local Masks M;
    size_t N = lim - b;
    if (!N) return;
    if (TIMING) DBG_BYTES[NF].fetch_add(N, std::memory_order_relaxed);
    build_masks(b, N, M);
    const u64* nl = M.nl.data();
    const u64* sep = M.sep.data();
    const u64* q = M.q.data();
    const char* fp[NF];
    u32 fl[NF];
    size_t ends[NF];
    size_t w = 0;
    u64 m = sep[0];
    size_t pos = 0;
    while (pos <= N) {
        size_t ls = pos, lineEnd;
        int k = 0;
        while (true) {  // walk separator bits in order
            while (!m) m = sep[++w];
            u64 bit = m & (0 - m);
            size_t idx = (w << 6) + (size_t)__builtin_ctzll(m);
            m ^= bit;
            if (nl[w] & bit) {
                lineEnd = idx;
                break;
            }
            if (k < NF) ends[k] = idx;
            k++;
        }
        pos = lineEnd + 1;
        if (TIMING) DBG_LINES[NF].fetch_add(1, std::memory_order_relaxed);
        if (lineEnd == N && ls == N) break;  // nothing after the last newline
        if (k != NF - 1) continue;           // wrong field count (an empty line has one field)
        ends[NF - 1] = lineEnd;
        u32 qc = range_pop(q, ls, lineEnd);
        size_t s = ls;
        if (!qc) {
            for (int f = 0; f < NF; f++) {
                fp[f] = b + s;
                fl[f] = (u32)(ends[f] - s);
                s = ends[f] + 1;
            }
            fn(fp, fl);
            continue;
        }
        u32 expect = 0;
        bool ok = true;
        for (int f = 0; f < NF; f++) {
            size_t e = ends[f];
            if (e > s && bit_at(q, s)) {
                if (e - s < 2 || !bit_at(q, e - 1)) {
                    ok = false;
                    break;
                }
                expect += 2;
                fp[f] = b + s + 1;
                fl[f] = (u32)(e - s - 2);
            } else {
                fp[f] = b + s;
                fl[f] = (u32)(e - s);
            }
            s = e + 1;
        }
        if (ok && qc == expect) fn(fp, fl);
    }
}

// small clean files (products, campaigns)
template <int NF, class F>
static void for_rows_text(const std::string& t, F fn) {
    std::string buf = t;
    buf.append(64, '\0');
    const char* b = buf.data();
    const char* lim = b + t.size();
    const char* nl = (const char*)memchr(b, '\n', t.size());
    if (!nl) return;
    for_rows<NF>(nl + 1, lim, fn);
}

struct Chunk {
    int file;
    int kind;  // 0 events, 1 clicks, 2 links
    size_t s, e;
};

// Loads the lines of file f whose first byte lies in [s,e) into buf; sets [lb, le).
static bool load_chunk(const InFile& f, size_t s, size_t e, std::vector<char>& buf, const char*& lb,
                       const char*& le) {
    const size_t EXTRA = 1 << 16;
    size_t rs = s == 0 ? 0 : s - 1;
    if (f.map && e + EXTRA < f.n) {
        // zero-copy: every line we touch, plus 64 bytes of SIMD over-read, lies inside the file
        const char* b = f.map;
        const char* nl0 = (const char*)memchr(b + rs, '\n', f.n - rs);
        if (!nl0) return false;
        const char* start = nl0 + 1;
        if ((size_t)(start - b) >= e) return false;
        const char* endnl = (const char*)memchr(b + e - 1, '\n', f.n - (e - 1));
        if (endnl && (size_t)(endnl - b) + 64 < f.n) {
            lb = start;
            le = endnl;
            return true;
        }
    }
    size_t re = std::min(f.n, e + EXTRA);
    while (true) {
        size_t need = re - rs + 64;
        if (buf.size() < need) buf.resize(need + (need >> 3));
        pread_all(f.fd, buf.data(), re - rs, rs);
        memset(buf.data() + (re - rs), 0, 64);
        const char* b = buf.data();
        const char* nl0 = (const char*)memchr(b, '\n', re - rs);
        if (!nl0) {
            if (re == f.n) return false;
            re = std::min(f.n, re + (re - rs));
            continue;
        }
        const char* start = nl0 + 1;  // s==0: skip header; else first line starting at >= s
        if ((size_t)(start - b) + rs >= e) return false;
        // last owned line ends at the first '\n' at file position >= e-1
        size_t from = (e - 1) - rs;
        const char* endnl = (const char*)memchr(b + from, '\n', (re - rs) - from);
        if (!endnl && re < f.n) {
            re = std::min(f.n, re + std::max(EXTRA, re - rs));
            continue;
        }
        lb = start;
        le = endnl ? endnl : b + (re - rs);
        return true;
    }
}

// ---------------------------------------------------------------- records
struct ER {  // event row as parsed (for dedup)
    u64 eid;
    u64 ukey;
    u32 ts;      // relative to BASE, or TS_INVALID
    i32 ing;     // ingest seconds relative to window start
    u32 packed;  // purchase: price | cat << 24 ; else 0
    u32 pad;
};
struct CK {  // valid click, before person resolution
    u64 ukey;
    u64 cid;
    u32 ts;
    u32 camp;
};
struct EV {  // surviving event
    u64 pkey;
    u32 ts;
    u32 packed;
};
struct CR {  // valid click
    u64 pkey;
    u64 cid;
    u32 ts;
    u32 camp;
};
struct PK {
    u64 a, b;
};
struct C2 {
    u32 ts;
    u32 camp;
    u64 cid;
};

// ---------------------------------------------------------------- parquet writer
struct TW {
    std::string b;
    std::vector<int> st;
    int last = 0;
    void varint(u64 v) {
        while (v >= 0x80) {
            b.push_back((char)(v | 0x80));
            v >>= 7;
        }
        b.push_back((char)v);
    }
    static u64 zz(i64 v) { return ((u64)v << 1) ^ (u64)(v >> 63); }
    void fh(int id, int t) {
        int d = id - last;
        if (d > 0 && d <= 15)
            b.push_back((char)((d << 4) | t));
        else {
            b.push_back((char)t);
            varint(zz(id));
        }
        last = id;
    }
    void i32f(int id, i64 v) {
        fh(id, 5);
        varint(zz(v));
    }
    void i64f(int id, i64 v) {
        fh(id, 6);
        varint(zz(v));
    }
    void binf(int id, const std::string& s) {
        fh(id, 8);
        varint(s.size());
        b += s;
    }
    void sbeg(int id) {
        fh(id, 12);
        st.push_back(last);
        last = 0;
    }
    void ebeg() {
        st.push_back(last);
        last = 0;
    }
    void send() {
        b.push_back(0);
        last = st.back();
        st.pop_back();
    }
    void lbeg(int id, int et, size_t n) {
        fh(id, 9);
        if (n < 15)
            b.push_back((char)((n << 4) | et));
        else {
            b.push_back((char)(0xF0 | et));
            varint(n);
        }
    }
};

enum PType { P_INT32 = 1, P_INT64 = 2, P_BYTES = 6 };
struct PCol {
    std::string name;
    int type = 0;
    bool optional = false;
    int logical = 0;      // 0 none, 1 string, 6 date
    std::string data;     // plain-encoded non-null values
    std::vector<u8> def;  // definition levels (optional only)
};

static void put_u32(std::string& s, u32 v) { s.append((const char*)&v, 4); }

static std::string rle_levels(const std::vector<u8>& d) {
    TW t;
    size_t i = 0;
    while (i < d.size()) {
        size_t j = i;
        while (j < d.size() && d[j] == d[i]) j++;
        t.varint((u64)(j - i) << 1);
        t.b.push_back((char)d[i]);
        i = j;
    }
    std::string out;
    put_u32(out, (u32)t.b.size());
    out += t.b;
    return out;
}

static void write_parquet(const std::string& path, std::vector<PCol>& cols, size_t nrows) {
    int fd = open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) die("cannot write parquet");
    std::string head = "PAR1";
    i64 pos = 0;
    struct Meta {
        i64 off, size;
    };
    std::vector<Meta> metas;
    std::vector<std::string> parts;  // header/levels pieces, written in order with data
    auto wr = [&](const char* p, size_t n) {
        while (n) {
            ssize_t r = write(fd, p, n);
            if (r <= 0) die("write failed");
            p += r;
            n -= r;
        }
        pos += 0;
    };
    wr(head.data(), 4);
    pos = 4;
    for (auto& c : cols) {
        std::string lv;
        if (c.optional) lv = rle_levels(c.def);
        size_t psize = lv.size() + c.data.size();
        TW ph;
        ph.i32f(1, 0);
        ph.i32f(2, (i64)psize);
        ph.i32f(3, (i64)psize);
        ph.sbeg(5);
        ph.i32f(1, (i64)nrows);
        ph.i32f(2, 0);
        ph.i32f(3, 3);
        ph.i32f(4, 3);
        ph.send();
        ph.b.push_back(0);
        i64 off = pos;
        wr(ph.b.data(), ph.b.size());
        if (!lv.empty()) wr(lv.data(), lv.size());
        wr(c.data.data(), c.data.size());
        pos += ph.b.size() + psize;
        metas.push_back({off, (i64)(ph.b.size() + psize)});
    }
    TW m;
    m.i32f(1, 1);
    m.lbeg(2, 12, cols.size() + 1);
    m.ebeg();
    m.binf(4, "schema");
    m.i32f(5, (i64)cols.size());
    m.send();
    for (auto& c : cols) {
        m.ebeg();
        m.i32f(1, c.type);
        m.i32f(3, c.optional ? 1 : 0);
        m.binf(4, c.name);
        if (c.logical == 1) m.i32f(6, 0);
        if (c.logical == 6) m.i32f(6, 6);
        if (c.logical) {
            m.sbeg(10);
            m.sbeg(c.logical);
            m.send();
            m.send();
        }
        m.send();
    }
    m.i64f(3, (i64)nrows);
    m.lbeg(4, 12, 1);
    m.ebeg();
    m.lbeg(1, 12, cols.size());
    i64 total = 0;
    for (size_t i = 0; i < cols.size(); i++) {
        auto& c = cols[i];
        m.ebeg();
        m.i64f(2, metas[i].off);
        m.sbeg(3);
        m.i32f(1, c.type);
        m.lbeg(2, 5, 2);
        m.varint(TW::zz(0));
        m.varint(TW::zz(3));
        m.lbeg(3, 8, 1);
        m.varint(c.name.size());
        m.b += c.name;
        m.i32f(4, 0);
        m.i64f(5, (i64)nrows);
        m.i64f(6, metas[i].size);
        m.i64f(7, metas[i].size);
        m.i64f(9, metas[i].off);
        m.send();
        m.send();
        total += metas[i].size;
    }
    m.i64f(2, total);
    m.i64f(3, (i64)nrows);
    m.send();
    m.binf(6, "ssmonalisa");
    m.b.push_back(0);
    put_u32(m.b, (u32)(m.b.size()));
    m.b += "PAR1";
    // footer length must exclude the 4-byte length and magic: fix up
    {
        u32 flen = (u32)(m.b.size() - 8);
        memcpy(&m.b[m.b.size() - 8], &flen, 4);
    }
    wr(m.b.data(), m.b.size());
    close(fd);
}

static inline void col_i64(PCol& c, i64 v) { c.data.append((const char*)&v, 8); }
static inline void col_i32(PCol& c, i32 v) { c.data.append((const char*)&v, 4); }
static inline void col_str(PCol& c, const std::string& s) {
    put_u32(c.data, (u32)s.size());
    c.data += s;
}
static inline void col_key(PCol& c, u64 k) {
    size_t at = c.data.size();
    c.data.append(4, '\0');
    key_append(k, c.data);
    u32 n = (u32)(c.data.size() - at - 4);
    memcpy(&c.data[at], &n, 4);
}

// ---------------------------------------------------------------- sorting helpers
static void radix_sort_hi32(u64* a, size_t n, std::vector<u64>& tmp) {
    // sorts by the high 32 bits (3 passes of 11 bits)
    tmp.resize(n);
    u64* src = a;
    u64* dst = tmp.data();
    for (int pass = 0; pass < 3; pass++) {
        int sh = 32 + pass * 11;
        size_t cnt[2048] = {0};
        for (size_t i = 0; i < n; i++) cnt[(src[i] >> sh) & 2047]++;
        size_t sum = 0;
        for (int b = 0; b < 2048; b++) {
            size_t c = cnt[b];
            cnt[b] = sum;
            sum += c;
        }
        for (size_t i = 0; i < n; i++) dst[cnt[(src[i] >> sh) & 2047]++] = src[i];
        std::swap(src, dst);
    }
    if (src != a) memcpy(a, src, n * sizeof(u64));
}

static inline void sort_events(u64* a, size_t n, std::vector<u64>& tmp) {
    if (n <= 24) {
        for (size_t i = 1; i < n; i++) {
            u64 x = a[i];
            size_t j = i;
            while (j > 0 && (a[j - 1] >> 32) > (x >> 32)) {
                a[j] = a[j - 1];
                j--;
            }
            a[j] = x;
        }
    } else if (n < 20000) {
        std::sort(a, a + n, [](u64 x, u64 y) { return (x >> 32) < (y >> 32); });
    } else {
        radix_sort_hi32(a, n, tmp);
    }
}

// ---------------------------------------------------------------- self-test (SS_SELFTEST=1)
// Fuzzes the fast parsers against the reference ones.
static int selftest(i64 WS) {
    u64 rng = 0x12345678abcdefULL;
    auto rnd = [&]() {
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        return rng;
    };
    std::vector<char> buf(64 + 64, 0);
    const char* alpha = "abcdefghijklmnopqrstuvwxyz0123456789_ ABCZ\r\"-:.\x80\xff\t";
    size_t na = strlen(alpha);
    long bad = 0;
    for (long it = 0; it < 4000000; it++) {
        size_t n = rnd() % 12;
        for (size_t i = 0; i < 64; i++) buf[i] = alpha[rnd() % na];  // garbage after the string too
        if (rnd() % 2) buf[0] = 'a' + rnd() % 26;
        if (rnd() % 2)
            for (size_t i = 1; i < n; i++) buf[i] = "0123456789_abcdefghijklmnopqrstuvwxyz"[rnd() % 37];
        bool c1, c2;
        u64 k1 = enc_key(buf.data(), n, c1), k2 = enc_key_ref(buf.data(), n, c2);
        if (k1 != k2 || c1 != c2) {
            if (bad++ < 5) fprintf(stderr, "enc_key mismatch n=%zu '%.*s'\n", n, (int)n, buf.data());
        }
    }
    char tmp[64];
    for (long it = 0; it < 4000000; it++) {
        i64 t = WS - 9 * DAYMS + (i64)(rnd() % (u64)(30 * DAYMS));
        int form = rnd() % 3;
        std::string s;
        if (form == 2) {
            snprintf(tmp, sizeof tmp, "%0*lld", (int)(13 + rnd() % 4), (long long)t);
            s = tmp;
        } else {
            i64 tt = t - t % 1000;
            int off = form == 1 ? (int)(rnd() % 1500) - 750 : 0;
            i64 local = tt + (i64)off * 60000;
            i64 days = local / DAYMS, rem = local % DAYMS;
            i64 z = days + 719468, era = z / 146097, doe = z - era * 146097;
            i64 yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365, y = yoe + era * 400;
            i64 doy = doe - (365 * yoe + yoe / 4 - yoe / 100), mp = (5 * doy + 2) / 153;
            i64 d = doy - (153 * mp + 2) / 5 + 1, m = mp < 10 ? mp + 3 : mp - 9;
            y += m <= 2;
            snprintf(tmp, sizeof tmp, "%04lld-%02lld-%02lldT%02lld:%02lld:%02lld", (long long)y, (long long)m,
                     (long long)d, (long long)(rem / 3600000), (long long)(rem / 60000 % 60),
                     (long long)(rem / 1000 % 60));
            s = tmp;
            if (form == 0)
                s += 'Z';
            else {
                int ao = off < 0 ? -off : off;
                snprintf(tmp, sizeof tmp, "%c%02d:%02d", off < 0 ? '-' : '+', ao / 60, ao % 60);
                s += tmp;
            }
        }
        int muts = rnd() % 4 == 0 ? 1 + rnd() % 2 : 0;
        for (int k = 0; k < muts && !s.empty(); k++) {
            size_t p = rnd() % s.size();
            int kind = rnd() % 4;
            if (kind == 0) s[p] = "0123456789Zz+-:T t.9"[rnd() % 20];
            else if (kind == 1) s.erase(p, 1);
            else if (kind == 2) s.insert(p, 1, "09:-"[rnd() % 4]);
            else s[p] = (char)('0' + rnd() % 10);
        }
        memset(buf.data(), 'x', buf.size());
        memcpy(buf.data() + 32, s.data(), s.size());
        i64 a = parse_ts_fast(buf.data() + 32, s.size()), b = parse_ts(buf.data() + 32, s.size());
        if (a != b) {
            if (bad++ < 10) fprintf(stderr, "ts mismatch '%s' fast=%lld ref=%lld\n", s.c_str(), (long long)a, (long long)b);
        }
    }
    fprintf(stderr, "selftest: %ld mismatches\n", bad);
    return bad ? 1 : 0;
}

// ---------------------------------------------------------------- main
int main() {
    T0 = now_s();
    TIMING = getenv("SS_TIMING") != nullptr;
    if (TIMING) perf_init();
    init_ch();
    const char* ev;
    std::string DATA = (ev = getenv("DATA_DIR")) ? ev : "/data";
    std::string OUT = (ev = getenv("OUTPUT_DIR")) ? ev : "/output";
    NT = (ev = getenv("NCPU")) ? atoi(ev) : (int)std::thread::hardware_concurrency();
    if (NT < 1) NT = 1;
    if (NT > 64) NT = 64;

    // ---- manifest
    i64 WS, WE;
    {
        std::string man = read_small(DATA + "/manifest.json");
        auto get = [&](const char* key) -> i64 {
            size_t p = man.find(key);
            if (p == std::string::npos) die("manifest key");
            p = man.find(':', p);
            p = man.find('"', p);
            size_t q = man.find('"', p + 1);
            i64 t = parse_ts(man.data() + p + 1, q - p - 1);
            if (t == TS_BAD) die("manifest ts");
            return t;
        };
        WS = get("\"window_start\"");
        WE = get("\"window_end\"");
    }
    const i64 BASE = WS - WEEK;  // relative timestamps
    auto floordiv = [](i64 a, i64 b) { return a >= 0 ? a / b : -((-a + b - 1) / b); };
    const i64 DAY0 = floordiv(WS, DAYMS);
    const int NDAYS = (int)(floordiv(WE - 1, DAYMS) - DAY0 + 1);
    build_date_table(DAY0 - 40, DAY0 + NDAYS + 40);
    if (getenv("SS_SELFTEST")) _exit(selftest(WS));

    // ---- input files
    auto evNames = list_csv(DATA + "/events");
    auto ckNames = list_csv(DATA + "/clicks");
    auto lkNames = list_csv(DATA + "/links");
    std::vector<InFile> files;
    std::vector<int> kindOf;
    for (auto& n : evNames) files.push_back(open_in(n)), kindOf.push_back(0);
    for (auto& n : ckNames) files.push_back(open_in(n)), kindOf.push_back(1);
    for (auto& n : lkNames) files.push_back(open_in(n)), kindOf.push_back(2);
    size_t totalBytes = 0;
    for (auto& f : files) totalBytes += f.n;
    ARENA.init(totalBytes * 2 + ((size_t)1 << 30));

    // ---- products / categories
    std::vector<std::string> cats;
    HMap prodMap;
    {
        std::string prodText = read_small(DATA + "/products.csv");
        std::unordered_map<std::string_view, int> catIdx;
        std::vector<std::string_view> catViews;
        prodMap.init(std::max<size_t>(1024, prodText.size() / 8));
        std::string buf = prodText;
        buf.append(64, '\0');
        const char* nl = (const char*)memchr(buf.data(), '\n', prodText.size());
        if (nl)
            for_rows<2>(nl + 1, buf.data() + prodText.size(), [&](const char** fp, const u32* fl) {
                if (!fl[0]) return;
                std::string_view c(fp[1], fl[1]);
                auto it = catIdx.find(c);
                int ci;
                if (it == catIdx.end()) {
                    ci = (int)cats.size();
                    catIdx.emplace(c, ci);
                    cats.emplace_back(c);
                } else
                    ci = it->second;
                bool cn;
                u64 k = enc_key(fp[0], fl[0], cn);
                bool ins;
                *prodMap.upsert(k, 0, ins) = ci;
            });
    }
    const int UNKNOWN_CAT = (int)cats.size();
    cats.push_back("unknown");
    if (cats.size() > 255) die("too many categories");
    std::vector<int> catRank(cats.size());
    {
        std::vector<int> ord(cats.size());
        for (size_t i = 0; i < ord.size(); i++) ord[i] = (int)i;
        std::sort(ord.begin(), ord.end(), [&](int a, int b) { return cats[a] < cats[b]; });
        for (size_t r = 0; r < ord.size(); r++) catRank[ord[r]] = (int)r;
    }

    // ---- campaigns
    std::vector<std::string> campIds, campCh;
    HMap campMap;
    {
        std::string t = read_small(DATA + "/campaigns.csv");
        campMap.init(4096);
        for_rows_text<2>(t, [&](const char** fp, const u32* fl) {
            if (!fl[0]) return;
            bool cn;
            u64 k = enc_key(fp[0], fl[0], cn);
            bool ins;
            campMap.upsert(k, campIds.size(), ins);
            if (ins) {
                campIds.emplace_back(fp[0], fl[0]);
                campCh.emplace_back(fp[1], fl[1]);
            }
        });
    }
    const int NCAMP = (int)campIds.size();
    const int ORGANIC = NCAMP;
    mark("metadata");

    // ---- parse links + events + clicks in one parallel pass
    const int P = 512;   // event dedup partitions
    const int K = 1024;  // person shards
    auto ppart = [](u64 k) { return (int)(mix64(k) & (P - 1)); };
    auto pshard = [](u64 k) { return (int)((mix64(k) >> 20) & (K - 1)); };

    std::vector<std::vector<BList<u64>>> lkKeys(NT, std::vector<BList<u64>>(256));
    std::vector<BList<PK>> lkPairs(NT);
    std::vector<std::vector<BList<ER>>> evParts(NT, std::vector<BList<ER>>(P));
    std::vector<BList<CK>> ckRaw(NT);
    std::vector<std::vector<std::pair<u64, std::string_view>>> regs(NT);
    {
        std::vector<Chunk> chunks;
        size_t target = std::max<size_t>(1 << 20, std::min<size_t>(8 << 20, totalBytes / (NT * 12) + 1));
        for (size_t f = 0; f < files.size(); f++) {
            size_t n = files[f].n;
            for (size_t s = 0; s < n; s += target) chunks.push_back({(int)f, kindOf[f], s, std::min(n, s + target)});
        }
        std::stable_sort(chunks.begin(), chunks.end(), [](const Chunk& a, const Chunk& b) {
            if ((a.kind == 2) != (b.kind == 2)) return a.kind == 2;
            return (a.e - a.s) > (b.e - b.s);
        });
        const i64 WSs = WS / SEC;
        run_tasks(chunks.size(), [&](int t, size_t ci) {
            static thread_local std::vector<char> buf;
            const Chunk& c = chunks[ci];
            const char *lb, *le;
            if (!load_chunk(files[c.file], c.s, c.e, buf, lb, le)) return;
            auto& rg = regs[t];
            auto save = [&](u64 k, const char* p, u32 n) {
                char* d = (char*)ARENA.get(n + 1);
                memcpy(d, p, n);
                rg.push_back({k, std::string_view(d, n)});
            };
            if (c.kind == 2) {
                auto& keys = lkKeys[t];
                auto& pairs = lkPairs[t];
                for_rows<2>(lb, le, [&](const char** fp, const u32* fl) {
                    if (!fl[0] || !fl[1]) return;
                    bool ca, cb;
                    u64 ka = enc_key(fp[0], fl[0], ca), kb = enc_key(fp[1], fl[1], cb);
                    if (!ca) save(ka, fp[0], fl[0]);
                    if (!cb) save(kb, fp[1], fl[1]);
                    if (ka == kb) return;
                    keys[region_of(ka)].push(ka, 512);
                    keys[region_of(kb)].push(kb, 512);
                    pairs.push({ka, kb}, 2048);
                });
            } else if (c.kind == 0) {
                auto& parts = evParts[t];
                for_rows<7>(lb, le, [&](const char** fp, const u32* fl) {
                    if (!fl[0]) return;
                    ER r;
                    bool cn;
                    r.eid = enc_key(fp[0], fl[0], cn);
                    r.pad = 0;
                    i64 ing = parse_ts_fast(fp[3], fl[3]);
                    i64 ingS = ing == TS_BAD ? INT32_MAX : ing / SEC - WSs;
                    if (ingS > INT32_MAX) ingS = INT32_MAX;
                    if (ingS < INT32_MIN) ingS = INT32_MIN;
                    r.ing = (i32)ingS;
                    r.ts = TS_INVALID;
                    r.ukey = 0;
                    r.packed = 0;
                    do {
                        if (!fl[1] || !fl[5]) break;
                        i64 ts = parse_ts_fast(fp[2], fl[2]);
                        if (ts == TS_BAD || ts < WS || ts >= WE) break;
                        if (ing == TS_BAD || ing - ts > DAYMS) break;
                        const char* ty = fp[4];
                        u32 tl = fl[4];
                        bool purchase = false;
                        if (tl == 4 && (memcmp(ty, "view", 4) == 0 || memcmp(ty, "cart", 4) == 0)) {
                        } else if (tl == 8 && memcmp(ty, "purchase", 8) == 0) {
                            purchase = true;
                        } else
                            break;
                        u32 packed = 0;
                        if (purchase) {
                            const char* pr = fp[6];
                            u32 pl = fl[6];
                            if (!pl) break;
                            bool ok = true;
                            for (u32 q = 0; q < pl; q++)
                                if (!dig(pr[q])) {
                                    ok = false;
                                    break;
                                }
                            if (!ok) break;
                            u32 j = 0;
                            while (j < pl && pr[j] == '0') j++;
                            if (pl - j > 8) break;
                            u32 price = 0;
                            for (; j < pl; j++) price = price * 10 + (pr[j] - '0');
                            if (price < 1 || price > 10000000) break;
                            u64 pk = enc_key(fp[5], fl[5], cn);
                            const u64* cv = prodMap.find(pk);
                            u32 cat = cv ? (u32)*cv : (u32)UNKNOWN_CAT;
                            packed = price | (cat << 24);
                        }
                        r.ukey = enc_key(fp[1], fl[1], cn);
                        if (!cn) save(r.ukey, fp[1], fl[1]);
                        r.ts = (u32)(ts - BASE);
                        r.packed = packed;
                    } while (0);
                    parts[ppart(r.eid)].push(r, 256);
                });
            } else {
                auto& out = ckRaw[t];
                for_rows<4>(lb, le, [&](const char** fp, const u32* fl) {
                    if (!fl[0] || !fl[1] || !fl[2]) return;
                    i64 ts = parse_ts_fast(fp[3], fl[3]);
                    if (ts == TS_BAD || ts < WS - WEEK || ts >= WE) return;
                    bool cn;
                    u64 ck = enc_key(fp[2], fl[2], cn);
                    const u64* cv = campMap.find(ck);
                    if (!cv) return;
                    if (!cn && campIds[*cv] != std::string_view(fp[2], fl[2])) return;
                    u64 uk = enc_key(fp[1], fl[1], cn);
                    if (!cn) save(uk, fp[1], fl[1]);
                    u64 cid = enc_key(fp[0], fl[0], cn);
                    if (!cn) save(cid, fp[0], fl[0]);
                    out.push({uk, cid, (u32)(ts - BASE), (u32)*cv}, 4096);
                });
            }
        });
        for (auto& r : regs) {
            for (auto& kv : r) REG.emplace(kv.first, kv.second);
            r.clear();
        }
    }
    mark("parse");
    if (TIMING)
        for (int f : {2, 4, 7}) fprintf(stderr, "  for_rows<%d>: %llu lines, %llu MB\n", f, (unsigned long long)DBG_LINES[f].load(), (unsigned long long)(DBG_BYTES[f].load() >> 20));

    // ---- link table + union-find
    RTable LT;
    {
        size_t upper = 0;
        for (int t = 0; t < NT; t++)
            for (int r = 0; r < 256; r++) upper += lkKeys[t][r].size();
        LT.init(std::max<size_t>(upper * 3 / 2, 1 << 16));
        std::vector<size_t> rcount(257, 0);
        size_t rsize = LT.rmask + 1;
        run_tasks(256, [&](int, size_t r) {
            size_t local = 0;
            for (int t = 0; t < NT; t++)
                lkKeys[t][r].each([&](u64 k) {
                    size_t i = LT.start(k);
                    while (true) {
                        if (LT.s[i].k == k) break;
                        if (!LT.s[i].k) {
                            LT.s[i].k = k;
                            LT.s[i].v = local++;
                            break;
                        }
                        i = LT.next(i);
                    }
                });
            if (local >= rsize) die("link region overflow");
            rcount[r] = local;
        });
        std::vector<size_t> rbase(257, 0);
        for (int r = 0; r < 256; r++) rbase[r + 1] = rbase[r] + rcount[r];
        size_t n = rbase[256];
        std::vector<u64> keyOf(n);
        run_tasks(256, [&](int, size_t r) {
            Slot* s = LT.s + r * rsize;
            for (size_t i = 0; i < rsize; i++)
                if (s[i].k) keyOf[rbase[r] + s[i].v] = s[i].k;
        });
        std::vector<std::atomic<u32>> parent(n);
        for (size_t i = 0; i < n; i++) parent[i].store((u32)i, std::memory_order_relaxed);
        auto find = [&](u32 x) {
            while (true) {
                u32 p = parent[x].load(std::memory_order_relaxed);
                if (p == x) return x;
                u32 gp = parent[p].load(std::memory_order_relaxed);
                if (gp != p) parent[x].store(gp, std::memory_order_relaxed);  // path halving (benign)
                x = gp;
            }
        };
        auto gidx = [&](u64 k) -> u32 {
            const Slot* sl = LT.find(k);
            return (u32)(rbase[region_of(k)] + sl->v);
        };
        run_tasks(NT, [&](int, size_t t) {
            lkPairs[t].each([&](const PK& pr) {
                u32 a = gidx(pr.a), b = gidx(pr.b);
                while (true) {
                    a = find(a);
                    b = find(b);
                    if (a == b) break;
                    if (a < b) std::swap(a, b);  // link larger root under smaller
                    u32 exp = a;
                    if (parent[a].compare_exchange_strong(exp, b, std::memory_order_relaxed)) break;
                }
            });
        });
        std::vector<u32> root(n);
        run_tasks(NT, [&](int, size_t t) {
            size_t lo = n * t / NT, hi = n * (t + 1) / NT;
            for (size_t i = lo; i < hi; i++) root[i] = find((u32)i);
        });
        std::vector<u64> minKey(n, 0);
        for (size_t i = 0; i < n; i++) {
            u32 r = root[i];
            if (minKey[r] == 0 || key_less(keyOf[i], minKey[r])) minKey[r] = keyOf[i];
        }
        run_tasks(256, [&](int, size_t r) {
            Slot* s = LT.s + r * rsize;
            for (size_t i = 0; i < rsize; i++)
                if (s[i].k) s[i].v = minKey[root[rbase[r] + s[i].v]];
        });
    }
    mark("links");

    // ---- dedup events + resolve persons + scatter to person shards
    std::vector<std::vector<BList<EV>>> evShards(NT, std::vector<BList<EV>>(K));
    std::vector<std::vector<BList<CR>>> ckShards(NT, std::vector<BList<CR>>(K));
    {
        std::vector<typename BList<CK>::Blk*> ckBlocks;
        for (int t = 0; t < NT; t++)
            for (auto* b = ckRaw[t].head; b; b = b->next) ckBlocks.push_back(b);
        const int PD = 16;
        run_tasks(P + ckBlocks.size(), [&](int t, size_t task) {
            static thread_local HMap m;
            static thread_local std::vector<const ER*> buf, surv;
            if (task < (size_t)P) {
                int p = (int)task;
                size_t n = 0;
                for (int s = 0; s < NT; s++) n += evParts[s][p].size();
                m.init(n + 16);
                buf.clear();
                buf.reserve(n);
                for (int s = 0; s < NT; s++)
                    evParts[s][p].each([&](const ER& r) {
                        bool ins;
                        u64* v = m.upsert(r.eid, buf.size(), ins);
                        if (ins)
                            buf.push_back(&r);
                        else if (r.ing < buf[*v]->ing)
                            buf[*v] = &r;
                    });
                surv.clear();
                for (const ER* r : buf)
                    if (r->ts != TS_INVALID) surv.push_back(r);
                auto& out = evShards[t];
                size_t ns = surv.size();
                for (size_t i = 0; i < ns; i++) {
                    if (i + PD < ns) LT.prefetch(surv[i + PD]->ukey);
                    const ER& r = *surv[i];
                    const Slot* sl = LT.find(r.ukey);
                    u64 pk = sl ? sl->v : r.ukey;
                    out[pshard(pk)].push({pk, r.ts, r.packed}, 256);
                }
            } else {
                auto* b = ckBlocks[task - P];
                CK* it = b->items();
                u32 nb = b->n;
                auto& out = ckShards[t];
                for (u32 i = 0; i < nb; i++) {
                    if (i + PD < nb) LT.prefetch(it[i + PD].ukey);
                    const Slot* sl = LT.find(it[i].ukey);
                    u64 pk = sl ? sl->v : it[i].ukey;
                    out[pshard(pk)].push({pk, it[i].cid, it[i].ts, it[i].camp}, 170);
                }
            }
        });
    }
    mark("dedup+resolve");

    // ---- sessionize + attribute per shard
    const size_t CAMPW = (size_t)(NCAMP + 1) * NDAYS;
    std::vector<std::vector<u64>> camp(NT);
    std::vector<std::vector<PCol>> ucols(NT);
    std::vector<size_t> urowsN(NT, 0);
    {
        const u32 W = (u32)WEEK, GAP = 1800000;
        const u32 DAYOFF = (u32)(WS - BASE);
        const i64 BASE_DAY_SHIFT = WS - DAY0 * DAYMS;
        for (int t = 0; t < NT; t++) {
            camp[t].assign(CAMPW * 4, 0);
            auto& cols = ucols[t];
            cols.resize(6);
            cols[0].name = "user_id", cols[0].type = P_BYTES, cols[0].logical = 1;
            cols[1].name = "day", cols[1].type = P_INT32, cols[1].logical = 6;
            cols[2].name = "sessions", cols[2].type = P_INT64;
            cols[3].name = "events", cols[3].type = P_INT64;
            cols[4].name = "revenue_cents", cols[4].type = P_INT64;
            cols[5].name = "top_category", cols[5].type = P_BYTES, cols[5].optional = true, cols[5].logical = 1;
        }
        run_tasks(K, [&](int t, size_t s) {
            static thread_local std::vector<u64> pkeys, E2, tmp;
            static thread_local std::vector<u32> evIdx, ckIdx, eoff, coff;
            static thread_local std::vector<C2> Cv;
            static thread_local std::vector<u32> cnt, since, cstart, wp2;
            static thread_local std::vector<u64> catSum;
            struct Sess {
                u32 start;
                int day;
                bool purchase;
                u64 rev;
            };
            static thread_local std::vector<Sess> S;
            static thread_local HMap pm;
            if (cnt.size() != (size_t)NCAMP + 1) {
                cnt.assign(NCAMP + 1, 0);
                since.assign(NCAMP + 1, 0);
            }
            if (catSum.size() != cats.size()) catSum.assign(cats.size(), 0);
            auto& cols = ucols[t];
            u64* cs = camp[t].data();
            u64* cp = cs + CAMPW;
            u64* cr = cs + 2 * CAMPW;
            u64* ca = cs + 3 * CAMPW;

            size_t ne = 0, nc = 0;
            for (int q = 0; q < NT; q++) ne += evShards[q][s].size(), nc += ckShards[q][s].size();
            if (!ne) return;
            pm.init(ne / 16 + 256);
            pkeys.clear();
            evIdx.resize(ne);
            eoff.clear();
            size_t at = 0;
            for (int q = 0; q < NT; q++)
                evShards[q][s].each([&](const EV& e) {
                    bool ins;
                    u64* v = pm.upsert(e.pkey, pkeys.size(), ins);
                    u32 pi = (u32)*v;
                    if (ins) {
                        pkeys.push_back(e.pkey);
                        eoff.push_back(0);
                        if (pkeys.size() * 2 > pm.cap()) {  // grow: rehash all persons
                            pm.init(pkeys.size() * 2);
                            for (size_t i = 0; i < pkeys.size(); i++) {
                                bool in2;
                                *pm.upsert(pkeys[i], i, in2) = i;
                            }
                        }
                    }
                    evIdx[at++] = pi;
                    eoff[pi]++;
                });
            size_t np = pkeys.size();
            // prefix sums (eoff becomes start offsets, size np+1)
            eoff.push_back(0);
            {
                u32 sum = 0;
                for (size_t i = 0; i <= np; i++) {
                    u32 c = eoff[i];
                    eoff[i] = sum;
                    sum += c;
                }
            }
            E2.resize(ne);
            {
                std::vector<u32>& wp = coff;  // reuse as write positions
                wp.assign(eoff.begin(), eoff.end());
                at = 0;
                for (int q = 0; q < NT; q++)
                    evShards[q][s].each([&](const EV& e) {
                        u32 pi = evIdx[at++];
                        E2[wp[pi]++] = ((u64)e.ts << 32) | e.packed;
                    });
            }
            // clicks of these persons
            cstart.assign(np + 1, 0);
            ckIdx.resize(nc);
            at = 0;
            for (int q = 0; q < NT; q++)
                ckShards[q][s].each([&](const CR& c) {
                    const u64* v = pm.find(c.pkey);
                    u32 pi = v ? (u32)*v : 0xFFFFFFFFu;
                    ckIdx[at++] = pi;
                    if (v) cstart[pi]++;
                });
            {
                u32 sum = 0;
                for (size_t i = 0; i <= np; i++) {
                    u32 c = cstart[i];
                    cstart[i] = sum;
                    sum += c;
                }
            }
            Cv.resize(cstart[np]);
            {
                std::vector<u32>& wp = wp2;
                wp.assign(cstart.begin(), cstart.end());
                at = 0;
                for (int q = 0; q < NT; q++)
                    ckShards[q][s].each([&](const CR& c) {
                        u32 pi = ckIdx[at++];
                        if (pi != 0xFFFFFFFFu) Cv[wp[pi]++] = {c.ts, c.camp, c.cid};
                    });
            }

            for (size_t pi = 0; pi < np; pi++) {
                u64 pk = pkeys[pi];
                u64* Ep = E2.data() + eoff[pi];
                size_t en = eoff[pi + 1] - eoff[pi];
                sort_events(Ep, en, tmp);
                C2* Cp = Cv.data() + cstart[pi];
                size_t cn = cstart[pi + 1] - cstart[pi];
                if (cn > 1)
                    std::sort(Cp, Cp + cn, [](const C2& a, const C2& b) {
                        if (a.ts != b.ts) return a.ts < b.ts;
                        return key_less(a.cid, b.cid);
                    });
                // sessions + user-day rows
                S.clear();
                int curDay = -1;
                u64 dSess = 0, dEv = 0, dRev = 0;
                bool anyCat = false;
                auto flush_day = [&]() {
                    if (curDay < 0) return;
                    col_key(cols[0], pk);
                    col_i32(cols[1], (i32)(DAY0 + curDay));
                    col_i64(cols[2], (i64)dSess);
                    col_i64(cols[3], (i64)dEv);
                    col_i64(cols[4], (i64)dRev);
                    if (anyCat) {
                        int best = -1;
                        for (size_t c = 0; c < catSum.size(); c++) {
                            if (!catSum[c]) continue;
                            if (best < 0 || catSum[c] > catSum[best] ||
                                (catSum[c] == catSum[best] && catRank[c] < catRank[best]))
                                best = (int)c;
                        }
                        col_str(cols[5], cats[best]);
                        cols[5].def.push_back(1);
                        std::fill(catSum.begin(), catSum.end(), 0);
                    } else
                        cols[5].def.push_back(0);
                    urowsN[t]++;
                };
                size_t k = 0;
                while (k < en) {
                    u32 st = (u32)(Ep[k] >> 32);
                    int day = (int)(((i64)(st - DAYOFF) + BASE_DAY_SHIFT) / DAYMS);
                    if (day != curDay) {
                        flush_day();
                        curDay = day;
                        dSess = dEv = dRev = 0;
                        anyCat = false;
                    }
                    Sess se{st, day, false, 0};
                    u32 prev = st;
                    u64 nev = 0;
                    while (k < en) {
                        u32 ts = (u32)(Ep[k] >> 32);
                        if (ts - prev > GAP) break;
                        prev = ts;
                        u32 pkd = (u32)Ep[k];
                        if (pkd) {
                            u32 price = pkd & 0xFFFFFF;
                            se.purchase = true;
                            se.rev += price;
                            catSum[pkd >> 24] += price;
                            anyCat = true;
                        }
                        nev++;
                        k++;
                    }
                    dSess++;
                    dEv += nev;
                    dRev += se.rev;
                    S.push_back(se);
                }
                flush_day();
                // attribution
                size_t lo = 0, hi = 0;
                u32 nS = (u32)S.size();
                auto close = [&](u32 c, u32 a, u32 b) {
                    for (u32 x = a; x < b; x++) ca[(size_t)c * NDAYS + S[x].day]++;
                };
                for (u32 si = 0; si < nS; si++) {
                    u32 st = S[si].start;
                    while (hi < cn && Cp[hi].ts <= st) {
                        u32 c = Cp[hi].camp;
                        if (cnt[c]++ == 0) since[c] = si;
                        hi++;
                    }
                    while (lo < hi && Cp[lo].ts + W < st) {
                        u32 c = Cp[lo].camp;
                        if (--cnt[c] == 0) close(c, since[c], si);
                        lo++;
                    }
                    u32 cc = ORGANIC;
                    if (lo < hi) {
                        size_t q = hi - 1;
                        while (q > lo && Cp[q - 1].ts == Cp[q].ts) q--;
                        cc = Cp[q].camp;
                    }
                    size_t o = (size_t)cc * NDAYS + S[si].day;
                    cs[o]++;
                    if (S[si].purchase) cp[o]++;
                    cr[o] += S[si].rev;
                }
                for (size_t q = lo; q < hi; q++) {
                    u32 c = Cp[q].camp;
                    if (cnt[c]) {
                        close(c, since[c], nS);
                        cnt[c] = 0;
                    }
                }
            }
        });
    }
    mark("sessionize");

    // ---- write outputs
    std::string udir = OUT + "/user_daily", cdir = OUT + "/campaign_daily";
    mkdir(OUT.c_str(), 0777);
    mkdir(udir.c_str(), 0777);
    mkdir(cdir.c_str(), 0777);
    size_t totalRows = 0;
    for (auto n : urowsN) totalRows += n;
    std::vector<std::thread> writers;
    for (int t = 0; t < NT; t++) {
        if (!urowsN[t] && !(t == 0 && totalRows == 0)) continue;
        writers.emplace_back([&, t]() {
            write_parquet(udir + "/part-" + std::to_string(t) + ".parquet", ucols[t], urowsN[t]);
        });
    }
    {
        std::vector<PCol> cols(7);
        cols[0].name = "campaign_id", cols[0].type = P_BYTES, cols[0].logical = 1;
        cols[1].name = "channel", cols[1].type = P_BYTES, cols[1].logical = 1;
        cols[2].name = "day", cols[2].type = P_INT32, cols[2].logical = 6;
        cols[3].name = "sessions", cols[3].type = P_INT64;
        cols[4].name = "purchasing_sessions", cols[4].type = P_INT64;
        cols[5].name = "revenue_cents", cols[5].type = P_INT64;
        cols[6].name = "assisted_sessions", cols[6].type = P_INT64;
        size_t n = 0;
        for (int c = 0; c <= NCAMP; c++)
            for (int d = 0; d < NDAYS; d++) {
                size_t o = (size_t)c * NDAYS + d;
                u64 v[4] = {0, 0, 0, 0};
                for (int t = 0; t < NT; t++)
                    for (int q = 0; q < 4; q++) v[q] += camp[t][q * CAMPW + o];
                if (!v[0] && !v[3]) continue;
                col_str(cols[0], c == ORGANIC ? std::string("organic") : campIds[c]);
                col_str(cols[1], c == ORGANIC ? std::string("organic") : campCh[c]);
                col_i32(cols[2], (i32)(DAY0 + d));
                col_i64(cols[3], (i64)v[0]);
                col_i64(cols[4], (i64)v[1]);
                col_i64(cols[5], (i64)v[2]);
                col_i64(cols[6], c == ORGANIC ? 0 : (i64)v[3]);
                n++;
            }
        write_parquet(cdir + "/part-0.parquet", cols, n);
    }
    for (auto& w : writers) w.join();
    mark("done");
    if (TIMING) {
        std::string sm = read_small("/proc/self/smaps_rollup");
        size_t p = sm.find("AnonHugePages");
        if (p != std::string::npos) fprintf(stderr, "%s\n", sm.substr(p, sm.find('\n', p) - p).c_str());
        fprintf(stderr, "arena used %zu MB\n", ARENA.off.load() >> 20);
    }
    _exit(0);
}
