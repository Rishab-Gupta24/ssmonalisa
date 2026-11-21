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
static void mark(const char* what) {
    if (!TIMING) return;
    struct rusage ru;
    getrusage(RUSAGE_SELF, &ru);
    double cpu = ru.ru_utime.tv_sec + ru.ru_utime.tv_usec / 1e6 + ru.ru_stime.tv_sec + ru.ru_stime.tv_usec / 1e6;
    fprintf(stderr, "%8.1f ms  cpu %7.1f ms  %s\n", (now_s() - T0) * 1000, cpu * 1000, what);
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
    void init(size_t c) {
        cap = c;
        void* p = mmap(nullptr, c, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE, -1, 0);
        if (p == MAP_FAILED) die("arena mmap");
        base = (char*)p;
        madvise(base, c, MADV_HUGEPAGE);
    }
    void* get(size_t n) {
        n = (n + 63) & ~(size_t)63;
        size_t o = off.fetch_add(n, std::memory_order_relaxed);
        if (o + n > cap) die("arena exhausted");
        return base + o;
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

static inline u64 enc_key(const char* s, size_t n, bool& canon) {
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
    std::vector<u64> k, v;
    u64 mask = 0;
    int shift = 64;
    void init(size_t n) {
        size_t cap = 16;
        while (cap < n * 2) cap <<= 1;
        if (k.size() == cap) {
            std::fill(k.begin(), k.end(), 0);
        } else {
            k.assign(cap, 0);
            v.assign(cap, 0);
        }
        mask = cap - 1;
        shift = 64 - __builtin_ctzll(cap);
    }
    inline size_t slot(u64 key) const { return (size_t)((key * 0x9E3779B97F4A7C15ULL) >> shift); }
    inline u64* upsert(u64 key, u64 def, bool& inserted) {
        size_t i = slot(key);
        while (true) {
            if (k[i] == key) {
                inserted = false;
                return &v[i];
            }
            if (k[i] == 0) {
                k[i] = key;
                v[i] = def;
                inserted = true;
                return &v[i];
            }
            i = (i + 1) & mask;
        }
    }
    inline const u64* find(u64 key) const {
        if (k.empty()) return nullptr;
        size_t i = slot(key);
        while (true) {
            if (k[i] == key) return &v[i];
            if (k[i] == 0) return nullptr;
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
// Buffers always have >= 64 readable bytes past their logical end.
struct Line {
    const char* p;
    u32 len;
    u32 nc;          // number of commas
    u32 commas[16];  // comma offsets (first min(nc,16))
    u32 qw[8];       // quote bit mask words (lines up to 256 bytes)
    u32 anyq;
};

// Scans one line starting at p (ends at '\n' or lim). Returns false if longer than 256 bytes
// (then only len is valid).
static inline bool scan_line(const char* p, const char* lim, Line& L) {
    const __m256i NL = _mm256_set1_epi8('\n'), CM = _mm256_set1_epi8(','), QT = _mm256_set1_epi8('"');
    L.p = p;
    L.nc = 0;
    L.anyq = 0;
    u32 off = 0;
    while (true) {
        __m256i v = _mm256_loadu_si256((const __m256i*)(p + off));
        u32 nl = (u32)_mm256_movemask_epi8(_mm256_cmpeq_epi8(v, NL));
        u32 cm = (u32)_mm256_movemask_epi8(_mm256_cmpeq_epi8(v, CM));
        u32 q = (u32)_mm256_movemask_epi8(_mm256_cmpeq_epi8(v, QT));
        if (p + off + 32 > lim) {
            u32 rem = (u32)(lim - (p + off));
            u32 m = rem >= 32 ? 0xFFFFFFFFu : ((1u << rem) - 1);
            nl = (nl & m) | (rem < 32 ? (1u << rem) : 0);
        }
        u32 keep = 0xFFFFFFFFu;
        if (nl) {
            u32 k = __builtin_ctz(nl);
            keep = k == 32 ? 0xFFFFFFFFu : ((1u << k) - 1);
            cm &= keep;
            q &= keep;
            if (off < 256) {
                L.qw[off >> 5] = q;
                L.anyq |= q;
                while (cm) {
                    if (L.nc < 16) L.commas[L.nc] = off + __builtin_ctz(cm);
                    L.nc++;
                    cm &= cm - 1;
                }
            }
            L.len = off + k;
            return L.len <= 256 && off < 256;
        }
        if (off < 256) {
            L.qw[off >> 5] = q;
            L.anyq |= q;
            while (cm) {
                if (L.nc < 16) L.commas[L.nc] = off + __builtin_ctz(cm);
                L.nc++;
                cm &= cm - 1;
            }
        }
        off += 32;
        if (off >= 4096) {
            // very long line: find its end with memchr
            const char* nlp = (const char*)memchr(p + off, '\n', lim - (p + off));
            L.len = (u32)((nlp ? nlp : lim) - p);
            return false;
        }
    }
}

static inline bool qbit(const Line& L, u32 x) { return (L.qw[x >> 5] >> (x & 31)) & 1; }

// Field extraction per rules.md from a scanned line. Returns false if malformed.
template <int NF>
static inline bool fields(const Line& L, const char** fp, u32* fl) {
    if (L.nc != NF - 1) return false;
    u32 s = 0;
    if (!L.anyq) {
        for (int k = 0; k < NF; k++) {
            u32 e = k < NF - 1 ? L.commas[k] : L.len;
            fp[k] = L.p + s;
            fl[k] = e - s;
            s = e + 1;
        }
        return true;
    }
    u32 total = 0;
    for (int w = 0; w < 8 && (u32)w * 32 < L.len + 1; w++) total += __builtin_popcount(L.qw[w]);
    u32 expect = 0;
    for (int k = 0; k < NF; k++) {
        u32 e = k < NF - 1 ? L.commas[k] : L.len;
        if (e > s && qbit(L, s)) {
            if (e - s < 2 || !qbit(L, e - 1)) return false;
            expect += 2;
            fp[k] = L.p + s + 1;
            fl[k] = e - s - 2;
        } else {
            fp[k] = L.p + s;
            fl[k] = e - s;
        }
        s = e + 1;
    }
    return total == expect;
}

// Slow path for lines longer than 256 bytes.
template <int NF>
static inline bool split_slow(const char* b, const char* e, const char** fp, u32* fl) {
    int k = 0;
    const char* fs = b;
    for (const char* p = b;; ++p) {
        if (p == e || *p == ',') {
            if (k >= NF) return false;
            size_t len = p - fs;
            if (len && *fs == '"') {
                if (len < 2 || p[-1] != '"') return false;
                fs++;
                len -= 2;
                if (len && memchr(fs, '"', len)) return false;
            } else if (len && memchr(fs, '"', len)) {
                return false;
            }
            fp[k] = fs;
            fl[k] = (u32)len;
            k++;
            if (p == e) break;
            fs = p + 1;
        }
    }
    return k == NF;
}

template <int NF, class F>
static inline void for_rows(const char* b, const char* lim, F fn) {
    Line L;
    const char* fp[NF];
    u32 fl[NF];
    const char* p = b;
    while (p < lim) {
        bool fast = scan_line(p, lim, L);
        bool ok = fast ? fields<NF>(L, fp, fl) : split_slow<NF>(p, p + L.len, fp, fl);
        if (ok) fn(fp, fl);
        p += L.len + 1;
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

// ---------------------------------------------------------------- main
int main() {
    T0 = now_s();
    TIMING = getenv("SS_TIMING") != nullptr;
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
    ARENA.init(totalBytes * 3 + ((size_t)1 << 30));

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
    std::vector<std::vector<std::vector<char>>> keep(NT);
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
            bool usedReg = false;
            if (c.kind == 2) {
                auto& keys = lkKeys[t];
                auto& pairs = lkPairs[t];
                for_rows<2>(lb, le, [&](const char** fp, const u32* fl) {
                    if (!fl[0] || !fl[1]) return;
                    bool ca, cb;
                    u64 ka = enc_key(fp[0], fl[0], ca), kb = enc_key(fp[1], fl[1], cb);
                    if (!ca) rg.push_back({ka, std::string_view(fp[0], fl[0])}), usedReg = true;
                    if (!cb) rg.push_back({kb, std::string_view(fp[1], fl[1])}), usedReg = true;
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
                    i64 ing = parse_ts(fp[3], fl[3]);
                    i64 ingS = ing == TS_BAD ? INT32_MAX : ing / SEC - WSs;
                    if (ingS > INT32_MAX) ingS = INT32_MAX;
                    if (ingS < INT32_MIN) ingS = INT32_MIN;
                    r.ing = (i32)ingS;
                    r.ts = TS_INVALID;
                    r.ukey = 0;
                    r.packed = 0;
                    do {
                        if (!fl[1] || !fl[5]) break;
                        i64 ts = parse_ts(fp[2], fl[2]);
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
                        if (!cn) rg.push_back({r.ukey, std::string_view(fp[1], fl[1])}), usedReg = true;
                        r.ts = (u32)(ts - BASE);
                        r.packed = packed;
                    } while (0);
                    parts[ppart(r.eid)].push(r, 256);
                });
            } else {
                auto& out = ckRaw[t];
                for_rows<4>(lb, le, [&](const char** fp, const u32* fl) {
                    if (!fl[0] || !fl[1] || !fl[2]) return;
                    i64 ts = parse_ts(fp[3], fl[3]);
                    if (ts == TS_BAD || ts < WS - WEEK || ts >= WE) return;
                    bool cn;
                    u64 ck = enc_key(fp[2], fl[2], cn);
                    const u64* cv = campMap.find(ck);
                    if (!cv) return;
                    if (!cn && campIds[*cv] != std::string_view(fp[2], fl[2])) return;
                    u64 uk = enc_key(fp[1], fl[1], cn);
                    if (!cn) rg.push_back({uk, std::string_view(fp[1], fl[1])}), usedReg = true;
                    u64 cid = enc_key(fp[0], fl[0], cn);
                    if (!cn) rg.push_back({cid, std::string_view(fp[0], fl[0])}), usedReg = true;
                    out.push({uk, cid, (u32)(ts - BASE), (u32)*cv}, 4096);
                });
            }
            if (usedReg) {
                // registry string_views point into buf: keep this buffer alive
                keep[t].push_back(std::move(buf));
                buf = std::vector<char>();
            }
        });
        for (auto& r : regs) {
            for (auto& kv : r) REG.emplace(kv.first, kv.second);
            r.clear();
        }
    }
    mark("parse");

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
            static thread_local std::vector<ER> buf;
            static thread_local std::vector<u32> surv;
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
                            buf.push_back(r);
                        else if (r.ing < buf[*v].ing)
                            buf[*v] = r;
                    });
                surv.clear();
                for (u32 i = 0; i < buf.size(); i++)
                    if (buf[i].ts != TS_INVALID) surv.push_back(i);
                auto& out = evShards[t];
                size_t ns = surv.size();
                for (size_t i = 0; i < ns; i++) {
                    if (i + PD < ns) LT.prefetch(buf[surv[i + PD]].ukey);
                    const ER& r = buf[surv[i]];
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
            static thread_local std::vector<u32> cnt, since;
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
                        if (pkeys.size() * 2 > pm.k.size()) {  // grow: rehash all persons
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
            std::vector<u32> cstart(np + 1, 0);
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
                std::vector<u32> wp(cstart.begin(), cstart.end());
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
    _exit(0);
}
