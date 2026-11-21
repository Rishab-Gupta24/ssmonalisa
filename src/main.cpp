// Shopping Sessions pipeline: one static C++ binary.
// Reads $DATA_DIR, writes $OUTPUT_DIR/user_daily and $OUTPUT_DIR/campaign_daily as Parquet.
#include <algorithm>
#include <atomic>
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
#include <sys/mman.h>
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
    exit(1);
}

// ---------------------------------------------------------------- threads
template <class F>
static void parallel(int n, F f) {
    std::vector<std::thread> th;
    th.reserve(n);
    for (int t = 0; t < n; t++) th.emplace_back(f, t);
    for (auto& x : th) x.join();
}

// ---------------------------------------------------------------- files
struct MFile {
    const char* p = nullptr;
    size_t n = 0;
};

static MFile map_file(const std::string& path) {
    MFile m;
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) return m;
    struct stat st;
    if (fstat(fd, &st) == 0 && st.st_size > 0) {
        void* p = mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
        if (p != MAP_FAILED) {
            m.p = (const char*)p;
            m.n = st.st_size;
            madvise(p, st.st_size, MADV_WILLNEED);
        }
    }
    close(fd);
    return m;
}

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

static std::string read_small(const std::string& path) {
    MFile m = map_file(path);
    std::string s(m.p ? m.p : "", m.n);
    if (m.p) munmap((void*)m.p, m.n);
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
    size_t i = 0;
    for (size_t k = 0; k < n; k++)
        if (!dig(s[k])) return TS_BAD;
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

static inline u64 hash_bytes(const char* s, size_t n) {
    u64 h = 0x9E3779B97F4A7C15ULL ^ (n * 0xff51afd7ed558ccdULL);
    for (size_t i = 0; i < n; i++) {
        h ^= (u8)s[i];
        h *= 0x100000001b3ULL;
        h ^= h >> 29;
    }
    h ^= h >> 33;
    h *= 0xc4ceb9fe1a85ec53ULL;
    h ^= h >> 33;
    return h;
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

static std::string key_str(u64 k) {
    if (k & NONCANON) {
        auto it = REG.find(k);
        if (it == REG.end()) die("unregistered key");
        return std::string(it->second);
    }
    std::string s;
    s.push_back((char)('a' + ((k >> 48) & 31) - 1));
    for (int sh = 42; sh >= 0; sh -= 6) {
        int c = (k >> sh) & 63;
        if (!c) break;
        s.push_back(DECH[c]);
    }
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
    size_t count = 0;
    void init(size_t n) {
        size_t cap = 16;
        while (cap < n * 2) cap <<= 1;
        k.assign(cap, 0);
        v.assign(cap, 0);
        mask = cap - 1;
        shift = 64 - __builtin_ctzll(cap);
        count = 0;
    }
    inline size_t slot(u64 key) const { return (size_t)((key * 0x9E3779B97F4A7C15ULL) >> shift); }
    // returns pointer to value, inserting (with value def) if absent; sets inserted
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
                count++;
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

// ---------------------------------------------------------------- CSV
// Splits [b,e) into exactly NF fields per rules.md; returns false if malformed.
template <int NF>
static inline bool split(const char* b, const char* e, const char** fp, u32* fl) {
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

struct Chunk {
    int file;
    size_t s, e;  // nominal byte range
    int kind;     // 0 events, 1 clicks, 2 links
};

static void make_chunks(const std::vector<MFile>& files, int kind, size_t target, std::vector<Chunk>& out,
                        int file_base) {
    for (size_t f = 0; f < files.size(); f++) {
        size_t n = files[f].n;
        if (!n) continue;
        for (size_t s = 0; s < n; s += target) out.push_back({(int)(file_base + f), s, std::min(n, s + target), kind});
    }
}

// Calls fn(line_begin, line_end) for each line whose first byte lies in [c.s, c.e), skipping the header.
template <class F>
static inline void for_lines(const MFile& m, size_t s, size_t e, F fn) {
    const char* base = m.p;
    const char* end = base + m.n;
    const char* p;
    if (s == 0) {
        // skip header line
        const char* nl = (const char*)memchr(base, '\n', m.n);
        if (!nl) return;
        p = nl + 1;
    } else {
        const char* nl = (const char*)memchr(base + s - 1, '\n', m.n - (s - 1));
        if (!nl) return;
        p = nl + 1;
    }
    const char* lim = base + e;
    while (p < lim) {
        const char* nl = (const char*)memchr(p, '\n', end - p);
        const char* le = nl ? nl : end;
        fn(p, le);
        if (!nl) break;
        p = nl + 1;
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
struct URow {
    u64 pkey;
    i32 day;
    i32 topcat;  // -1 = null
    u64 sessions, events, revenue;
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
    void ebeg() {  // struct as list element
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
    int type;
    bool optional;
    int logical;  // 0 none, 1 string, 6 date
    std::string data;   // plain-encoded non-null values
    std::vector<u8> def;  // definition levels (optional only)
    size_t nvals = 0;
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
    std::string f = "PAR1";
    struct Meta {
        i64 off, size;
    };
    std::vector<Meta> metas;
    for (auto& c : cols) {
        std::string page;
        if (c.optional) page += rle_levels(c.def);
        page += c.data;
        TW ph;
        ph.i32f(1, 0);
        ph.i32f(2, (i64)page.size());
        ph.i32f(3, (i64)page.size());
        ph.sbeg(5);
        ph.i32f(1, (i64)nrows);
        ph.i32f(2, 0);
        ph.i32f(3, 3);
        ph.i32f(4, 3);
        ph.send();
        ph.b.push_back(0);
        i64 off = f.size();
        f += ph.b;
        f += page;
        metas.push_back({off, (i64)(ph.b.size() + page.size())});
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
    f += m.b;
    put_u32(f, (u32)m.b.size());
    f += "PAR1";
    FILE* fp = fopen(path.c_str(), "wb");
    if (!fp) die("cannot write parquet");
    fwrite(f.data(), 1, f.size(), fp);
    fclose(fp);
}

static void col_i64(PCol& c, i64 v) {
    c.data.append((const char*)&v, 8);
    c.nvals++;
}
static void col_i32(PCol& c, i32 v) {
    c.data.append((const char*)&v, 4);
    c.nvals++;
}
static void col_str(PCol& c, const std::string& s) {
    put_u32(c.data, (u32)s.size());
    c.data += s;
    c.nvals++;
}

// ---------------------------------------------------------------- main
int main() {
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

    // ---- products / categories
    std::vector<std::string> cats;
    HMap prodMap;
    std::string prodText = read_small(DATA + "/products.csv");
    {
        std::unordered_map<std::string, int> catIdx;
        prodMap.init(1 << 16);
        MFile m{prodText.data(), prodText.size()};
        for_lines(m, 0, m.n, [&](const char* b, const char* e) {
            const char* fp[2];
            u32 fl[2];
            if (!split<2>(b, e, fp, fl) || !fl[0]) return;
            std::string c(fp[1], fl[1]);
            auto it = catIdx.find(c);
            int ci;
            if (it == catIdx.end()) {
                ci = (int)cats.size();
                catIdx[c] = ci;
                cats.push_back(c);
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
        campMap.init(1 << 12);
        MFile m{t.data(), t.size()};
        for_lines(m, 0, m.n, [&](const char* b, const char* e) {
            const char* fp[2];
            u32 fl[2];
            if (!split<2>(b, e, fp, fl) || !fl[0]) return;
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

    // ---- map input files
    auto evNames = list_csv(DATA + "/events");
    auto ckNames = list_csv(DATA + "/clicks");
    auto lkNames = list_csv(DATA + "/links");
    std::vector<MFile> files;
    for (auto& n : evNames) files.push_back(map_file(n));
    for (auto& n : ckNames) files.push_back(map_file(n));
    for (auto& n : lkNames) files.push_back(map_file(n));
    const int EV0 = 0, CK0 = (int)evNames.size(), LK0 = CK0 + (int)ckNames.size();

    size_t totalBytes = 0;
    for (auto& f : files) totalBytes += f.n;
    size_t target = std::max<size_t>(1 << 20, std::min<size_t>(16 << 20, totalBytes / (NT * 16) + 1));

    // per-thread registries of noncanonical strings
    std::vector<std::vector<std::pair<u64, std::string_view>>> regs(NT);
    auto merge_regs = [&]() {
        for (auto& r : regs) {
            for (auto& kv : r) REG.emplace(kv.first, kv.second);
            r.clear();
        }
    };

    // ---- links
    HMap linkMap;  // user key -> person key
    {
        std::vector<Chunk> chunks;
        std::vector<MFile> lf(files.begin() + LK0, files.end());
        make_chunks(lf, 2, target, chunks, LK0);
        std::vector<std::vector<std::pair<u64, u64>>> pairs(NT);
        std::atomic<size_t> next{0};
        parallel(NT, [&](int t) {
            auto& pv = pairs[t];
            auto& rg = regs[t];
            size_t i;
            while ((i = next++) < chunks.size()) {
                const Chunk& c = chunks[i];
                for_lines(files[c.file], c.s, c.e, [&](const char* b, const char* e) {
                    const char* fp[2];
                    u32 fl[2];
                    if (!split<2>(b, e, fp, fl) || !fl[0] || !fl[1]) return;
                    bool ca, cb;
                    u64 ka = enc_key(fp[0], fl[0], ca), kb = enc_key(fp[1], fl[1], cb);
                    if (!ca) rg.push_back({ka, std::string_view(fp[0], fl[0])});
                    if (!cb) rg.push_back({kb, std::string_view(fp[1], fl[1])});
                    if (ka != kb) pv.push_back({ka, kb});
                });
            }
        });
        merge_regs();
        size_t np = 0;
        for (auto& p : pairs) np += p.size();
        linkMap.init(np * 2 + 16);
        std::vector<u32> parent;
        parent.reserve(np);
        std::vector<u64> keyOf;
        keyOf.reserve(np);
        auto idx = [&](u64 k) -> u32 {
            bool ins;
            u64* v = linkMap.upsert(k, keyOf.size(), ins);
            if (ins) {
                parent.push_back((u32)keyOf.size());
                keyOf.push_back(k);
            }
            return (u32)*v;
        };
        auto find = [&](u32 x) {
            while (parent[x] != x) {
                parent[x] = parent[parent[x]];
                x = parent[x];
            }
            return x;
        };
        for (auto& pv : pairs)
            for (auto& pr : pv) {
                u32 a = find(idx(pr.first)), b = find(idx(pr.second));
                if (a != b) {
                    if (a < b) std::swap(a, b);
                    parent[a] = b;
                }
            }
        size_t n = keyOf.size();
        std::vector<u64> minKey(n, 0);
        for (size_t i = 0; i < n; i++) {
            u32 r = find((u32)i);
            if (minKey[r] == 0 || key_less(keyOf[i], minKey[r])) minKey[r] = keyOf[i];
        }
        for (size_t i = 0; i < n; i++) *(u64*)linkMap.find(keyOf[i]) = minKey[find((u32)i)];
    }
    auto person = [&](u64 ukey) -> u64 {
        const u64* v = linkMap.find(ukey);
        return v ? *v : ukey;
    };

    // ---- parse events + clicks
    const int P = 256;  // event dedup partitions
    const int K = 256;  // person shards
    auto ppart = [](u64 k) { return (int)((k * 0x9E3779B97F4A7C15ULL) >> 56); };
    auto pshard = [](u64 k) { return (int)(((k ^ (k >> 31)) * 0xD6E8FEB86659FD93ULL) >> 56); };

    std::vector<std::vector<std::vector<ER>>> evParts(NT, std::vector<std::vector<ER>>(P));
    std::vector<std::vector<std::vector<CR>>> ckShards(NT, std::vector<std::vector<CR>>(K));
    {
        std::vector<Chunk> chunks;
        std::vector<MFile> ef(files.begin() + EV0, files.begin() + CK0);
        std::vector<MFile> cf(files.begin() + CK0, files.begin() + LK0);
        make_chunks(ef, 0, target, chunks, EV0);
        make_chunks(cf, 1, target, chunks, CK0);
        std::sort(chunks.begin(), chunks.end(),
                  [](const Chunk& a, const Chunk& b) { return (a.e - a.s) > (b.e - b.s); });
        std::atomic<size_t> next{0};
        const i64 WSs = WS / SEC;
        parallel(NT, [&](int t) {
            auto& parts = evParts[t];
            auto& shards = ckShards[t];
            auto& rg = regs[t];
            size_t i;
            while ((i = next++) < chunks.size()) {
                const Chunk& c = chunks[i];
                if (c.kind == 0) {
                    for_lines(files[c.file], c.s, c.e, [&](const char* b, const char* e) {
                        const char* fp[7];
                        u32 fl[7];
                        if (!split<7>(b, e, fp, fl) || !fl[0]) return;
                        ER r;
                        bool ecn;
                        r.eid = enc_key(fp[0], fl[0], ecn);
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
                            if (tl == 4 && memcmp(ty, "view", 4) == 0) {
                            } else if (tl == 4 && memcmp(ty, "cart", 4) == 0) {
                            } else if (tl == 8 && memcmp(ty, "purchase", 8) == 0) {
                                purchase = true;
                            } else
                                break;
                            u32 packed = 0;
                            if (purchase) {
                                const char* pr = fp[6];
                                u32 pl = fl[6];
                                if (!pl) break;
                                u32 j = 0;
                                bool ok = true;
                                for (u32 q = 0; q < pl; q++)
                                    if (!dig(pr[q])) {
                                        ok = false;
                                        break;
                                    }
                                if (!ok) break;
                                while (j < pl && pr[j] == '0') j++;
                                if (pl - j > 8) break;
                                u32 price = 0;
                                for (; j < pl; j++) price = price * 10 + (pr[j] - '0');
                                if (price < 1 || price > 10000000) break;
                                bool cn;
                                u64 pk = enc_key(fp[5], fl[5], cn);
                                const u64* cv = prodMap.find(pk);
                                u32 cat = cv ? (u32)*cv : (u32)UNKNOWN_CAT;
                                packed = price | (cat << 24);
                            }
                            bool cn;
                            r.ukey = enc_key(fp[1], fl[1], cn);
                            if (!cn) rg.push_back({r.ukey, std::string_view(fp[1], fl[1])});
                            r.ts = (u32)(ts - BASE);
                            r.packed = packed;
                        } while (0);
                        parts[ppart(r.eid)].push_back(r);
                    });
                } else {
                    for_lines(files[c.file], c.s, c.e, [&](const char* b, const char* e) {
                        const char* fp[4];
                        u32 fl[4];
                        if (!split<4>(b, e, fp, fl) || !fl[0] || !fl[1] || !fl[2]) return;
                        i64 ts = parse_ts(fp[3], fl[3]);
                        if (ts == TS_BAD || ts < WS - WEEK || ts >= WE) return;
                        bool cn;
                        u64 ck = enc_key(fp[2], fl[2], cn);
                        const u64* cv = campMap.find(ck);
                        if (!cv) return;
                        // campaign keys could collide only if noncanonical; verify string
                        if (!cn && campIds[*cv] != std::string_view(fp[2], fl[2])) return;
                        u64 uk = enc_key(fp[1], fl[1], cn);
                        if (!cn) rg.push_back({uk, std::string_view(fp[1], fl[1])});
                        u64 cid = enc_key(fp[0], fl[0], cn);
                        if (!cn) rg.push_back({cid, std::string_view(fp[0], fl[0])});
                        u64 pk = person(uk);
                        shards[pshard(pk)].push_back({pk, cid, (u32)(ts - BASE), (u32)*cv});
                    });
                }
            }
        });
        merge_regs();
    }

    // ---- dedup events per partition
    std::vector<std::vector<std::vector<EV>>> evShards(NT, std::vector<std::vector<EV>>(K));
    {
        std::atomic<int> next{0};
        parallel(NT, [&](int t) {
            HMap m;
            std::vector<ER> buf;
            auto& out = evShards[t];
            int p;
            while ((p = next++) < P) {
                size_t n = 0;
                for (int s = 0; s < NT; s++) n += evParts[s][p].size();
                m.init(n + 16);
                buf.clear();
                buf.reserve(n);
                for (int s = 0; s < NT; s++) {
                    for (const ER& r : evParts[s][p]) {
                        bool ins;
                        u64* v = m.upsert(r.eid, buf.size(), ins);
                        if (ins)
                            buf.push_back(r);
                        else if (r.ing < buf[*v].ing)
                            buf[*v] = r;
                    }
                    std::vector<ER>().swap(evParts[s][p]);
                }
                for (const ER& r : buf) {
                    if (r.ts == TS_INVALID) continue;
                    u64 pk = person(r.ukey);
                    out[pshard(pk)].push_back({pk, r.ts, r.packed});
                }
            }
        });
    }

    // ---- sessionize + attribute per shard
    const size_t CAMPW = (size_t)(NCAMP + 1) * NDAYS;
    std::vector<std::vector<URow>> urows(NT);
    // per thread: [4][NCAMP+1][NDAYS] : sessions, purchasing, revenue, assisted
    std::vector<std::vector<u64>> camp(NT, std::vector<u64>(CAMPW * 4, 0));
    {
        std::atomic<int> next{0};
        const u32 W = (u32)WEEK, GAP = 1800000;
        const u32 DAYOFF = (u32)(WS - BASE);  // rel ts of window start
        const i64 BASE_DAY_SHIFT = WS - DAY0 * DAYMS;  // window start offset within its day
        parallel(NT, [&](int t) {
            std::vector<EV> E;
            std::vector<CR> C;
            auto& rows = urows[t];
            u64* cs = camp[t].data();
            u64* cp = cs + CAMPW;
            u64* cr = cs + 2 * CAMPW;
            u64* ca = cs + 3 * CAMPW;
            std::vector<u32> cnt(NCAMP + 1, 0), since(NCAMP + 1, 0);
            std::vector<u64> catSum(cats.size(), 0);
            struct Sess {
                u32 start;
                int day;
                bool purchase;
                u64 rev;
            };
            std::vector<Sess> S;
            int s;
            while ((s = next++) < K) {
                E.clear();
                C.clear();
                for (int q = 0; q < NT; q++) {
                    auto& v = evShards[q][s];
                    E.insert(E.end(), v.begin(), v.end());
                    std::vector<EV>().swap(v);
                    auto& w = ckShards[q][s];
                    C.insert(C.end(), w.begin(), w.end());
                    std::vector<CR>().swap(w);
                }
                std::sort(E.begin(), E.end(), [](const EV& a, const EV& b) {
                    return a.pkey != b.pkey ? a.pkey < b.pkey : a.ts < b.ts;
                });
                std::sort(C.begin(), C.end(), [](const CR& a, const CR& b) {
                    if (a.pkey != b.pkey) return a.pkey < b.pkey;
                    if (a.ts != b.ts) return a.ts < b.ts;
                    return key_less(a.cid, b.cid);
                });
                size_t ci = 0;
                size_t i = 0;
                while (i < E.size()) {
                    u64 pk = E[i].pkey;
                    size_t j = i;
                    while (j < E.size() && E[j].pkey == pk) j++;
                    while (ci < C.size() && C[ci].pkey < pk) ci++;
                    size_t c0 = ci;
                    while (ci < C.size() && C[ci].pkey == pk) ci++;
                    size_t c1 = ci;
                    // sessions
                    S.clear();
                    int curDay = -1;
                    URow ur{};
                    bool anyCat = false;
                    auto flush_day = [&]() {
                        if (curDay < 0) return;
                        ur.pkey = pk;
                        ur.day = (i32)(DAY0 + curDay);
                        ur.topcat = -1;
                        if (anyCat) {
                            int best = -1;
                            for (size_t c = 0; c < catSum.size(); c++) {
                                if (!catSum[c]) continue;
                                if (best < 0 || catSum[c] > catSum[best] ||
                                    (catSum[c] == catSum[best] && catRank[c] < catRank[best]))
                                    best = (int)c;
                            }
                            ur.topcat = best;
                            std::fill(catSum.begin(), catSum.end(), 0);
                        }
                        rows.push_back(ur);
                    };
                    size_t k = i;
                    while (k < j) {
                        u32 st = E[k].ts;
                        int day = (int)(((i64)(st - DAYOFF) + BASE_DAY_SHIFT) / DAYMS);
                        if (day != curDay) {
                            flush_day();
                            curDay = day;
                            ur = URow{};
                            anyCat = false;
                        }
                        Sess se{st, day, false, 0};
                        u32 prev = st;
                        u64 nev = 0;
                        while (k < j && E[k].ts - prev <= GAP) {
                            prev = E[k].ts;
                            u32 pkd = E[k].packed;
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
                        ur.sessions++;
                        ur.events += nev;
                        ur.revenue += se.rev;
                        S.push_back(se);
                    }
                    flush_day();
                    // attribution
                    size_t lo = c0, hi = c0;
                    u32 nS = (u32)S.size();
                    auto close = [&](u32 c, u32 a, u32 b) {
                        for (u32 x = a; x < b; x++) ca[(size_t)c * NDAYS + S[x].day]++;
                    };
                    for (u32 si = 0; si < nS; si++) {
                        u32 st = S[si].start;
                        while (hi < c1 && C[hi].ts <= st) {
                            u32 c = C[hi].camp;
                            if (cnt[c]++ == 0) since[c] = si;
                            hi++;
                        }
                        while (lo < hi && C[lo].ts + W < st) {
                            u32 c = C[lo].camp;
                            if (--cnt[c] == 0) close(c, since[c], si);
                            lo++;
                        }
                        u32 cc = ORGANIC;
                        if (lo < hi) {
                            size_t q = hi - 1;
                            while (q > lo && C[q - 1].ts == C[q].ts) q--;
                            cc = C[q].camp;
                        }
                        size_t o = (size_t)cc * NDAYS + S[si].day;
                        cs[o]++;
                        if (S[si].purchase) cp[o]++;
                        cr[o] += S[si].rev;
                    }
                    for (size_t q = lo; q < hi; q++) {
                        u32 c = C[q].camp;
                        if (cnt[c]) {
                            close(c, since[c], nS);
                            cnt[c] = 0;
                        }
                    }
                    i = j;
                }
            }
        });
    }

    // ---- write outputs
    std::string udir = OUT + "/user_daily", cdir = OUT + "/campaign_daily";
    mkdir(OUT.c_str(), 0777);
    mkdir(udir.c_str(), 0777);
    mkdir(cdir.c_str(), 0777);
    size_t totalRows = 0;
    for (auto& r : urows) totalRows += r.size();
    std::vector<std::thread> writers;
    for (int t = 0; t < NT; t++) {
        if (urows[t].empty() && !(t == 0 && totalRows == 0)) continue;
        writers.emplace_back([&, t]() {
            auto& rows = urows[t];
            std::vector<PCol> cols(6);
            cols[0] = {"user_id", P_BYTES, false, 1};
            cols[1] = {"day", P_INT32, false, 6};
            cols[2] = {"sessions", P_INT64, false, 0};
            cols[3] = {"events", P_INT64, false, 0};
            cols[4] = {"revenue_cents", P_INT64, false, 0};
            cols[5] = {"top_category", P_BYTES, true, 1};
            for (auto& r : rows) {
                col_str(cols[0], key_str(r.pkey));
                col_i32(cols[1], r.day);
                col_i64(cols[2], (i64)r.sessions);
                col_i64(cols[3], (i64)r.events);
                col_i64(cols[4], (i64)r.revenue);
                if (r.topcat >= 0) {
                    col_str(cols[5], cats[r.topcat]);
                    cols[5].def.push_back(1);
                } else
                    cols[5].def.push_back(0);
            }
            write_parquet(udir + "/part-" + std::to_string(t) + ".parquet", cols, rows.size());
        });
    }
    {
        std::vector<PCol> cols(7);
        cols[0] = {"campaign_id", P_BYTES, false, 1};
        cols[1] = {"channel", P_BYTES, false, 1};
        cols[2] = {"day", P_INT32, false, 6};
        cols[3] = {"sessions", P_INT64, false, 0};
        cols[4] = {"purchasing_sessions", P_INT64, false, 0};
        cols[5] = {"revenue_cents", P_INT64, false, 0};
        cols[6] = {"assisted_sessions", P_INT64, false, 0};
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
    fflush(stdout);
    _exit(0);
}
