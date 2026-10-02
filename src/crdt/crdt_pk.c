//
//  crdt_pk.c
//
//  Primary-key encoding, byte-compatible with sqlite-sync's pk_encode_prikey (checked against it by the differential test test/oracle_pk.c).
//    byte 0 = number of values; then per value: a type byte (low 3 bits = type, high 5 bits = the number of bytes of the length / integer that follows) and
//    INTEGER: the magnitude in 1..8 big-endian bytes, type 0 = negative, type 1 = positive, type 6 = INT64_MIN (no bytes);
//    FLOAT: type 2 (7 = negative) and the 8 bytes of the IEEE754 magnitude in little-endian order;
//    TEXT / BLOB: types 3 / 4, the length in 1..8 big-endian bytes, the data; NULL: type 5.
//
#include <string.h>
#include "crdt.h"

enum { T_NEG_INT = 0, T_MAX_NEG_INT = 6, T_NEG_FLOAT = 7 };

static size_t nbytes_for (uint64_t v) { size_t n = 1; while (v > 0xFF && n < 8) { v >>= 8; n++; } return n; }
static size_t put_be (uint8_t *o, uint64_t v, size_t nb) { for (size_t i = 0; i < nb; i++) o[i] = (uint8_t)(v >> (8 * (nb - 1 - i))); return nb; }

size_t crdt_pk_encode (const crdt_value *v, int n, uint8_t *out, size_t cap) {
    if (n < 0 || n > 255) return 0;
    size_t need = 1;
    for (int i = 0; i < n; i++) {
        switch (v[i].type) {
            case CRDT_INTEGER: need += v[i].i == INT64_MIN ? 1 : 1 + nbytes_for((uint64_t)(v[i].i < 0 ? -v[i].i : v[i].i)); break;
            case CRDT_FLOAT: need += 9; break;
            case CRDT_TEXT: case CRDT_BLOB: need += 1 + nbytes_for(v[i].n) + v[i].n; break;
            case CRDT_NULL: need += 1; break;
            default: return 0;
        }
    }
    if (!out || cap < need) return need;
    size_t o = 0; out[o++] = (uint8_t)n;
    for (int i = 0; i < n; i++) {
        switch (v[i].type) {
            case CRDT_INTEGER: {
                int64_t x = v[i].i;
                if (x == INT64_MIN) { out[o++] = T_MAX_NEG_INT; break; }
                unsigned type = CRDT_INTEGER; if (x < 0) { x = -x; type = T_NEG_INT; }
                size_t nb = nbytes_for((uint64_t)x);
                out[o++] = (uint8_t)((nb << 3) | type); o += put_be(out + o, (uint64_t)x, nb);
                break;
            }
            case CRDT_FLOAT: {
                double d = v[i].d; unsigned type = CRDT_FLOAT; if (d < 0) { d = -d; type = T_NEG_FLOAT; }
                uint64_t bits; memcpy(&bits, &d, 8);
                out[o++] = (uint8_t)type;
                for (int b = 0; b < 8; b++) out[o + (size_t)b] = (uint8_t)(bits >> (8 * b));       // little-endian bytes of the bit pattern, on every host
                o += 8;
                break;
            }
            case CRDT_TEXT: case CRDT_BLOB: {
                size_t nb = nbytes_for(v[i].n);
                out[o++] = (uint8_t)((nb << 3) | (unsigned)v[i].type); o += put_be(out + o, v[i].n, nb);
                if (v[i].n) memcpy(out + o, v[i].p, v[i].n);
                o += v[i].n;
                break;
            }
            default: out[o++] = CRDT_NULL;
        }
    }
    return o;
}

int crdt_pk_decode (const uint8_t *b, size_t len, crdt_value *out, int max) {
    if (len < 1) return -1;
    int count = b[0]; size_t o = 1;
    if (count > max) return -1;
    for (int i = 0; i < count; i++) {
        if (o >= len) return -1;
        uint8_t tb = b[o++]; unsigned type = tb & 7; size_t nb = (tb >> 3) & 0x1F;
        crdt_value *x = &out[i]; memset(x, 0, sizeof *x);
        switch (type) {
            case T_MAX_NEG_INT: if (nb) return -1; x->type = CRDT_INTEGER; x->i = INT64_MIN; break;
            case T_NEG_INT: case CRDT_INTEGER: {
                if (nb < 1 || nb > 8 || o + nb > len) return -1;
                uint64_t u = 0; for (size_t k = 0; k < nb; k++) u = (u << 8) | b[o + k]; o += nb;
                x->type = CRDT_INTEGER; x->i = type == T_NEG_INT ? -(int64_t)u : (int64_t)u; break;
            }
            case T_NEG_FLOAT: case CRDT_FLOAT: {
                if (nb || o + 8 > len) return -1;
                uint64_t bits = 0; for (int k = 0; k < 8; k++) bits |= (uint64_t)b[o + (size_t)k] << (8 * k); o += 8;
                double d; memcpy(&d, &bits, 8); x->type = CRDT_FLOAT; x->d = type == T_NEG_FLOAT ? -d : d; break;
            }
            case CRDT_TEXT: case CRDT_BLOB: {
                if (nb < 1 || nb > 8 || o + nb > len) return -1;
                uint64_t l = 0; for (size_t k = 0; k < nb; k++) l = (l << 8) | b[o + k]; o += nb;
                if (l > len - o) return -1;
                x->type = (crdt_type)type; x->p = b + o; x->n = (size_t)l; o += (size_t)l; break;
            }
            case CRDT_NULL: if (nb) return -1; x->type = CRDT_NULL; break;
            default: return -1;
        }
    }
    return count;
}
