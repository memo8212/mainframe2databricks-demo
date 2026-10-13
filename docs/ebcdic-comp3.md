# EBCDIC, COMP-3 and Fixed-Length Records — Deep Dive

Mainframe data doesn't look like modern files. Before Spark can read it you must
**decode** three things: the character set (EBCDIC), the numeric encoding
(packed/zoned decimal), and the record structure (fixed-length, copybook-driven).
This page explains each, with the working decoder in `converter/mf_codec.py` and
a runnable demo in `runner/ebcdic_comp3_demo.py`.

---

## 1. EBCDIC vs ASCII

EBCDIC (Extended Binary Coded Decimal Interchange Code) is IBM's character
encoding. The letter `A` is `0x41` in ASCII but `0xC1` in EBCDIC; digits differ
too (`'0'` = `0x30` ASCII vs `0xF0` EBCDIC).

Consequences:
- You **cannot** `read.csv` a raw EBCDIC file and get sensible text.
- Decode with a **code page**: US EBCDIC is `cp037`; others exist (`cp500` for
  international, `cp1140` etc.).

```python
b"\xc1\xc2\xc3".decode("cp037")   # -> 'ABC'
"ABC".encode("cp037")             # -> b'\xc1\xc2\xc3'
```

In `mf_codec.py`:

```python
EBCDIC = "cp037"
row[f.name] = chunk.decode(EBCDIC).strip()   # character fields
```

> Watch the code page: the same bytes decode to different characters under
> `cp037` vs `cp500`. Confirm the source's code page during discovery.

---

## 2. Numeric encodings

COBOL stores numbers several ways. The two you meet most:

### 2.1 Zoned / display decimal (`PIC 9(n)`, `S9(n)`)

One **byte per digit**, in EBCDIC. The last byte's zone carries the sign:
`C`/`F` = positive, `D` = negative. Length = number of digits.

```
PIC 9(08) value 20240131   ->  F2 F0 F2 F4 F0 F1 F3 F1   (8 bytes)
```

### 2.2 Packed decimal (`PIC S9(n)V99 COMP-3`)

**Two digits per byte**; the final nibble is the sign: `C`=+, `D`=−, `F`=unsigned.
The `V` is an **implied** decimal point — it occupies **no** byte.

Length = `ceil((digits + 1) / 2)`.

Example: `PIC S9(11)V99 COMP-3` → digits = 13, +1 sign = 14 nibbles = **7 bytes**.

```
value 12345.67  ->  scaled integer 1234567
nibbles: 0 0 0 0 0 1 2 3 4 5 6 7 C      (13 digits + sign C)
bytes:   00 00 01 23 45 67 0C
```

Sign handling on decode:
```python
sign_nib = nibbles[-1]
digits   = "".join(str(n) for n in nibbles[:-1])
value    = int(digits) / 10**decimals
if sign_nib == 0x0D:   # 'D'
    value = -value
```

`mf_codec.py` implements both directions (`_pack` / `_unpack`), so you can
round-trip.

---

## 3. Fixed-length records & the copybook

Mainframe files are **fixed-length**, laid out back-to-back with no delimiters.
The **copybook** is the schema:

```
       01  ACCT-RECORD.
           05 ACCT-NO          PIC X(10).        <- offset 0,  len 10
           05 ACCT-NAME        PIC X(30).        <- offset 10, len 30
           05 ACCT-TYPE        PIC X(02).        <- offset 40, len 2
           05 ACCT-BALANCE     PIC S9(11)V99 COMP-3.  <- offset 42, len 7
           05 ACCT-STATUS      PIC X(01).        <- offset 49, len 1
           05 ACCT-LAST-DATE   PIC 9(08).        <- offset 50, len 8
                                                 total = 58 bytes
```

`parse_copybook()` reads the `PIC` clauses and computes each field's **offset**
and **length**, so decoding is just slicing:

```python
for i in range(0, len(blob), record_length):
    row = decode_record(blob[i:i+record_length], layout)
```

### The decode pipeline

```
VSAM/sequential file (EBCDIC, fixed-length, COMP-3)
        │  mf_codec.decode_record()  <- copybook-driven
        ▼
rows (strings + decimals)
        │  write Parquet
        ▼
bridge  ->  Spark reads Parquet  ->  Bronze/Silver/Gold Delta
```

Decoding is a **Bronze-step concern**: do it once, write Parquet, and let Spark
work in normal types from then on.

---

## 4. Gotchas that bite in real migrations

| Gotcha | Symptom | Handling |
|---|---|---|
| Wrong code page | garbled text, wrong symbols | confirm `cp037` vs `cp500` vs `cp1140` |
| Implied decimals (`V`) | values 100× too big | track `decimals` per field |
| Sign nibble | negatives become huge positives | `D`/`C`/`F` handling |
| Unsigned vs signed | unexpected negatives | honor `S` in the PIC |
| Redefines / OCCURS | misaligned fields after a point | model as union/arrays; offsets shift |
| Variable-length (`RECFM=VB`) | records run together | strip the 4-byte RDW/BDW prefix |
| COMP (binary) vs COMP-3 | wrong numeric values | `COMP`/`COMP-4` are big-endian binary, not packed |
| Trailing spaces | join keys mismatch | `.strip()` char fields |
| Padded/justified text | `ACCT-NAME` trailing blanks | trim; or preserve fixed width if needed |

### Redefines and OCCURS (briefly)

- **REDEFINES** overlays two interpretations on the same bytes → decode both and
  choose per record, or pick the layout that matches the record type.
- **OCCURS n TIMES** repeats a group → length × n; decode into an array.

These are why a *generic* copybook parser is hard; production tools (and careful
hand-coding) handle the long tail. This POC covers the common 90%.

---

## 5. `COMP` vs `COMP-3` vs `DISPLAY`

| USAGE | Stored as | Length (PIC 9(9)) | Notes |
|---|---|---|---|
| `DISPLAY` (default) | EBCDIC digit per byte | 9 | zoned, sign in last zone |
| `COMP-3` (packed) | 2 digits/byte + sign | 5 | most common for money |
| `COMP` / `COMP-4` | big-endian binary | 4 | integers; `COMP-1/2` are float |

The demo uses `COMP-3` for `ACCT-BALANCE` (money) and `DISPLAY` for
`ACCT-LAST-DATE` — a typical mix.

---

## 6. Try it

```bash
python converter/mf_codec.py          # field layout, hex, round-trip self-test
python runner/ebcdic_comp3_demo.py    # encode CSV -> EBCDIC, decode back, verify
```

`mf_codec.py` prints the packed balance bytes and their nibbles so you can see
the sign nibble; the demo encodes all accounts, decodes the raw bytes, and
confirms the decoded rows equal the original CSV before writing a Parquet bridge.

---

## 7. Why this matters for the migration

- **Fidelity:** decoding correctly is the difference between "the pipeline runs"
  and "the numbers are right". Money in COMP-3 is where errors hide.
- **One-time cost:** decode at the edge (Bronze), then everything downstream is
  ordinary Spark/Delta.
- **Testability:** `runner/ebcdic_comp3_demo.py` is a regression test — it proves
  the codec round-trips before you trust it on production extracts.
- **Automation:** the copybook already exists; reuse it to generate the layout
  (and even the Spark `StructType`, see `converter/mf2dbx_converter.py`).
