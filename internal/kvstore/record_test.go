package kvstore

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"strings"
	"testing"
)

func mustFrame(t *testing.T, key, value string) []byte {
	t.Helper()
	entry, err := EncodeEntry(key, value)
	if err != nil {
		t.Fatalf("EncodeEntry(%q,%q) = %v", key, value, err)
	}
	frame, err := EncodeFrame(entry)
	if err != nil {
		t.Fatalf("EncodeFrame = %v", err)
	}
	return frame
}

// 逐字节对齐 2.1 §5 的字段表与算例。
func TestRecordBytesMatchSpec(t *testing.T) {
	entry, err := EncodeEntry("k1", "hello")
	if err != nil {
		t.Fatalf("EncodeEntry = %v", err)
	}
	wantEntry := []byte{
		0x01,
		0x00, 0x00, 0x00, 0x02, 'k', '1',
		0x00, 0x00, 0x00, 0x05, 'h', 'e', 'l', 'l', 'o',
	}
	if len(entry) != 16 {
		t.Fatalf("entry = %d bytes, want 16", len(entry))
	}
	for i := range wantEntry {
		if entry[i] != wantEntry[i] {
			t.Fatalf("entry[%d] = 0x%02x, want 0x%02x", i, entry[i], wantEntry[i])
		}
	}

	frame, err := EncodeFrame(entry)
	if err != nil {
		t.Fatalf("EncodeFrame = %v", err)
	}
	if len(frame) != 24 {
		t.Fatalf("record = %d bytes, want 24", len(frame))
	}
	if got := binary.BigEndian.Uint32(frame[:4]); got != 16 {
		t.Errorf("len field = %d, want 16", got)
	}
	if want := crcOf(16, entry); binary.BigEndian.Uint32(frame[4:8]) != want {
		t.Errorf("crc field = 0x%08x, want 0x%08x", binary.BigEndian.Uint32(frame[4:8]), want)
	}
}

// crc 的覆盖范围必须含 len：只盖 entry 的写法要能被这条测出来。
func TestCRCOverCoversLenField(t *testing.T) {
	frame := mustFrame(t, "k1", "hello")
	entry := frame[FrameHeaderSize:]
	stored := binary.BigEndian.Uint32(frame[4:8])

	if entryOnly := crc32.Checksum(entry, crcTable); entryOnly == stored {
		t.Error("crc equals the entry-only value, so len is not covered")
	}
	if ieee := crc32.ChecksumIEEE(append(frame[:4:4], entry...)); ieee == stored {
		t.Errorf("crc equals the IEEE value 0x%08x, not Castagnoli", ieee)
	}
	if got := crc32.Checksum(append([]byte{}, frame[:4]...), crcTable); got == stored {
		t.Error("len-field-only crc collides with the record crc")
	}
}

func TestFlipEveryLenBitIsDetected(t *testing.T) {
	frame := mustFrame(t, "k1", "hello")
	entry := frame[FrameHeaderSize:]

	for bit := 0; bit < 32; bit++ {
		tampered := append([]byte{}, frame...)
		tampered[bit/8] ^= 1 << (bit % 8)

		size, checksum, err := DecodeFrame(tampered[:FrameHeaderSize], 0)
		if err == nil {
			err = CheckFrame(size, checksum, entry, 0)
		}
		if !errors.Is(err, ErrCorruptRecord) {
			t.Fatalf("len bit %d undetected: size=%d err=%v", bit, size, err)
		}
	}
}

func TestFlipEveryEntryBitIsDetected(t *testing.T) {
	frame := mustFrame(t, "k1", "hello")

	for bit := 0; bit < len(frame[FrameHeaderSize:])*8; bit++ {
		tampered := append([]byte{}, frame...)
		tampered[FrameHeaderSize+bit/8] ^= 1 << (bit % 8)

		size, checksum, err := DecodeFrame(tampered[:FrameHeaderSize], 0)
		if err == nil {
			err = CheckFrame(size, checksum, tampered[FrameHeaderSize:], 0)
		}
		if !errors.Is(err, ErrCorruptRecord) {
			t.Fatalf("entry bit %d undetected: err=%v", bit, err)
		}
	}
}

func TestDecodeFrameRejectsOversizeLen(t *testing.T) {
	head := make([]byte, FrameHeaderSize)
	binary.BigEndian.PutUint32(head[:4], MaxEntrySize+1)

	if _, _, err := DecodeFrame(head, 0); !errors.Is(err, ErrCorruptRecord) {
		t.Fatalf("DecodeFrame(4MB+1) err = %v, want ErrCorruptRecord", err)
	}
	binary.BigEndian.PutUint32(head[:4], MaxEntrySize)
	if size, _, err := DecodeFrame(head, 0); err != nil || size != MaxEntrySize {
		t.Fatalf("DecodeFrame(4MB) = %d, %v; the limit itself must pass", size, err)
	}
}

func TestEncodeRejectsOversize(t *testing.T) {
	if _, err := EncodeFrame(make([]byte, MaxEntrySize+1)); !errors.Is(err, ErrTooLarge) {
		t.Errorf("EncodeFrame(4MB+1) err = %v, want ErrTooLarge", err)
	}
	entry, err := EncodeEntry("k", string(make([]byte, MaxValueSize)))
	if err != nil {
		t.Fatalf("value at the 1MB limit: %v", err)
	}
	if _, err := EncodeFrame(entry); err != nil {
		t.Errorf("EncodeFrame at the business limit = %v", err)
	}
}

func TestEncodeRejectsOversizedKeyAndValue(t *testing.T) {
	cases := []struct {
		name       string
		key, value string
	}{
		{"key 超 1KB", string(make([]byte, MaxKeySize+1)), "v"},
		{"value 超 1MB", "k", string(make([]byte, MaxValueSize+1))},
	}
	for _, tc := range cases {
		entry, err := EncodeEntry(tc.key, tc.value)
		if !errors.Is(err, ErrTooLarge) {
			t.Errorf("%s: err = %v, want ErrTooLarge", tc.name, err)
			continue
		}
		if len(entry) != 0 {
			t.Errorf("%s produced %d bytes after rejecting", tc.name, len(entry))
		}
		if !strings.Contains(err.Error(), "limit") {
			t.Errorf("%s: message = %q, want the limit in it", tc.name, err)
		}
	}
}

func TestOpMustBePut(t *testing.T) {
	entry, err := EncodeEntry("k1", "hello")
	if err != nil {
		t.Fatalf("EncodeEntry = %v", err)
	}
	entry[0] = 0x02
	_, _, err = DecodeEntry(entry, 0)
	if !errors.Is(err, ErrCorruptRecord) {
		t.Fatalf("op 0x02 accepted: err = %v", err)
	}
	if !strings.Contains(err.Error(), "offset=0") {
		t.Errorf("corrupt error lost the offset: %v", err)
	}
}

func TestEmptyEntryIsCorrupt(t *testing.T) {
	frame, err := EncodeFrame(nil)
	if err != nil {
		t.Fatalf("EncodeFrame(nil) = %v", err)
	}
	size, checksum, err := DecodeFrame(frame, 0)
	if err != nil {
		t.Fatalf("len=0 must pass the header check: %v", err)
	}
	if size != 0 || CheckFrame(size, checksum, nil, 0) != nil {
		t.Fatalf("len=0 frame did not verify: size=%d", size)
	}
	if _, _, err := DecodeEntry(frame[FrameHeaderSize:], 0); !errors.Is(err, ErrCorruptRecord) {
		t.Errorf("empty entry decoded without error: %v", err)
	}
}

// 内部长度与 entry 实际长度不符，且 crc 是自算的：只能被字段校验挡住。
func TestInconsistentInnerLengths(t *testing.T) {
	entry, err := EncodeEntry("k1", "hello")
	if err != nil {
		t.Fatalf("EncodeEntry = %v", err)
	}
	cases := map[string]func([]byte){
		"keylen 虚高":          func(e []byte) { binary.BigEndian.PutUint32(e[1:5], 3) },
		"vallen 虚高":          func(e []byte) { binary.BigEndian.PutUint32(e[1+4+2:1+4+2+4], 6) },
		"keylen 跑到 entry 之后": func(e []byte) { binary.BigEndian.PutUint32(e[1:5], 1<<20) },
	}
	for name, tamper := range cases {
		bad := append([]byte{}, entry...)
		tamper(bad)
		if _, _, err := DecodeEntry(bad, 7); !errors.Is(err, ErrCorruptRecord) {
			t.Errorf("%s: err = %v, want ErrCorruptRecord", name, err)
		} else if !strings.Contains(err.Error(), "offset=7") {
			t.Errorf("%s: message = %q, want offset=7", name, err)
		}
	}
}

func TestRoundTrip(t *testing.T) {
	cases := [][2]string{
		{"k1", "hello"},
		{"", ""},
		{"", "value without key"},
		{"key without value", ""},
		{"中文键", "中文值"},
		{"binary\x00key", "binary\x00value"},
	}
	for _, tc := range cases {
		entry, err := EncodeEntry(tc[0], tc[1])
		if err != nil {
			t.Fatalf("EncodeEntry(%q,%q) = %v", tc[0], tc[1], err)
		}
		frame, err := EncodeFrame(entry)
		if err != nil {
			t.Fatalf("EncodeFrame = %v", err)
		}

		size, checksum, err := DecodeFrame(frame[:FrameHeaderSize], 0)
		if err != nil {
			t.Fatalf("DecodeFrame = %v", err)
		}
		if int(size) != len(entry) {
			t.Fatalf("size = %d, want %d", size, len(entry))
		}
		gotEntry := frame[FrameHeaderSize:]
		if err := CheckFrame(size, checksum, gotEntry, 0); err != nil {
			t.Fatalf("CheckFrame(%q,%q) = %v", tc[0], tc[1], err)
		}
		key, value, err := DecodeEntry(gotEntry, 0)
		if err != nil {
			t.Fatalf("DecodeEntry(%q,%q) = %v", tc[0], tc[1], err)
		}
		if key != tc[0] || value != tc[1] {
			t.Errorf("round trip = (%q,%q), want (%q,%q)", key, value, tc[0], tc[1])
		}
	}
}
