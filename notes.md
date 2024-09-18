## Notes

# Binary Encoding

- 8 bits = 1 byte

- Masking

> Masking is a technique used to isolate certain bits in a byte. In binary each bit is either a 0 or 1, and masking allows you to select the bits you care about.

> The basic idea is to use `Bitwise AND` operation with a **mask** - a binary numbers where some bits are set to 1 (to keep the corresponding bits in the original order.) and others are set to 0 to "mask" out the other bits.


See the below example:

```
b := 0xA7 // which is 10100111 in binary
```

Say we wanted to extract the lower 6 bits of this byte and ignore the first two bits, we can do the following:

```
lower6bits := (b & 0x3F)
```

In binary, `0x3F` = `00111111` so this can be used as our mask.

This works as follows:

```
    b:    10100111  (0xA7)
    mask: 00111111  (0x3F)
--------------------------
    result: 00100111  (0x27)
```

So the size in this case is `0x27` or `39` in decimal.

- Bit Masking

> Bit masking works by shifting bits to either the left or right. This can be useful when you want to check only certain bits in a given byte. For example:

```go
b := []byte("test")
firstTwoBits := b >> 6
```

In the above example, we're shifting bits to the right by 6 so as we can ge the first two bits.

- Big Endian

> In a big-endian system, the most significant _byte_ is stored first at the smallest address. This is like reading numbers from left to right.

`0x12345678` is stored as `12 34 56 78`.

- Little Endian

> In a littel-endian system the least significant _byte_ is stored first at the smallest address. This is like reading numbers backwards.

`0x12345678` is stored as `78 56 34 12`.
