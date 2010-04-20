// Written in the D programming language.

/**
 * MessagePack for D, serializing routine
 *
 * Copyright: Copyright Masahiro Nakagawa 2010.
 * License:   <a href = "http://www.boost.org/LICENSE_1_0.txt">Boost License 1.0</a>.
 * Authors:   Masahiro Nakagawa
 */
module msgpack.packer;

import msgpack.common;

import std.traits;
import std.typetuple;


/**
 * $(D isWritableBuffer) is a constraint for buffer that $(D Packer) uses.
 *
 * Returns true if $(D_PARAM Buffer) is an byte buffer that defines write method.
 * The following code is a concept example.
-----
Buffer  buffer;
ubyte[] values = [1];

buffer.write(values[0]);
buffer.write(values);
-----
 */
template isWritableBuffer(Buffer)
{
    // which is better __traits or is(typeof({}()))?
    enum bool isWritableBuffer = __traits(compiles,
    {
        Buffer  buffer;
        ubyte[] values = [1];
        buffer.write(values[0]);
        buffer.write(values);
    });
}


/**
 * $(D Packer) is a $(D MessagePack) serializer
 *
 * Example:
-----
auto buffer = vrefBuffer;
auto packer = packer(&buffer);

packer.packArray(4);  // sets array length
packer.packFalse();   // false
packer.pack(100);     // 100   of int
packer.pack(1e-10);   // 1e-10 of double
packer.packNil();     // null

stdout.rawWrite(buffer.data);  // or packer.buffer.data
-----
 *
 * Some buffers that Packer can use are in $(D msgpack.buffer),
 * and you use another object if the $(D Buffer) object meets $(D isWritableBuffer).
 */
struct Packer(Buffer) if (isWritableBuffer!(Buffer))
{
  private:
    alias .Packer!(Buffer) Packer;

    enum size_t Offset = 1;  // type-information offset

    Buffer   buffer_;  // the buffer to write
    ubyte[9] store_;   // stores serialized value


  public:
    /**
     * Constructs a packer with $(D_PARAM buffer).
     *
     * Params:
     *  buffer = the buffer to write.
     */
    this(Buffer buffer)
    {
        buffer_ = buffer;
    }


    /**
     * Forwards to buffer.
     *
     * Returns:
     *  the buffer.
     */
    @property Buffer buffer()
    {
        return buffer_;
    }


    /**
     * Serializes $(D_PARAM value) and writes to buffer.
     *
     * Params:
     *  value = the content to serialize.
     *
     * Returns:
     *  this to method chain.
     */
    Packer pack(T)(in T value) if (is(Unqual!T == bool))
    {
        return value ? packTrue() : packFalse();
    }


    /// ditto
    Packer pack(T)(in T value) if (is(Unqual!T == ubyte))
    {
        if (value < (1 << 7)) {
            // fixnum
            buffer_.write(take8from(value));
        } else {
            // uint 8
            store_[0] = Format.UINT8;
            store_[1] = take8from(value);
            buffer_.write(store_[0..Offset + ubyte.sizeof]);
        }

        return this;
    }


    /// ditto
    Packer pack(T)(in T value) if (is(Unqual!T == ushort))
    {
        if (value < (1 << 7)) {
            // fixnum
            buffer_.write(take8from!16(value));
        } else if (value < (1 << 8)) {
            // uint 8
            store_[0] = Format.UINT8;
            store_[1] = take8from!16(value);
            buffer_.write(store_[0..Offset + ubyte.sizeof]);
        } else {
            // uint 16
            const temp = convertEndianTo!16(value);

            store_[0] = Format.UINT16;
            *cast(ushort*)&store_[Offset] = temp;
            buffer_.write(store_[0..Offset + ushort.sizeof]);
        }

        return this;
    }


    /// ditto
    Packer pack(T)(in T value) if (is(Unqual!T == uint))
    {
        if (value < (1 << 8)) {
            if (value < (1 << 7)) {
                // fixnum
                buffer_.write(take8from!32(value));
            } else {
                // uint 8
                store_[0] = Format.UINT8;
                store_[1] = take8from!32(value);
                buffer_.write(store_[0..Offset + ubyte.sizeof]);
            }
        } else {
            if (value < (1 << 16)) {
                // uint 16
                const temp = convertEndianTo!16(value);

                store_[0] = Format.UINT16;
                *cast(ushort*)&store_[Offset] = temp;
                buffer_.write(store_[0..Offset + ushort.sizeof]);
            } else {
                // uint 32
                const temp = convertEndianTo!32(value);

                store_[0] = Format.UINT32;
                *cast(uint*)&store_[Offset] = temp;
                buffer_.write(store_[0..Offset + uint.sizeof]);
            }
        }

        return this;
    }


    /// ditto
    Packer pack(T)(in T value) if (is(Unqual!T == ulong))
    {
        if (value < (1UL << 8)) {
            if (value < (1UL << 7)) {
                // fixnum
                buffer_.write(take8from!64(value));
            } else {
                // uint 8
                store_[0] = Format.UINT8;
                store_[1] = take8from!64(value);
                buffer_.write(store_[0..Offset + ubyte.sizeof]);
            }
        } else {
            if (value < (1UL << 16)) {
                // uint 16
                const temp = convertEndianTo!16(value);

                store_[0] = Format.UINT16;
                *cast(ushort*)&store_[Offset] = temp;
                buffer_.write(store_[0..Offset + ushort.sizeof]);
            } else if (value < (1UL << 32)){
                // uint 32
                const temp = convertEndianTo!32(value);

                store_[0] = Format.UINT32;
                *cast(uint*)&store_[Offset] = temp;
                buffer_.write(store_[0..Offset + uint.sizeof]);
            } else {
                // uint 64
                const temp = convertEndianTo!64(value);

                store_[0] = Format.UINT64;
                *cast(ulong*)&store_[Offset] = temp;
                buffer_.write(store_);
            }
        }

        return this;
    }


    /// ditto
    Packer pack(T)(in T value) if (is(Unqual!T == byte))
    {
        if (value < -(1 << 5)) {
            // int 8
            store_[0] = Format.INT8;
            store_[1] = take8from(value);
            buffer_.write(store_[0..Offset + byte.sizeof]);
        } else {
            // fixnum
            buffer_.write(take8from(value));
        }

        return this;
    }


    /// ditto
    Packer pack(T)(in T value) if (is(Unqual!T == short))
    {
        if (value < -(1 << 5)) {
            if (value < -(1 << 7)) {
                // int 16
                const temp = convertEndianTo!16(value);

                store_[0] = Format.INT16;
                *cast(short*)&store_[Offset] = temp;
                buffer_.write(store_[0..Offset + short.sizeof]);
            } else {
                // int 8
                store_[0] = Format.INT8;
                store_[1] = take8from!16(value);
                buffer_.write(store_[0..Offset + byte.sizeof]);
            }
        } else if (value < (1 << 7)) {
            // fixnum
            buffer_.write(take8from!16(value));
        } else {
            if (value < (1 << 8)) {
                // uint 8
                store_[0] = Format.UINT8;
                store_[1] = take8from!16(value);
                buffer_.write(store_[0..Offset + ubyte.sizeof]);                
            } else {
                // uint 16
                const temp = convertEndianTo!16(value);

                store_[0] = Format.UINT16;
                *cast(ushort*)&store_[Offset] = temp;
                buffer_.write(store_[0..Offset + ushort.sizeof]);
            }
        }

        return this;
    }


    /// ditto
    Packer pack(T)(in T value) if (is(Unqual!T == int))
    {
        if (value < -(1 << 5)) {
            if (value < -(1 << 15)) {
                // int 32
                const temp = convertEndianTo!32(value);

                store_[0] = Format.INT32;
                *cast(int*)&store_[Offset] = temp;
                buffer_.write(store_[0..Offset + int.sizeof]);
            } else if (value < -(1 << 7)) {
                // int 16
                const temp = convertEndianTo!16(value);

                store_[0] = Format.INT16;
                *cast(short*)&store_[Offset] = temp;
                buffer_.write(store_[0..Offset + short.sizeof]);
            } else {
                // int 8
                store_[0] = Format.INT8;
                store_[1] = take8from!32(value);
                buffer_.write(store_[0..Offset + byte.sizeof]);
            }
        } else if (value < (1 << 7)) {
            // fixnum
            buffer_.write(take8from!32(value));
        } else {
            if (value < (1 << 8)) {
                // uint 8
                store_[0] = Format.UINT8;
                store_[1] = take8from!32(value);
                buffer_.write(store_[0..Offset + ubyte.sizeof]);
            } else if (value < (1 << 16)) {
                // uint 16
                const temp = convertEndianTo!16(value);

                store_[0] = Format.UINT16;
                *cast(ushort*)&store_[Offset] = temp;
                buffer_.write(store_[0..Offset + ushort.sizeof]);
            } else {
                // uint 32
                const temp = convertEndianTo!32(value);

                store_[0] = Format.UINT32;
                *cast(uint*)&store_[Offset] = temp;
                buffer_.write(store_[0..Offset + uint.sizeof]);
            }
        }

        return this;
    }


    /// ditto
    Packer pack(T)(in T value) if (is(Unqual!T == long))
    {
        if (value < -(1L << 5)) {
            if (value < -(1L << 15)) {
                if (value < -(1L << 31)) {
                    // int 64
                    const temp = convertEndianTo!64(value);

                    store_[0] = Format.INT64;
                    *cast(long*)&store_[Offset] = temp;
                    buffer_.write(store_);
                } else {
                    // int 32
                    const temp = convertEndianTo!32(value);

                    store_[0] = Format.INT32;
                    *cast(int*)&store_[Offset] = temp;
                    buffer_.write(store_[0..Offset + int.sizeof]);
                }
            } else {
                if (value < -(1L << 7)) {
                    // int 16
                    const temp = convertEndianTo!16(value);

                    store_[0] = Format.INT16;
                    *cast(short*)&store_[Offset] = temp;
                    buffer_.write(store_[0..Offset + short.sizeof]);
                } else {
                    // int 8
                    store_[0] = Format.INT8;
                    store_[1] = take8from!64(value);
                    buffer_.write(store_[0..Offset + byte.sizeof]);
                }
            }
        } else if (value < (1L << 7)) {
            // fixnum
            buffer_.write(take8from!64(value));
        } else {
            if (value < (1L << 16)) {
                if (value < (1L << 8)) {
                    // uint 8
                    store_[0] = Format.UINT8;
                    store_[1] = take8from!64(value);
                    buffer_.write(store_[0..Offset + ubyte.sizeof]);
                } else {
                    // uint 16
                    const temp = convertEndianTo!16(value);

                    store_[0] = Format.UINT16;
                    *cast(ushort*)&store_[Offset] = temp;
                    buffer_.write(store_[0..Offset + ushort.sizeof]);
                }
            } else {
                if (value < (1L << 32)) {
                    // uint 32
                    const temp = convertEndianTo!32(value);

                    store_[0] = Format.UINT32;
                    *cast(uint*)&store_[Offset] = temp;
                    buffer_.write(store_[0..Offset + uint.sizeof]);
                } else {
                    // uint 64
                    const temp = convertEndianTo!64(value);

                    store_[0] = Format.UINT64;
                    *cast(ulong*)&store_[Offset] = temp;
                    buffer_.write(store_);
                }
            }
        }

        return this;
    }


    /// ditto
    Packer pack(T)(in T value) if (is(Unqual!T == float))
    {
        union _ { float f; uint i; }

        const temp = convertEndianTo!32(_(value).i);

        store_[0] = Format.FLOAT;
        *cast(uint*)&store_[Offset] = temp;
        buffer_.write(store_[0..Offset + uint.sizeof]);

        return this;
    }


    /// ditto
    Packer pack(T)(in T value) if (is(Unqual!T == double))
    {
        union _ { double f; ulong i; }

        const temp = convertEndianTo!64(_(value).i);

        store_[0] = Format.DOUBLE;
        *cast(ulong*)&store_[Offset] = temp;
        buffer_.write(store_);

        return this;
    }


    /// ditto
    Packer pack(T)(in T array) if (isArray!(T))
    {
        alias typeof(T.init[0]) U;

        if (array is null)
            return packNil();

        // Raw bytes
        static if (isByte!(U) || isSomeChar!(U)) {
            ubyte[] raw = cast(ubyte[])array;

            packRaw(raw.length);
            buffer_.write(raw);
        } else {
            packArray(array.length);

            foreach (elem; array)
                pack(elem);
        }

        return this;
    }


    /// ditto
    Packer pack(T)(in T array) if (isAssociativeArray!(T))
    {
        if (array is null)
            return packNil();

        packMap(array.length);

        foreach (key, value; array) {
            pack(key);
            pack(value);
        }

        return this;
    }


    /// ditto
    Packer pack(T)(in T object) if (is(Unqual!T == class))
    {
        static if (!__traits(compiles, { T t; t.mp_pack(this); }))
            static assert(false, T.stringof ~ " is not MessagePackable object");

        object.mp_pack(this);

        return this;
    }


    /// ditto
    Packer pack(T)(ref T object) if (is(Unqual!T == struct))
    {
        static if (!__traits(compiles, { T t; t.mp_pack(this); }))
            static assert(false, T.stringof ~ " is not MessagePackable object");

        object.mp_pack(this);

        return this;
    }


    /**
     * Serializes type-information to buffer.
     *
     * Params:
     *  length = the length of container.
     *
     * Returns:
     *  this.
     */
    Packer packArray(in size_t length)
    {
        if (length < 16) {
            ubyte temp = Format.ARRAY | cast(ubyte)length;
            buffer_.write(take8from(temp));
        } else if (length < 65536) {
            const temp = convertEndianTo!16(length);

            store_[0] = Format.ARRAY16;
            *cast(ushort*)&store_[Offset] = temp;
            buffer_.write(store_[0..Offset + ushort.sizeof]);
        } else {
            const temp = convertEndianTo!32(length);

            store_[0] = Format.ARRAY32;
            *cast(uint*)&store_[Offset] = temp;
            buffer_.write(store_[0..Offset + uint.sizeof]);
        }

        return this;
    }


    /// ditto
    Packer packMap(in size_t length)
    {
        if (length < 16) {
            ubyte temp = Format.MAP | cast(ubyte)length;
            buffer_.write(take8from(temp));
        } else if (length < 65536) {
            const temp = convertEndianTo!16(length);

            store_[0] = Format.MAP16;
            *cast(ushort*)&store_[Offset] = temp;
            buffer_.write(store_[0..Offset + ushort.sizeof]);
        } else {
            const temp = convertEndianTo!32(length);

            store_[0] = Format.MAP32;
            *cast(uint*)&store_[Offset] = temp;
            buffer_.write(store_[0..Offset + uint.sizeof]);
        }

        return this;
    }


    /// ditto
    Packer packRaw(in size_t length)
    {
        if (length < 32) {
            ubyte temp = Format.RAW | cast(ubyte)length;
            buffer_.write(take8from(temp));
        } else if (length < 65536) {
            const temp = convertEndianTo!16(length);

            store_[0] = Format.RAW16;
            *cast(ushort*)&store_[Offset] = temp;
            buffer_.write(store_[0..Offset + ushort.sizeof]);
        } else {
            const temp = convertEndianTo!32(length);

            store_[0] = Format.RAW32;
            *cast(uint*)&store_[Offset] = temp;
            buffer_.write(store_[0..Offset + uint.sizeof]);
        }

        return this;
    }


    /**
     * Serializes the unique value.
     *
     * Returns:
     *  this.
     */
    Packer packNil()
    {
        buffer_.write(Format.NIL);
        return this;
    }


    /// ditto
    Packer packTrue()
    {
        buffer_.write(Format.TRUE);
        return this;
    }


    /// ditto
    Packer packFalse()
    {
        buffer_.write(Format.FALSE);
        return this;
    }
}


/**
 * Helper for $(D Packer) construction.
 *
 * Params:
 *  buffer = the buffer to write.
 *
 * Returns:
 *  a $(D Packer) object instantiated and initialized according to the arguments.
 */
Packer!(Buffer) packer(Buffer)(Buffer buffer)
{
    return typeof(return)(buffer);
}


version (unittest) 
{
    import msgpack.buffer;

    import std.c.string;

    mixin template DefinePacker()
    {
        SimpleBuffer buffer; Packer!(SimpleBuffer*) packer = packer(&buffer);
    }
}

unittest
{
    { // unique value
        mixin DefinePacker;

        ubyte[] result = [Format.NIL, Format.TRUE, Format.FALSE,
                                      Format.TRUE, Format.FALSE];

        packer.packNil().packTrue().packFalse().pack(true).pack(false);
        foreach (i, value; packer.buffer.data)
            assert(value == result[i]);
    }
    { // uint *
        static struct UTest { ubyte format; ulong value; }

        enum : ulong { A = ubyte.max, B = ushort.max, C = uint.max, D = ulong.max }

        static UTest[][] tests = [
            [{Format.UINT8, A}], 
            [{Format.UINT8, A}, {Format.UINT16, B}],
            [{Format.UINT8, A}, {Format.UINT16, B}, {Format.UINT32, C}],
            [{Format.UINT8, A}, {Format.UINT16, B}, {Format.UINT32, C}, {Format.UINT64, D}],
        ];

        foreach (I, T; TypeTuple!(ubyte, ushort, uint, ulong)) {
            foreach (i, test; tests[I]) {
                mixin DefinePacker;

                packer.pack(cast(T)test.value);
                assert(buffer.data[0] == test.format);

                switch (i) {
                case 0:
                    auto answer = take8from!(T.sizeof * 8)(test.value);
                    assert(memcmp(&buffer.data[1], &answer, ubyte.sizeof) == 0);
                    break;
                case 1:
                    auto answer = convertEndianTo!16(test.value);
                    assert(memcmp(&buffer.data[1], &answer, ushort.sizeof) == 0);
                    break;
                case 2:
                    auto answer = convertEndianTo!32(test.value);
                    assert(memcmp(&buffer.data[1], &answer, uint.sizeof) == 0);
                    break;
                default:
                    auto answer = convertEndianTo!64(test.value);
                    assert(memcmp(&buffer.data[1], &answer, ulong.sizeof) == 0);
                }
            }
        }
    }
    { // int *
        static struct STest { ubyte format; long value; }

        enum : long { A = byte.min, B = short.min, C = int.min, D = long.min }

        static STest[][] tests = [
            [{Format.INT8, A}], 
            [{Format.INT8, A}, {Format.INT16, B}],
            [{Format.INT8, A}, {Format.INT16, B}, {Format.INT32, C}],
            [{Format.INT8, A}, {Format.INT16, B}, {Format.INT32, C}, {Format.INT64, D}],
        ];

        foreach (I, T; TypeTuple!(byte, short, int, long)) {
            foreach (i, test; tests[I]) {
                mixin DefinePacker;

                packer.pack(cast(T)test.value);
                assert(buffer.data[0] == test.format);

                switch (i) {
                case 0:
                    auto answer = take8from!(T.sizeof * 8)(test.value);
                    assert(memcmp(&buffer.data[1], &answer, byte.sizeof) == 0);
                    break;
                case 1:
                    auto answer = convertEndianTo!16(test.value);
                    assert(memcmp(&buffer.data[1], &answer, short.sizeof) == 0);
                    break;
                case 2:
                    auto answer = convertEndianTo!32(test.value);
                    assert(memcmp(&buffer.data[1], &answer, int.sizeof) == 0);
                    break;
                default:
                    auto answer = convertEndianTo!64(test.value);
                    assert(memcmp(&buffer.data[1], &answer, long.sizeof) == 0);
                }
            }
        }
    }
    { // fload, double
        static struct FTest { ubyte format; double value; }
        static union _f { float  f; uint  i; }
        static union _d { double f; ulong i; }

        static FTest[] tests = [
            {Format.FLOAT,  float.min},
            {Format.DOUBLE, double.max},
        ];

        foreach (I, T; TypeTuple!(float, double)) {
            mixin DefinePacker;

            packer.pack(cast(T)tests[I].value);
            assert(buffer.data[0] == tests[I].format);

            static if (I) {
                const answer = convertEndianTo!64(_d(cast(T)tests[I].value).i);
                assert(memcmp(&buffer.data[1], &answer, double.sizeof) == 0);
            } else {
                const answer = convertEndianTo!32(_f(cast(T)tests[I].value).i);
                assert(memcmp(&buffer.data[1], &answer, float.sizeof) == 0);                
            }
        }
    }
    { // container
        static struct Test { ubyte format; size_t value; }

        enum : ulong { A = 16 / 2, B = ushort.max, C = uint.max }

        static Test[][] tests = [
            [{Format.ARRAY | A, Format.ARRAY | A}, {Format.ARRAY16, B}, {Format.ARRAY32, C}],
            [{Format.MAP   | A, Format.MAP   | A}, {Format.MAP16,   B}, {Format.MAP32,   C}],
            [{Format.RAW   | A, Format.RAW   | A}, {Format.RAW16,   B}, {Format.RAW32,   C}],
        ];

        foreach (I, Name; TypeTuple!("Array", "Map", "Raw")) {
            auto test = tests[I];

            foreach (i, T; TypeTuple!(ubyte, ushort, uint)) {
                mixin DefinePacker; 
                mixin("packer.pack" ~ Name ~ "(i ? test[i].value : A);");

                assert(buffer.data[0] == test[i].format);

                switch (i) {
                case 0:
                    auto answer = take8from(test[i].value);
                    assert(memcmp(&buffer.data[0], &answer, ubyte.sizeof) == 0);
                    break;
                case 1:
                    auto answer = convertEndianTo!16(test[i].value);
                    assert(memcmp(&buffer.data[1], &answer, ushort.sizeof) == 0);
                    break;
                default:
                    auto answer = convertEndianTo!32(test[i].value);
                    assert(memcmp(&buffer.data[1], &answer, uint.sizeof) == 0);
                }
            }
        }
    }
    { // user defined
        {
            static struct S
            {
                uint num = uint.max;

                void mp_pack(P)(ref P p) const { p.packArray(1); p.pack(num); }
            }

            mixin DefinePacker; S test;

            packer.pack(test);

            assert(buffer.data[0] == (Format.ARRAY | 1));
            assert(buffer.data[1] ==  Format.UINT32);
            assert(memcmp(&buffer.data[2], &test.num, uint.sizeof) == 0);
        }
        {
            static class C
            {
                uint num;

                this(uint n) { num = n; }

                void mp_pack(P)(ref P p) const { p.packArray(1); p.pack(num); }
            }

            mixin DefinePacker; C test = new C(ushort.max);

            packer.pack(test);

            assert(buffer.data[0] == (Format.ARRAY | 1));
            assert(buffer.data[1] ==  Format.UINT16);
            assert(memcmp(&buffer.data[2], &test.num, ushort.sizeof) == 0);
        }
    }
}


private:


template isByte(T)
{
    enum isByte = staticIndexOf!(Unqual!(T), byte, ubyte) >= 0;
}


unittest
{
    static assert(isByte!(byte));
    static assert(isByte!(ubyte));
    static assert(!isByte!(short));
    static assert(!isByte!(ulong));
    static assert(!isByte!(char));
    static assert(!isByte!(string));
}
