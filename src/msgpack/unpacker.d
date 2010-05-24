// Written in the D programming language.

/**
 * MessagePack for D, deserializing routine
 *
 * ToDo:
 *  Currently, Unpacker uses internal buffer.
 *  Uses stream if Phobos will have truly stream module.
 *
 * Copyright: Copyright Masahiro Nakagawa 2010.
 * License:   <a href = "http://www.boost.org/LICENSE_1_0.txt">Boost License 1.0</a>.
 * Authors:   Masahiro Nakagawa
 */
module msgpack.unpacker;

public import msgpack.object;

import std.array;  // for Range
import std.traits;

import msgpack.common;

version(unittest) import std.typetuple, std.typecons, msgpack.packer, msgpack.buffer;


@trusted:


/**
 * $(D UnpackException) is thrown on parse error
 */
class UnpackException : Exception
{
    this(string message)
    { 
        super(message);
    }
}


/**
 * Internal buffer and related operations for Unpacker
 *
 * Following Unpackers mixin this template.
 *
 * -----
 * //buffer image:
 * +-------------------------------------------+
 * | [object] | [obj | unparsed... | unused... |
 * +-------------------------------------------+
 *            ^ offset
 *                   ^ current
 *                                 ^ used
 *                                             ^ buffer.length
 * -----
 */
mixin template InternalBuffer()
{
  private:
    ubyte[] buffer_;  // internal buffer
    size_t  used_;    // index that buffer cosumed
    size_t  offset_;  // index that buffer parsed
    size_t  parsed_;  // total size of parsed message
    bool    hasRaw_;  // indicates whether Raw object has been deserialized


  public:
    /**
     * Forwards to internal buffer.
     *
     * Returns:
     *  the reference of internal buffer.
     */
    @property nothrow ubyte[] buffer()
    {
        return buffer_;
    }


    /**
     * Fills internal buffer with $(D_PARAM target).
     *
     * Params:
     *  target = new serialized buffer to deserialize.
     */
    void feed(in ubyte[] target)
    in
    {
        assert(target.length);
    }
    body
    {
        /*
         * Expands internal buffer.
         *
         * Params:
         *  size = new buffer size to append.
         */
        void expandBuffer(in size_t size)
        {
            // rewinds buffer(completed deserialization)
            if (used_ == offset_ && !hasRaw_) {
                used_ =  offset_ = 0;

                if (buffer_.length < size)
                    buffer_.length = size;

                return;
            }

            // deserializing state is mid-flow(buffer has non-parsed data yet)
            auto unparsed = buffer_[offset_..used_];
            auto restSize = buffer_.length - used_ + offset_;
            auto newSize  = size > restSize ? unparsedSize + size : buffer_.length;

            if (hasRaw_) {
                hasRaw_ = false;
                buffer_ = new ubyte[](newSize);
            } else {
                buffer_.length = newSize;

                // avoids overlapping copy
                auto area = buffer_[0..unparsedSize];
                unparsed  = area.overlap(unparsed) ? unparsed.dup : unparsed;
            }

            buffer_[0..unparsedSize] = unparsed;
            used_   = unparsedSize;
            offset_ = 0;
        }

        const size = target.length;

        // lacks current buffer?
        if (buffer_.length - used_ < size)
            expandBuffer(size);

        buffer_[used_..used_ + size] = target;
        used_ += size;
    }


    /**
     * Consumes buffer. This method is helper for buffer property.
     * You must use this method if you write bytes to buffer directly.
     *
     * Params:
     *  size = the number of consuming.
     */
    nothrow void bufferConsumed(in size_t size)
    {
        if (used_ + size > buffer_.length)
            used_ = buffer_.length;
        else
            used_ += size;
    }


    /**
     * Removes unparsed buffer.
     */
    nothrow void removeUnparsed()
    {
        used_ = offset_;
    }


    /**
     * Returns:
     *  the total size including unparsed buffer size.
     */
    @property nothrow size_t size() const
    {
        return parsed_ - offset_ + used_;
    }


    /**
     * Returns:
     *  the parsed size of buffer.
     */
    @property nothrow size_t parsedSize() const
    {
        return parsed_;
    }


    /**
     * Returns:
     *  the unparsed size of buffer.
     */
    @property nothrow size_t unparsedSize() const
    {
        return used_ - offset_;
    }


  private:
    /**
     * Initializes buffer.
     *
     * Params:
     *  target     = byte buffer to deserialize
     *  bufferSize = size limit of buffer size
     */
    void initializeBuffer(in ubyte[] target, in size_t bufferSize = 8192)
    {
        const size = target.length;

        buffer_ = new ubyte[](size > bufferSize ? size : bufferSize); 
        used_   = size;
        buffer_[0..size] = target;
    }
}


/**
 * This $(D Unpacker) is a $(D MessagePack) direct-conversion deserializer
 *
 * This implementation is suitable for fixed data.
 *
 * Example:
 * -----
 * // serializedData is [10, 0.1, false]
 * auto unpacker = unpacker!(false)(serializedData);
 *
 * // manually
 * uint   n;
 * double d;
 * bool   b;
 *
 * auto size = unpacker.unpackArray();
 * if (size != 3)
 *     throw new Exception("Size is mismatched!");
 *
 * unpacker.unpack(n).unpack(d).unpack(b); // or unpack(n, d, b)
 *
 * // or
 * Tuple!(uint, double, true) record;
 * unpacker.unpack(record);  // record is [10, 0.1, false]
 * -----
 */
struct Unpacker(bool isStream : false)
{
  private:
    enum Offset = 1;

    mixin InternalBuffer;


  public:
    /**
     * Constructs a $(D Unpacker).
     *
     * Params:
     *  target     = byte buffer to deserialize
     *  bufferSize = size limit of buffer size
     */
    this(in ubyte[] target, in size_t bufferSize = 8192)
    {
        initializeBuffer(target, bufferSize);
    }


    /**
     * Clears some states for next deserialization.
     */
    nothrow void clear()
    {
        parsed_ = 0;
    }


    /**
     * Deserializes $(D_PARAM T) object and assigns to $(D_PARAM value).
     *
     * Params:
     *  value = the reference of value to assign.
     *
     * Returns:
     *  this to method chain.
     *
     * Throws:
     *  UnpackException when doesn't read from buffer or precision loss occurs and
     *  InvalidTypeException when $(D_PARAM T) type doesn't match serialized type.
     */
    ref Unpacker unpack(T)(ref T value) if (is(Unqual!T == bool))
    {
        canRead(Offset, 0);
        const header = read();

        switch (header) {
        case Format.TRUE:
            value = true;
            break;
        case Format.FALSE:
            value = false;
            break;
        default:
            rollback(0);
        }

        return this;
    }


    /// ditto
    ref Unpacker unpack(T)(ref T value) if (isUnsigned!(Unqual!T))
    {
        canRead(Offset, 0);
        const header = read();

        if (0x00 <= header && header <= 0x7f) {
            value = header;
        } else {
            switch (header) {
            case Format.UINT8:
                canRead(ubyte.sizeof);
                value = read();
                break;
            case Format.UINT16:
                canRead(ushort.sizeof);
                auto us = load16To!ushort(read(ushort.sizeof));
                if (us > T.max)
                    rollback(ushort.sizeof);
                value = cast(T)us;
                break;
            case Format.UINT32:
                canRead(uint.sizeof);
                auto ui = load32To!uint(read(uint.sizeof));
                if (ui > T.max)
                    rollback(uint.sizeof);
                value = cast(T)ui;
                break;
            case Format.UINT64:
                canRead(ulong.sizeof);
                auto ul = load64To!ulong(read(ulong.sizeof));
                if (ul > T.max)
                    rollback(ulong.sizeof);
                value = cast(T)ul;
                break;
            default:
                rollback(0);
            }
        }

        return this;
    }


    /// ditto
    ref Unpacker unpack(T)(ref T value) if (isSigned!(Unqual!T) && !isFloatingPoint!(Unqual!T))
    {
        canRead(Offset, 0);
        const header = read();

        if ((0x00 <= header && header <= 0x7f) || (0xe0 <= header && header <= 0xff)) {
            value = cast(T)header;
        } else {
            switch (header) {
            case Format.UINT8:
                canRead(ubyte.sizeof);
                auto ub = read();
                if (ub > T.max)
                    rollback(ubyte.sizeof);
                value = cast(T)ub;
                break;
            case Format.UINT16:
                canRead(ushort.sizeof);
                auto us = load16To!ushort(read(ushort.sizeof));
                if (us > T.max)
                    rollback(ushort.sizeof);
                value = cast(T)us;
                break;
            case Format.UINT32:
                canRead(uint.sizeof);
                auto ui = load32To!uint(read(uint.sizeof));
                if (ui > T.max)
                    rollback(uint.sizeof);
                value = cast(T)ui;
                break;
            case Format.UINT64:
                canRead(ulong.sizeof);
                auto ul = load64To!ulong(read(ulong.sizeof));
                if (ul > T.max)
                    rollback(ulong.sizeof);
                value = cast(T)ul;
                break;
            case Format.INT8:
                canRead(byte.sizeof);
                value = cast(byte)read();
                break;
            case Format.INT16:
                canRead(short.sizeof);
                auto s = load16To!short(read(short.sizeof));
                if (s < T.min || T.max < s)
                    rollback(short.sizeof);
                value = cast(T)s;
                break;
            case Format.INT32:
                canRead(int.sizeof);
                auto i = load32To!int(read(int.sizeof));
                if (i < T.min || T.max < i)
                    rollback(int.sizeof);
                value = cast(T)i;
                break;
            case Format.INT64:
                canRead(long.sizeof);
                auto l = load64To!long(read(long.sizeof));
                if (l < T.min || T.max < l)
                    rollback(long.sizeof);
                value = cast(T)l;
                break;
            default:
                rollback(0);
            }
        }

        return this;
    }


    /// ditto
    ref Unpacker unpack(T)(ref T value) if (isFloatingPoint!(Unqual!T))
    {
        canRead(Offset, 0);
        const header = read();

        switch (header) {
        case Format.FLOAT:
            _f temp;

            canRead(uint.sizeof);
            temp.i = load32To!uint(read(uint.sizeof));
            value  = cast(T)temp.f;
            break;
        case Format.DOUBLE:
            // check precision loss
            static if (is(Unqual!T == float))
                rollback(0);

            _d temp;

            canRead(ulong.sizeof);
            temp.i = load64To!ulong(read(ulong.sizeof));
            value  = cast(T)temp.f;
            break;
        case Format.REAL:
            // check precision loss
            static if (is(Unqual!T == float) || is(Unqual!T == double))
                rollback(0);

            canRead(ubyte.sizeof);
            if (read() != real.sizeof)
                throw new UnpackException("Real type on this environment is different from serialized real type.");

            _r temp;

            canRead(_r.sizeof);
            temp.fraction = load64To!(typeof(temp.fraction))(read(temp.fraction.sizeof));
            mixin("temp.exponent = load" ~ ES.stringof[0..2] ~ // delete u suffix
                  "To!(typeof(temp.exponent))(read(temp.exponent.sizeof));");
            value = temp.f;
            break;
        default:
            rollback(0);
        }

        return this;
    }


    /**
     * Deserializes $(D_PARAM T) object and assigns to $(D_PARAM array).
     *
     * This is convenient method for array deserialization.
     * Rollback will be successful if you deserialize raw type(ubyte[] or string).
     * But rollback wiil be unsuccessful if you deserialize other type(int[], double[int], etc..)
     *
     * No assign if the length of deserialized object is 0.
     *
     * In a static array, this method checks length. Rollbacks and throws exception
     * if length of $(D_PARAM array) is different from length of deserialized object.
     *
     * Params:
     *  array = the reference of array to assign.
     *
     * Returns:
     *  this to method chain.
     *
     * Throws:
     *  UnpackException when doesn't read from buffer or precision loss occurs and
     *  InvalidTypeException when $(D_PARAM T) type doesn't match serialized type.
     */
    ref Unpacker unpack(T)(ref T array) if (isArray!T)
    {
        alias typeof(T.init[0]) U;

        // Raw bytes
        static if (isByte!U || isSomeChar!U) {
            auto length = unpackRaw();
            auto offset = (length < 32 ? 0 : length < 65536 ? ushort.sizeof : uint.sizeof);
            if (length == 0)
                return this;

            static if (isStaticArray!T) {
                if (length != array.length)
                    rollback(offset);
            }

            canRead(length, offset + Offset);
            array = cast(T)read(length);

            static if (isDynamicArray!T)
                hasRaw_ = true;
        } else {
            auto length = unpackArray();
            if (length == 0)
                return this;

            static if (isStaticArray!T) {
                if (length != array.length)
                    rollback(length < 16 ? 0 : length < 65536 ? ushort.sizeof : uint.sizeof);
            } else {
                array.length = length;
            }

            foreach (i; 0..length)
                unpack(array[i]);
        }

        return this;
    }


    /// ditto
    ref Unpacker unpack(T)(ref T array) if (isAssociativeArray!T)
    {
        alias typeof(T.init.keys[0])   K;
        alias typeof(T.init.values[0]) V;

        auto length = unpackMap();
        if (length == 0)
            return this;

        foreach (i; 0..length) {
            K k; unpack(k);
            V v; unpack(v);
            array[k] = v;
        }

        return this;
    }


    /**
     * Deserializes $(D_PARAM T) object and assigns to $(D_PARAM array).
     *
     * $(D_KEYWORD struct) and $(D_KEYWORD class) need to implement $(D mp_unpack) method.
     * $(D mp_unpack) signature is:
     * -----
     * void mp_unpack(ref Unpacker unpacker)
     * -----
     * Assumes $(D std.typecons.Tuple) if $(D_KEYWORD struct) doens't implement $(D mp_unpack).
     * Checks length if $(D_PARAM T) is a $(D std.typecons.Tuple).
     *
     * Params:
     *  array = the reference of array to assign.
     *  args  = the arguments to class constructor(class only).
     *          This is used at new statement if $(D_PARAM object) is $(D_KEYWORD null).
     *
     * Returns:
     *  this to method chain.
     */
    template unpack(T, Args...) if (is(Unqual!T == class))
    {
        ref Unpacker unpack(ref T object, auto ref Args args)
        {
            static if (!__traits(compiles, { T t; t.mp_unpack(this); }))
                static assert(false, T.stringof ~ " is not a MessagePackable object");

            if (object is null)
                object = new T(args);

            object.mp_unpack(this);

            return this;
        }
    }
    /*
     * @@@BUG@@@ http://d.puremagic.com/issues/show_bug.cgi?id=2460
    ref Unpacker unpack(T, Args...)(ref T object, auto ref Args args) if (is(Unqual!T == class))
    { // do stuff }
    */


    /// ditto
    ref Unpacker unpack(T)(ref T object) if (is(Unqual!T == struct))
    {
        static if (__traits(compiles, { T t; t.mp_unpack(this); })) {
            object.mp_unpack(this);
        } else {
            auto length = unpackArray();
            if (length == 0)
                return this;

            if (length != T.Types.length)
                rollback(length < 16 ? 0 : length < 65536 ? ushort.sizeof : uint.sizeof);

            foreach (i, Type; T.Types)
                unpack(object.field[i]);
        }

        return this;
    }


    /**
     * Deserializes $(D_PARAM Types) objects and assigns to each object.
     *
     * Params:
     *  objects = the references of objects to assign.
     *
     * Returns:
     *  this to method chain.
     */
    template unpack(Types...) if (Types.length > 1)
    {
        ref Unpacker unpack(ref Types objects)
        {
            foreach (i, T; Types)
                unpack!(T)(objects[i]);

            return this;
        }
    }
    /*
     * @@@BUG@@@ http://d.puremagic.com/issues/show_bug.cgi?id=2460
    ref Unpacker unpack(Types...)(ref Types objects) if (Types.length > 1)
    { // do stuff }
     */


    /**
     * Deserializes type-information of container.
     *
     * Returns:
     *  the container size.
     */
    size_t unpackArray()
    {
        canRead(Offset, 0);
        const  header = read();
        size_t length;

        if (0x90 <= header && header <= 0x9f) {
            length = header & 0x0f;
        } else {
            switch (header) {
            case Format.ARRAY16:
                canRead(ushort.sizeof);
                length = load16To!size_t(read(ushort.sizeof));
                break;
            case Format.ARRAY32:
                canRead(uint.sizeof);
                length = load32To!size_t(read(uint.sizeof));
                break;
            case Format.NIL:
                break;
            default:
                rollback(0);
            }
        }

        return length;
    }


    /// ditto
    size_t unpackMap()
    {
        canRead(Offset, 0);
        const  header = read();
        size_t length;

        if (0x80 <= header && header <= 0x8f) {
            length = header & 0x0f;
        } else {
            switch (header) {
            case Format.MAP16:
                canRead(ushort.sizeof);
                length = load16To!size_t(read(ushort.sizeof));
                break;
            case Format.MAP32:
                canRead(uint.sizeof);
                length = load32To!size_t(read(uint.sizeof));
                break;
            case Format.NIL:
                break;
            default:
                rollback(0);
            }
        }

        return length;
    }


    /// ditto
    size_t unpackRaw()
    {
        canRead(Offset, 0);
        const  header = read();
        size_t length;

        if (0xa0 <= header && header <= 0xbf) {
            length = header & 0x1f;
        } else {
            switch (header) {
            case Format.RAW16:
                canRead(ushort.sizeof);
                length = load16To!size_t(read(ushort.sizeof));
                break;
            case Format.RAW32:
                canRead(uint.sizeof);
                length = load32To!size_t(read(uint.sizeof));
                break;
            case Format.NIL:
                break;
            default:
                rollback(0);
            }
        }

        return length;
    }


    /**
     * Deserializes nil object and assigns to $(D_PARAM value).
     *
     * Params:
     *  value = the reference of value to assign.
     *
     * Returns:
     *  this to method chain.
     *
     * Throws:
     *  UnpackException when doesn't read from buffer or precision loss occurs and
     *  InvalidTypeException when $(D_PARAM T) type doesn't match serialized type.
     */
    ref Unpacker unpackNil(T)(ref T value)
    {
        canRead(Offset, 0);
        const header = read();

        if (header == Format.NIL)
            value = null;
        else
            rollback(0);

        return this;
    }


    /**
     * Scans an entire buffer and converts each objects.
     *
     * This method is used for unpacking record-like objects.
     *
     * Example:
     * -----
     * // serialized data is "[1, 2][3, 4][5, 6][...".
     * auto unpacker = unpacker!(false)(serializedData);
     * foreach (n, d; &unpacker.scan!(int, int))  // == "foreach (int n, int d; unpacker)"
     *     writeln(n, d); // 1st loop "1, 2", 2nd loop "3, 4"...
     * -----
     */
    int scan(Types...)(scope int delegate(ref Types) dg)
    {
        return opApply!(Types)(delegate int(ref Types objects) { return dg(objects); });
    }


    /// ditto
    int opApply(Types...)(scope int delegate(ref Types) dg)
    {
        int result;

        while (used_ - offset_) {
            auto length = unpackArray();
            if (length != Types.length)
                rollback(length < 16 ? 0 : length < 65536 ? ushort.sizeof : uint.sizeof);

            Types objects;
            foreach (i, T; Types)
                unpack!(T)(objects[i]);

            result = dg(objects);
            if (result)
                return result;
        }

        return result;
    }


  private:
    /*
     * Reading test to buffer.
     *
     * Params:
     *  size   = the size to read.
     *  offset = the offset to subtract when doesn't read from buffer.
     *
     * Throws:
     *  UnpackException when doesn't read from buffer.
     */
    void canRead(in size_t size, in size_t offset = 1)
    {
        if (used_ - offset_ < size) {
            if (offset)
                offset_ -= offset;

            throw new UnpackException("Insufficient buffer");
        }
    }


    /*
     * Reads value from buffer and advances offset.
     */
    ubyte read()
    {
        return buffer_[offset_++];
    }


    /*
     * Reads value from buffer and advances offset.
     */
    ubyte[] read(in size_t size)
    {
        auto result = buffer_[offset_..offset_ + size];

        offset_ += size;

        return result;
    }


    /*
     * Rollbacks offset and throws exception.
     */
    void rollback(in size_t size)
    {
        offset_ -= size + Offset;
        onInvalidType();
    }
}


unittest
{
    { // unique
        mixin DefinePacker;

        Tuple!(bool, bool) result, test = tuple(true, false);

        packer.pack(test);

        auto unpacker = unpacker!(false)(packer.buffer.data);
        unpacker.unpack(result);

        assert(test == result);
    }
    { // uint *
        mixin DefinePacker;

        Tuple!(ubyte, ushort, uint, ulong) result,
            test = tuple(cast(ubyte)ubyte.max, cast(ushort)ushort.max,
                         cast(uint)uint.max,   cast(ulong)ulong.max);

        packer.pack(test);

        auto unpacker = unpacker!(false)(packer.buffer.data);
        unpacker.unpack(result);

        assert(test == result);
    }
    { // int *
        mixin DefinePacker;

        Tuple!(byte, short, int, long) result,
            test = tuple(cast(byte)byte.min, cast(short)short.min,
                         cast(int)int.min,   cast(long)long.min);

        packer.pack(test);

        auto unpacker = unpacker!(false)(packer.buffer.data);
        unpacker.unpack(result);

        assert(test == result);
    }
    { // floating point
        mixin DefinePacker;

        static if (real.sizeof == double.sizeof)
            Tuple!(float, double, double) result,
                test = tuple(cast(float)float.min, cast(double)double.max, cast(real)real.min);
        else
            Tuple!(float, double, real) result,
                test = tuple(cast(float)float.min, cast(double)double.max, cast(real)real.min);

        packer.pack(test);

        auto unpacker = unpacker!(false)(packer.buffer.data);
        unpacker.unpack(result);

        assert(test == result);
    }
    { // container
        mixin DefinePacker;

        Tuple!(ulong[], double[uint], string, bool[2]) result,
            test = tuple([1UL, 2], [3U:4.0, 5:6.0, 7:8.0],
                         "MessagePack is nice!", [true, false]);

        packer.pack(test);

        auto unpacker = unpacker!(false)(packer.buffer.data);
        unpacker.unpack(result);

        assert(test == result);
    }
    { // user defined
        {
            static struct S
            {
                uint num;

                void mp_pack(P)(ref P p) const { p.packArray(1); p.pack(num); }
                void mp_unpack(ref Unpacker!(false) u)
                { 
                    assert(u.unpackArray == 1);
                    u.unpack(num);
                }
            }

            mixin DefinePacker; S result, test = S(uint.max);

            packer.pack(test);

            auto unpacker = unpacker!(false)(packer.buffer.data);
            unpacker.unpack(result);

            assert(test.num == result.num);
        }
        {
            static class C
            {
                uint num;

                this(uint n) { num = n; }

                void mp_pack(P)(ref P p) const { p.packArray(1); p.pack(num - 1); }
                void mp_unpack(ref Unpacker!(false) u)
                {
                    assert(u.unpackArray == 1);
                    u.unpack(num);
                }
            }

            mixin DefinePacker; C result, test = new C(ushort.max);

            packer.pack(test);

            auto unpacker = unpacker!(false)(packer.buffer.data);
            unpacker.unpack(result, ushort.max);

            assert(test.num == result.num + 1);
        }
    }
    { // variadic
        mixin DefinePacker;

        Tuple!(uint, long, double) test = tuple(uint.max, long.min, double.max);

        packer.pack(test);

        auto unpacker = unpacker!(false)(packer.buffer.data);

        uint u; long l; double d;

        auto size = unpacker.unpackArray();
        unpacker.unpack(u, l, d);

        assert(test == tuple(u, l, d));
    }
    { // scan / opApply
        ubyte[] data;

        foreach (i; 0..2) {
            mixin DefinePacker;
            packer.pack(tuple(1, 0.5, "Hi!"));
            data ~= packer.buffer.data;
        }

        foreach (n, d, s; &unpacker!(false)(data).scan!(int, double, string)) {
            assert(n == 1);
            assert(d == 0.5);
            assert(s == "Hi!");
        }
    }
}


/**
 * $(D Unpacked) is a $(D InputRange) wrapper for stream deserialization result
 */
struct Unpacked
{
    mp_Object object;

    alias object this;


    /**
     * Constructs a $(D Unpacked) with argument.
     *
     * Params:
     *  object = a deserialized object.
     */
    this(ref mp_Object object)
    {
        this.object = object;
    }


    /**
     * Range primitive operation that checks iteration state.
     *
     * Returns:
     *  true if there are no more elements to be iterated.
     */
    @property nothrow bool empty() const  // std.array.empty isn't nothrow function
    {
        return (object.type == mp_Type.ARRAY) && !object.via.array.length;
    }


    /**
     * Range primitive operation that returns the currently iterated element.
     *
     * Returns:
     *  the deserialized $(D mp_Object).
     */
    @property ref mp_Object front()
    {
        return object.via.array.front;
    }


    /**
     * Range primitive operation that advances the range to its next element.
     */
    void popFront()
    {
        object.via.array.popFront();
    }
}


/**
 * This $(D Unpacker) is a $(D MessagePack) stream deserializer
 *
 * This implementation enables you to load multiple objects from a stream(like network).
 *
 * Example:
 * -----
 * ...
 * auto unpacker = unpacker(serializedData);
 * ...
 *
 * // appends new data to buffer if pre execute() call didn't finish deserialization.
 * unpacker.feed(newSerializedData);
 *
 * while(unpacker.execute()) {
 *     foreach (obj; unpacker.purge()) {
 *         // do stuff
 *     }
 * }
 * 
 * if (unpacker.size)
 *     throw new Exception("Message is too large");
 * -----
 */
struct Unpacker(bool isStream : true)
{
  private:
    /*
     * Context state of deserialization
     */
    enum State
    {
        HEADER = 0x00,

        // Floating point, Unsigned, Signed interger (== header & 0x03)
        FLOAT = 0x0a,
        DOUBLE,
        UINT8,
        UINT16,
        UINT32,
        UINT64,
        INT8,
        INT16,
        INT32,
        INT64,

        // Container (== header & 0x01)
        RAW16 = 0x1a,
        RAW32,
        ARRAY16,
        ARRAY36,
        MAP16,
        MAP32,
        RAW,

        // D-specific type
        REAL
    }


    /*
     * Element type of container
     */
    enum ContainerElement
    {
        ARRAY_ITEM,
        MAP_KEY,
        MAP_VALUE
    }


    /*
     * Internal stack context
     */
    static struct Context
    {
        static struct Container
        {
            ContainerElement type;    // object container type
            mp_Object        object;  // current object
            mp_Object        key;     // for map object
            size_t           count;   // container length
        }

        State       state;  // current state of deserialization
        size_t      trail;  // current deserializing size
        size_t      top;    // current index of stack
        Container[] stack;  // storing objects
    }

    Context context_;  // stack environment for streaming deserialization

    mixin InternalBuffer;


  public:
    /**
     * Constructs a $(D Unpacker).
     *
     * Params:
     *  target     = byte buffer to deserialize
     *  bufferSize = size limit of buffer size
     */
    this(in ubyte[] target, in size_t bufferSize = 8192)
    {
        initializeBuffer(target, bufferSize);
        initializeContext();
    }


    /**
     * Forwards to deserialized object.
     *
     * Returns:
     *  the $(D Unpacked) object contains deserialized object.
     */
    @property Unpacked unpacked()
    {
        return Unpacked(context_.stack[0].object);
    }


    /**
     * Clears some states for next deserialization.
     */
    nothrow void clear()
    {
        initializeContext();

        parsed_ = 0;
    }


    /**
     * Convenient method for unpacking and clearing states.
     *
     * Example:
     * -----
     * foreach (obj; unpacker.purge()) {
     *     // do stuff
     * }
     * -----
     * is equivalent to
     * -----
     * foreach (obj; unpacker.unpacked) {
     *     // do stuff
     * }
     * unpacker.clear();
     * -----
     *
     * Returns:
     *  the $(D Unpacked) object contains deserialized object.
     */
    Unpacked purge()
    {
        auto result = Unpacked(context_.stack[0].object);

        clear();

        return result;
    }


    /**
     * Executes deserialization.
     *
     * Returns:
     *  true if deserialization has been completed, otherwise false.
     *
     * Throws:
     *  $(D UnpackException) when parse error occurs.
     */
    bool execute()
    {
        /*
         * Current implementation is very dirty(goto! goto!! goto!!!).
         * This Complexity for performance(avoid function call).
         */

        bool      ret;
        size_t    cur = offset_;
        mp_Object obj;

        // restores before state
        auto state =  context_.state;
        auto trail =  context_.trail;
        auto top   =  context_.top;
        auto stack = &context_.stack;

        /*
         * Helper for container deserialization
         */
        bool startContainer(string Type)(ContainerElement type, size_t length)
        {
            mixin("callback" ~ Type ~ "((*stack)[top].object, length);");

            if (length == 0)
                return false;

            (*stack)[top].type  = type;
            (*stack)[top].count = length;
            (*stack).length     = ++top + 1;

            return true;
        }

        // non-deserialized data is nothing
        if (used_ - offset_ == 0)
            goto Labort;

        do {
          Lstart:
            if (state == State.HEADER) {
                const header = buffer_[cur];

                if (0x00 <= header && header <= 0x7f) {         // positive
                    callbackUInt(obj, header);
                    goto Lpush;
                } else if (0xe0 <= header && header <= 0xff) {  // negative
                    callbackInt(obj, cast(byte)header);
                    goto Lpush;
                } else if (0xa0 <= header && header <= 0xbf) {  // fix raw
                    trail = header & 0x1f;
                    if (trail == 0)
                        goto Lraw;
                    state = State.RAW;
                    cur++;
                    goto Lstart;
                } else if (0x90 <= header && header <= 0x9f) {  // fix array
                    if (!startContainer!"Array"(ContainerElement.ARRAY_ITEM, header & 0x0f))
                        goto Lpush;
                    goto Lagain;
                } else if (0x80 <= header && header <= 0x8f) {  // fix map
                    if (!startContainer!"Map"(ContainerElement.MAP_KEY, header & 0x0f))
                        goto Lpush;
                    goto Lagain;
                } else {
                    switch (header) {
                    case Format.UINT8:
                    case Format.UINT16:
                    case Format.UINT32:
                    case Format.UINT64:
                    case Format.INT8:
                    case Format.INT16:
                    case Format.INT32:
                    case Format.INT64:
                    case Format.FLOAT:
                    case Format.DOUBLE:
                        trail = 1 << (header & 0x03); // computes object size
                        state = cast(State)(header & 0x1f);
                        break;
                    case Format.REAL:
                        const realSize = buffer_[++cur];
                        if (realSize == real.sizeof) {
                            trail = real.sizeof;
                            state = State.REAL;
                        } else {
                            throw new UnpackException("Real type on this environment is different from serialized real type.");
                        }
                        break;
                    case Format.ARRAY16:
                    case Format.ARRAY32:
                    case Format.MAP16:
                    case Format.MAP32:
                    case Format.RAW16:
                    case Format.RAW32:
                        trail = 2 << (header & 0x01);  // computes container size
                        state = cast(State)(header & 0x1f);
                        break;
                    case Format.NIL:
                        callbackNil(obj);
                        goto Lpush;
                    case Format.TRUE:
                        callbackBool(obj, true);
                        goto Lpush;
                    case Format.FALSE:
                        callbackBool(obj, false);
                        goto Lpush;
                    default:
                        onUnknownType();
                    }

                    cur++;
                    goto Lstart;
                }
            } else {
                // data lack for deserialization
                if (used_ - cur < trail)
                    goto Labort;

                const base = cur; cur += trail - 1;  // fix current position

                final switch (state) {
                case State.FLOAT:
                    _f temp;

                    temp.i = load32To!uint(buffer_[base..base + trail]);
                    callbackFloat(obj, temp.f);
                    goto Lpush;
                case State.DOUBLE:
                    _d temp;

                    temp.i = load64To!ulong(buffer_[base..base + trail]);
                    callbackFloat(obj, temp.f);
                    goto Lpush;
                case State.REAL:
                    _r temp; const expb = base + temp.fraction.sizeof;

                    temp.fraction = load64To!(typeof(temp.fraction))(buffer_[base..expb]);
                    mixin("temp.exponent = load" ~ ES.stringof[0..2] ~ // delete u suffix
                          "To!(typeof(temp.exponent))(buffer_[expb..expb + temp.exponent.sizeof]);");
                    callbackFloat(obj, temp.f);
                    goto Lpush;
                case State.UINT8:
                    callbackUInt(obj, buffer_[base]);
                    goto Lpush;
                case State.UINT16:
                    callbackUInt(obj, load16To!ulong(buffer_[base..base + trail]));
                    goto Lpush;
                case State.UINT32:
                    callbackUInt(obj, load32To!ulong(buffer_[base..base + trail]));
                    goto Lpush;
                case State.UINT64:
                    callbackUInt(obj, load64To!ulong(buffer_[base..base + trail]));
                    goto Lpush;
                case State.INT8:
                    callbackInt(obj, cast(byte)buffer_[base]);
                    goto Lpush;
                case State.INT16:
                    callbackInt(obj, load16To!long(buffer_[base..base + trail]));
                    goto Lpush;
                case State.INT32:
                    callbackInt(obj, load32To!long(buffer_[base..base + trail]));
                    goto Lpush;
                case State.INT64:
                    callbackInt(obj, load64To!long(buffer_[base..base + trail]));
                    goto Lpush;
                case State.RAW: Lraw:
                    hasRaw_ = true;
                    callbackRaw(obj, buffer_[base..base + trail]);
                    goto Lpush;
                case State.RAW16:
                    trail = load16To!size_t(buffer_[base..base + trail]);
                    if (trail == 0)
                        goto Lraw;
                    state = State.RAW;
                    cur++;
                    goto Lstart;
                case State.RAW32:
                    trail = load32To!size_t(buffer_[base..base + trail]);
                    if (trail == 0)
                        goto Lraw;
                    state = State.RAW;
                    cur++;
                    goto Lstart;
                case State.ARRAY16:
                    if (!startContainer!"Array"(ContainerElement.ARRAY_ITEM,
                                                load16To!size_t(buffer_[base..base + trail])))
                        goto Lpush;
                    goto Lagain;
                case State.ARRAY36:
                    if (!startContainer!"Array"(ContainerElement.ARRAY_ITEM,
                                                load32To!size_t(buffer_[base..base + trail])))
                        goto Lpush;
                    goto Lagain;
                case State.MAP16:
                    if (!startContainer!"Map"(ContainerElement.MAP_KEY,
                                              load16To!size_t(buffer_[base..base + trail])))
                        goto Lpush;
                    goto Lagain;
                case State.MAP32:
                    if (!startContainer!"Map"(ContainerElement.MAP_KEY,
                                              load32To!size_t(buffer_[base..base + trail])))
                        goto Lpush;
                    goto Lagain;
                case State.HEADER:
                    break;
                }
            }

          Lpush:
            if (top == 0)
                goto Lfinish;

            auto container = &(*stack)[top - 1];

            final switch (container.type) {
            case ContainerElement.ARRAY_ITEM:
                container.object.via.array ~= obj;
                if (--container.count == 0) {
                    obj = container.object;
                    top--;
                    goto Lpush;
                }
                break;
            case ContainerElement.MAP_KEY:
                container.key  = obj;
                container.type = ContainerElement.MAP_VALUE;
                break;
            case ContainerElement.MAP_VALUE:
                container.object.via.map ~= mp_KeyValue(container.key, obj);
                if (--container.count == 0) {
                    obj = container.object;
                    top--;
                    goto Lpush;
                }
                container.type = ContainerElement.MAP_KEY;
            }

          Lagain:
            state = State.HEADER;
            cur++;
        } while (cur < used_);

        goto Labort;

      Lfinish:
        (*stack)[0].object = obj;
        ret = true;
        cur++;
        goto Lend;

      Labort:
        ret = false;

      Lend:
        context_.state = state;
        context_.trail = trail;
        context_.top   = top;
        parsed_       += cur - offset_;
        offset_        = cur;

        return ret;
    }


    /**
     * supports foreach. One loop provides $(D Unpacked) object contains execute() result.
     * This is convenient in case that $(D MessagePack) objects are continuous.
     *
     * NOTE:
     *  Why opApply? Currently, D's Range is state-less.
     *  I will change to Range if Phobos supports stream.
     */
    int opApply(scope int delegate(ref Unpacked) dg)
    {
        int result;

        while (execute()) {
            result = dg(Unpacked(context_.stack[0].object));
            if (result)
                break;

            clear();
        }

        return result;
    }


  private:
    /**
     * initializes internal stack environment.
     */
    nothrow void initializeContext()
    {
        context_.state        = State.HEADER;
        context_.trail        = 0;
        context_.top          = 0;
        context_.stack.length = 1;
    }
}


/**
 * Helper for $(D Unpacker) construction.
 *
 * Params:
 *  target     = byte buffer to deserialize.
 *  bufferSize = size limit of buffer size.
 *
 * Returns:
 *  a $(D Unpacker) object instantiated and initialized according to the arguments.
 *  Stream deserializer if $(D_PARAM isStream) is true, otherwise direct-conversion deserializer.
 */
Unpacker!(isStream) unpacker(bool isStream = true)(in ubyte[] target, in size_t bufferSize = 8192)
{
    return typeof(return)(target, bufferSize);
}


unittest
{
    // serialize
    mixin DefinePacker;
    enum Size = mp_Type.max + 1;

    packer.packArray(Size);
    packer.packNil().packTrue().pack(1, -2, "Hi!", [1], [1:1], real.max);

    // deserialize
    auto unpacker = unpacker(packer.buffer.data); unpacker.execute();
    auto unpacked = unpacker.purge();

    // Range test
    foreach (unused; 0..2) {
        uint i;

        foreach (obj; unpacked)
            i++;

        assert(i == Size);
    }

    auto result = unpacked.via.array;

    assert(result[0].type          == mp_Type.NIL);
    assert(result[1].via.boolean   == true);
    assert(result[2].via.uinteger  == 1);
    assert(result[3].via.integer   == -2);
    assert(result[4].via.raw       == [72, 105, 33]);
    assert(result[5].as!(int[])    == [1]);
    assert(result[6].as!(int[int]) == [1:1]);
    assert(result[7].as!(real)     == real.max);
}


private:


/**
 * Sets object type and value.
 *
 * Params:
 *  object = the object to set
 *  value  = the content to set
 */
void callbackUInt(ref mp_Object object, ulong value)
{
    object.type         = mp_Type.POSITIVE_INTEGER;
    object.via.uinteger = value;
}


/// ditto
void callbackInt(ref mp_Object object, long value)
{
    object.type        = mp_Type.NEGATIVE_INTEGER;
    object.via.integer = value;
}


/// ditto
void callbackFloat(ref mp_Object object, real value)
{
    object.type         = mp_Type.FLOAT;
    object.via.floating = value;
}


/// ditto
void callbackRaw(ref mp_Object object, ubyte[] raw)
{
    object.type    = mp_Type.RAW;
    object.via.raw = raw;
}


/// ditto
void callbackArray(ref mp_Object object, size_t length)
{
    object.type = mp_Type.ARRAY;
    object.via.array.length = 0;
    object.via.array.reserve(length);
}


/// ditto
void callbackMap(ref mp_Object object, size_t length)
{
    object.type = mp_Type.MAP;
    object.via.map.length = 0;
    object.via.map.reserve(length);
}


/// ditto
void callbackNil(ref mp_Object object)
{
    object.type = mp_Type.NIL;
}


/// ditto
void callbackBool(ref mp_Object object, bool value)
{
    object.type        = mp_Type.BOOLEAN;
    object.via.boolean = value;
}


unittest
{
    mp_Object object;

    // Unsigned integer
    callbackUInt(object, uint.max);
    assert(object.type         == mp_Type.POSITIVE_INTEGER);
    assert(object.via.uinteger == uint.max);

    // Signed integer
    callbackInt(object, int.min);
    assert(object.type        == mp_Type.NEGATIVE_INTEGER);
    assert(object.via.integer == int.min);

    // Floating point
    callbackFloat(object, real.max);
    assert(object.type         == mp_Type.FLOAT);
    assert(object.via.floating == real.max);

    // Raw
    callbackRaw(object, cast(ubyte[])[1]);
    assert(object.type    == mp_Type.RAW);
    assert(object.via.raw == cast(ubyte[])[1]);

    // Array
    mp_Object[] array; array.reserve(16);

    callbackArray(object, 16);
    assert(object.type               == mp_Type.ARRAY);
    assert(object.via.array.capacity == array.capacity);

    // Map
    mp_KeyValue[] map; map.reserve(16);

    callbackMap(object, 16);
    assert(object.type             == mp_Type.MAP);
    assert(object.via.map.capacity == map.capacity);

    // NIL
    callbackNil(object);
    assert(object.type == mp_Type.NIL);

    // Bool
    callbackBool(object, true);
    assert(object.type        == mp_Type.BOOLEAN);
    assert(object.via.boolean == true);
}


/*
 * A callback for type-mismatched error in deserialization process.
 */
void onInvalidType()
{
    throw new InvalidTypeException("Attempt to unpack with non-compatible type");
}


/*
 * A callback for finding unknown-format in deserialization process.
 */
void onUnknownType()
{
    throw new UnpackException("Unknown type");
}
