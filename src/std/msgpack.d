// Written in the D programming language.

/**
 * MessagePack serializer and deserializer implementation.
 *
 * MessagePack is a binary-based serialization specification.
 *
 * Example:
 * -----
 * auto data = tuple("MessagePack!", [1, 2], true);
 *
 * auto serialized = pack(data);
 *
 * // ...
 *
 * typeof(data) deserialized;
 *
 * unpack(serialized, deserialized);
 *
 * assert(data == deserialized);
 * -----
 *
 * See_Also:
 *  $(LINK2 http://msgpack.org/, The MessagePack Project)
 *
 * Copyright: Copyright Masahiro Nakagawa 2010.
 * License:   <a href="http://www.boost.org/LICENSE_1_0.txt">Boost License 1.0</a>.
 * Authors:   Masahiro Nakagawa
 *
 *          Copyright Masahiro Nakagawa 2010.
 * Distributed under the Boost Software License, Version 1.0.
 *    (See accompanying file LICENSE_1_0.txt or copy at
 *          http://www.boost.org/LICENSE_1_0.txt)
 */
module std.msgpack;

import std.array;
import std.range;
import std.stdio;
import std.traits;
import std.typetuple;
import std.zlib : ZlibException;  // avoiding Z_* symbols conflict

import etc.c.zlib;  // for DeflateFilter

// for VRefBuffer
version(Posix)
{
    import core.sys.posix.sys.uio : iovec;
}
else
{
    /**
     * from core.sys.posix.sys.uio.iovec for compatibility with posix.
     */
    struct iovec
    {
        void*  iov_base;
        size_t iov_len;
    }
}

// for Converting Endian using ntohs and ntohl;
version (Windows)
{
    import std.c.windows.winsock;
}
else
{
    import core.sys.posix.arpa.inet;
}


version(unittest) import std.file, std.typecons, std.c.string;


@trusted:


// Buffer implementations


/**
 * This alias provides clear name for simple buffer.
 */
alias Appender!(ubyte[]) SimpleBuffer;


/**
 * $(D VRefBuffer) is a zero copy buffer for more efficient
 *
 * Example:
 * -----
 * auto packer = packer(vrefBuffer());
 *
 * // packs data
 *
 * writev(fd, cast(void*)packer.buffer.vector.ptr, packer.buffer.vector.length);
 * -----
 *
 * See_Also:
 *  $(LINK http://msgpack.sourceforge.net/doc:introduction#zerocopy_serialization)
 */
struct VRefBuffer
{
  private:
    immutable size_t RefSize, ChunkSize;

    // for putCopy
    ubyte[][] chunk_;  // memory chunk for buffer
    size_t[]  uList_;  // used size list for chunk
    size_t    index_;  // index for cunrrent chunk

    // for putRef
    iovec[] vecList_;  // referece to large data or copied data.


  public:
    /**
     * Constructs a buffer.
     *
     * Params:
     *  refSize   = the threshold of writing value.
     *  chunkSize = the default size of chunk for allocation.
     */
    @safe this(in size_t refSize = 32, in size_t chunkSize = 8192)
    {
        RefSize   = refSize;
        ChunkSize = chunkSize;

        uList_.length = 1;
        chunk_.length = 1;
        chunk_[index_].length = chunkSize;
    }


    /**
     * Returns the buffer contents excluding references.
     *
     * Returns:
     *  the non-contiguous copied contents.
     */
    @property @safe nothrow ubyte[] data()
    {
        ubyte[] result;

        foreach (i, data; chunk_)
            result ~= data[0..uList_[i]];

        return result;
    }


    /**
     * Forwards to all buffer contents.
     *
     * Returns:
     *  the array of iovec struct that stores references.
     */
    @property @safe nothrow iovec[] vector()
    {
        return vecList_;
    }


    /**
     * Writes $(D_PARAM value) to buffer.
     *
     * Params:
     *  value = the content to write.
     */
    void put(in ubyte value)
    {
        ubyte[1] values = [value];
        putCopy(values);
    }


    /**
     * Writes $(D_PARAM values) to buffer if $(D_PARAM values) size is smaller
     * than RefSize, otherwise stores reference of $(D_PARAM values).
     *
     * Params:
     *  values = the content to write.
     */
    void put(in ubyte[] values)
    {
        if (values.length < RefSize)
            putCopy(values);
        else
            putRef(values);
    }


  private:
    /**
     * Stores reference of $(D_PARAM values).
     *
     * Params:
     *  values = the content to write.
     */
    void putRef(in ubyte[] values)
    {
        vecList_.length += 1;
        vecList_[$ - 1]  = iovec(cast(void*)values.ptr, values.length);
    }


    /**
     * Writes $(D_PARAM values) to buffer and appends to reference.
     *
     * Params:
     *  values = the contents to write.
     */
    void putCopy(in ubyte[] values)
    {
        /*
         * Helper for expanding new space.
         */
        void expand(in size_t size)
        {
            const newSize = size < ChunkSize ? ChunkSize : size;

            index_++;
            uList_.length += 1;
            chunk_.length += 1;
            chunk_[index_].length = newSize;
        }

        const size = values.length;

        // lacks current chunk?
        if (chunk_[index_].length - uList_[index_] < size)
            expand(size);

        const base = uList_[index_];                     // start index
        auto  data = chunk_[index_][base..base + size];  // chunk to write

        data[]          = values;
        uList_[index_] += size;

        // Optimization for avoiding iovec allocation.
        if (vecList_.length && data.ptr == (vecList_[$ - 1].iov_base +
                                            vecList_[$ - 1].iov_len))
            vecList_[$ - 1].iov_len += size;
        else
            putRef(data);
    }
}


/**
 * Helper for $(D VRefBuffer) construction.
 *
 * Params:
 *  refSize   = the threshold of writing value.
 *  chunkSize = the default size of chunk for allocation.
 *
 * Returns:
 *  a $(D VRefBuffer) object instantiated and initialized according to the arguments.
 */
@safe VRefBuffer vrefBuffer(in size_t refSize = 32, in size_t chunkSize = 8192)
{
    return typeof(return)(refSize, chunkSize);
}


unittest
{
    auto buffer = vrefBuffer(2, 4);

    ubyte[] tests = [1, 2];
    foreach (v; tests)
        buffer.put(v);
    buffer.put(tests);

    assert(buffer.data == tests, "putCopy failed");

    iovec[] vector = buffer.vector;
    ubyte[] result;

    assert(vector.length == 2, "Optimization failed");

    foreach (v; vector)
        result ~= (cast(ubyte*)v.iov_base)[0..v.iov_len];

    assert(result == tests ~ tests);
}


/**
 * $(D BinaryFileWriter) is a wrapper for $(LINK2 http://www.digitalmars.com/d/2.0/phobos/std_stdio.html#File, File)
 */
struct BinaryFileWriter
{
  private:
    File         file_;     // stream to write
    bool         doCache_;  // indicates whether caches content
    SimpleBuffer cache_;    // buffer for cache


  public:
    /**
     * Constructs a writer.
     *
     * Params:
     *  file    = the $(D File) to output.
     *  doCache = caching content if true.
     */
    this(ref File file, bool doCache = false)
    {
        file_    = file;
        doCache_ = doCache;
    }


    /**
     * Constructs a writer and opens $(D_PARAM name) file.
     *
     * Params:
     *  name    = filename to $(D File) construction.
     *  doCache = caching content if true.
     */
    this(in string name, bool doCache = false)
    {
        file_    = File(name, "wb");
        doCache_ = doCache;
    }


    /**
     * Forwards to cache contents.
     *
     * Returns:
     *  the cache contents if doCache is true, otherwise null.
     */
    @property /* nothrow */ ubyte[] data() // data method of Appender isn't nothrow
    {
        return doCache_ ? cache_.data : null;
    }


    /**
     * Closes $(D File) object.
     */
    void close()
    {
        file_.close();
    }


    /**
     * Writes $(D_PARAM value) to buffer.
     *
     * Params:
     *  value = the content to write.
     *
     * Throws:
     *  $(D StdioException) if file closed.
     */
    void put(in ubyte value)
    {
        ubyte[1] values = [value];
        put(values);
    }


    /// ditto
    void put(in ubyte[] values)
    {
        if (doCache_)
            cache_.put(values);

        if (file_.isOpen)
            file_.rawWrite(values);
        else
            throw new StdioException("File has been closed", 5);  // EIO
    }
}


unittest
{
    auto    name  = "deleteme";
    ubyte[] tests = [1, 2];

    { // output to name file
        auto writer = BinaryFileWriter(name, true);

        foreach (v; tests)
            writer.put(v);
        writer.put(tests);

        writer.close();

        assert(writer.data == tests ~ tests);
    }
    { // input from name file
        auto input  = File(name, "rb");
        auto result = new ubyte[](4);

        input.rawRead(result);

        assert(result == tests ~ tests);
    }

    remove(name);
}


/**
 * This OutputRange filter compresses data using Deflate algorithm.
 *
 * Example:
 * -----
 * auto deflater = deflateFilter(BinaryFileWriter("deflated.mpac"));
 * auto packer   = packer(deflater);
 *
 * packer.pack(data);
 *
 * deflater.flush();
 * deflater.buffer.close();
 * -----
 *
 * This implementation uses etc.c.zlib module.
 */
class DeflateFilter(Buffer) if (isOutputRange!(Buffer, ubyte[]))
{
  private:
    enum TempSize = 128;  // for temporary buffer

    Buffer   buffer_;  // buffer to output
    z_stream stream_;  // zlib-stream for deflation


  public:
    /**
     * Constructs a buffer.
     *
     * Params:
     *  buffer     = the buffer to output.
     *  level      = Compression level for deflation.
     *
     * Throws:
     *  $(D ZlibException) if initialization of zlib-stream failed.
     */
    this(Buffer buffer, in int level = Z_DEFAULT_COMPRESSION)
    in
    {
        assert(Z_DEFAULT_COMPRESSION <= level && level <= Z_BEST_COMPRESSION);
    }
    body
    {
        check(deflateInit(&stream_, level));

        buffer_ = buffer;
    }


    /**
     * Ends zlib-stream.
     */
    ~this()
    {
        deflateEnd(&stream_);
    }


    /**
     * Forwards to buffer.
     *
     * Returns:
     *  the buffer.
     */
    static if (is(Unqual!Buffer == struct)) {
        @property @safe nothrow ref Buffer buffer()
        {
            return buffer_;
        }
    } else {
        @property @safe nothrow Buffer buffer()
        {
            return buffer_;
        }
    }


    /**
     * Writes $(D_PARAM value) to buffer with compression.
     *
     * Params:
     *  value = the content to compress.
     *
     * Throws:
     *  $(D ZlibException) if deflation failed.
     */
    void put(in ubyte value)
    {
        ubyte[1] values = [value];
        put(values);
    }


    /// ditto
    void put(in ubyte[] values)
    in
    {
        assert(values.length);
    }
    body
    {
        ubyte[]         result;
        ubyte[TempSize] temp;

        // set input
        stream_.next_in  = cast(ubyte*)values.ptr;
        stream_.avail_in = values.length;

        do {
            // set output
            stream_.next_out  = temp.ptr;
            stream_.avail_out = TempSize;

            check(deflate(&stream_, Z_NO_FLUSH));

            if (TempSize - stream_.avail_out)  // for performance(zlib has internal buffer)
                result ~= temp[0..TempSize - stream_.avail_out];
        } while (stream_.avail_in > 0)

        if (result.length)  // avoids function call
            buffer_.put(result);
    }


    /**
     * Flushes the zlib-stream.
     */
    void flush()
    {
        ubyte[]         result;
        ubyte[TempSize] temp;

        while (true) {
            stream_.next_out  = temp.ptr;
            stream_.avail_out = TempSize;

            auto status = deflate(&stream_, Z_FINISH);

            switch (status) {
            case Z_STREAM_END:
                buffer_.put(result ~ temp[0..TempSize - stream_.avail_out]);
                return;
            case Z_OK:
                result ~= temp;
                break;
            default:
                check(status);
            }
        }
    }


    /**
     * Resets the zlib-stream.
     *
     * Throws:
     *  $(D ZlibException) if resets of zlib-stream failed.
     */
    void reset()
    {
        check(deflateReset(&stream_));
    }


  private:
    /**
     * Checks stream status.
     *
     * Params:
     *  status = return code from zlib function.
     *
     * Throws:
     *  $(D ZlibException) if $(D_PARAM status) isn't $(D Z_OK).
     */
    void check(in int status)
    {
        if (status != Z_OK) {
            deflateEnd(&stream_);

            throw new ZlibException(status);
        }
    }
}


/**
 * This InputRange filter uncompresses data using Deflate algorithm.
 *
 * Example:
 * -----
 * ubyte[] buffer;
 * auto inflater = deflateFilter(ByChunk(File("deflated.mpac", "rb"), 128));
 *
 * foreach (inflated; inflater)
 *     buffer ~= inflated;
 * -----
 *
 * This implementation uses etc.c.zlib module.
 */
class DeflateFilter(Buffer) if (isInputRange!(Buffer))
{
  private:
    enum TempSize = 128;  // for temporary buffer

    Buffer   buffer_;  // buffer to input
    z_stream stream_;  // zlib-stream for inflation


  public:
    /**
     * Constructs a buffer.
     *
     * Params:
     *  buffer = the buffer to input
     *
     * Throws:
     *  $(D ZlibException) if initialization of zlib-stream failed.
     */
    this(Buffer buffer)
    {
        check(inflateInit(&stream_));

        buffer_ = buffer;
    }


    /**
     * Ends zlib-stream.
     */
    ~this()
    {
        inflateEnd(&stream_);
    }


    /**
     * Forwards to buffer.
     *
     * Returns:
     *  the buffer.
     */
    static if (is(Unqual!Buffer == struct)) {
        @property @safe nothrow ref Buffer buffer()
        {
            return buffer_;
        }
    } else {
        @property @safe nothrow Buffer buffer()
        {
            return buffer_;
        }
    }


    /**
     * Range primitive operation that checks iteration state.
     *
     * Returns:
     *  true if there are no more elements to be iterated.
     */
    @property bool empty()
    {
        return buffer_.empty;
    }


    /**
     * Range primitive operation that returns the currently iterated element.
     *
     * Returns:
     *  the uncompressed data.
     */
    @property ubyte[] front()
    {
        ubyte[]         result, data = buffer_.front;
        ubyte[TempSize] temp;

        // set input
        stream_.next_in  = data.ptr;
        stream_.avail_in = data.length;

        do {
            // set output
            stream_.next_out  = temp.ptr;
            stream_.avail_out = TempSize;

            auto status = inflate(&stream_, Z_SYNC_FLUSH);
            if (status != Z_STREAM_END)
                check(status);

            result ~= temp[0..TempSize - stream_.avail_out];
        } while (stream_.avail_in > 0)

        return result;
    }


    /**
     * Range primitive operation that advances the range to its next element.
     */
    void popFront()
    { 
        buffer_.popFront();
    }


    /**
     * Resets the zlib-stream.
     *
     * Throws:
     *  $(D ZlibException) if resets of zlib-stream failed.
     */
    void reset()
    {
        check(inflateReset(&stream_));
    }


  private:
    /**
     * Checks stream status.
     *
     * Params:
     *  status = return code from zlib function.
     *
     * Throws:
     *  $(D ZlibException) if $(D_PARAM status) isn't $(D Z_OK).
     */
    void check(in int status)
    {
        if (status != Z_OK) {
            inflateEnd(&stream_);

            throw new ZlibException(status);
        }
    }
}


/**
 * Helper for $(D DeflateFilter) construction.
 *
 * Creates compression filter if $(D_PARAM Buffer) is OutputRange, otherwise uncompression filter.
 *
 * Params:
 *  buffer = the buffer to output / input
 *  level  = Compression level for compression.
 *
 * Returns:
 *  a $(D DeflateFilter) object instantiated and initialized according to the arguments.
 */
DeflateFilter!(Buffer) deflateFilter(Buffer)(Buffer buffer, lazy int level = Z_DEFAULT_COMPRESSION)
{
    static if (isOutputRange!(Buffer, ubyte[]))
        return new typeof(return)(buffer, level);
    else
        return new typeof(return)(buffer);
}


unittest
{
    static struct PartialData
    {
        ubyte[] data, now;

        this(ubyte[] src) { data = src; popFront(); }

        @property bool empty() { return data.empty; }

        @property ubyte[] front() { return now; }

        void popFront()
        { 
            auto size = data.length > 4 ? 4 : data.length;

            now  = data[0..size];
            data = data[size..$];
        }
    }

    void check(in int status)
    {
        if (status != Z_OK && status != Z_STREAM_END)
            throw new ZlibException(status);
    }

    ubyte[] result, tests = [1, 2];

    // deflation
    scope deflater = deflateFilter(SimpleBuffer());

    foreach (v; tests)
        deflater.put(v);
    deflater.put(tests);
    deflater.flush();

    // inflation
    scope inflater = deflateFilter(PartialData(deflater.buffer.data));

    foreach (inflated; inflater)
        result ~= inflated;

    assert(result == tests ~ tests);
}


// Serializing routines


/**
 * $(D Packer) is a $(D MessagePack) serializer
 *
 * Example:
 * -----
 * auto packer = packer(SimpleBuffer());
 *
 * packer.packArray(4);  // sets array length
 * packer.packFalse();   // false
 * packer.pack(100);     // 100   of int
 * packer.pack(1e-10);   // 1e-10 of double
 * packer.packNil();     // null
 *
 * // or packer.packArray(4).pack(false, 100, 1e-10).packNil();
 *
 * stdout.rawWrite(packer.buffer.data);
 * -----
 */
struct Packer(Buffer) if (isOutputRange!(Buffer, ubyte) && isOutputRange!(Buffer, ubyte[]))
{
  private:
    enum size_t Offset = 1;  // type-information offset

    Buffer                          buffer_;  // the buffer to write
    ubyte[Offset + 1 + real.sizeof] store_;   // stores serialized value


  public:
    /**
     * Constructs a packer with $(D_PARAM buffer).
     *
     * Params:
     *  buffer = the buffer to write.
     */
    @safe this(Buffer buffer)
    {
        buffer_ = buffer;
    }


    /**
     * Forwards to buffer.
     *
     * Returns:
     *  the buffer.
     */
    static if (is(Unqual!Buffer == struct)) {
        @property @safe nothrow ref Buffer buffer()
        {
            return buffer_;
        }
    } else {
        @property @safe nothrow Buffer buffer()
        {
            return buffer_;
        }
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
    ref Packer pack(T)(in T value) if (is(Unqual!T == bool))
    {
        return value ? packTrue() : packFalse();
    }


    /// ditto
    ref Packer pack(T)(in T value) if (is(Unqual!T == ubyte))
    {
        if (value < (1 << 7)) {
            // fixnum
            buffer_.put(take8from(value));
        } else {
            // uint 8
            store_[0] = Format.UINT8;
            store_[1] = take8from(value);
            buffer_.put(store_[0..Offset + ubyte.sizeof]);
        }

        return this;
    }


    /// ditto
    ref Packer pack(T)(in T value) if (is(Unqual!T == ushort))
    {
        if (value < (1 << 7)) {
            // fixnum
            buffer_.put(take8from!16(value));
        } else if (value < (1 << 8)) {
            // uint 8
            store_[0] = Format.UINT8;
            store_[1] = take8from!16(value);
            buffer_.put(store_[0..Offset + ubyte.sizeof]);
        } else {
            // uint 16
            const temp = convertEndianTo!16(value);

            store_[0] = Format.UINT16;
            *cast(ushort*)&store_[Offset] = temp;
            buffer_.put(store_[0..Offset + ushort.sizeof]);
        }

        return this;
    }


    /// ditto
    ref Packer pack(T)(in T value) if (is(Unqual!T == uint))
    {
        if (value < (1 << 8)) {
            if (value < (1 << 7)) {
                // fixnum
                buffer_.put(take8from!32(value));
            } else {
                // uint 8
                store_[0] = Format.UINT8;
                store_[1] = take8from!32(value);
                buffer_.put(store_[0..Offset + ubyte.sizeof]);
            }
        } else {
            if (value < (1 << 16)) {
                // uint 16
                const temp = convertEndianTo!16(value);

                store_[0] = Format.UINT16;
                *cast(ushort*)&store_[Offset] = temp;
                buffer_.put(store_[0..Offset + ushort.sizeof]);
            } else {
                // uint 32
                const temp = convertEndianTo!32(value);

                store_[0] = Format.UINT32;
                *cast(uint*)&store_[Offset] = temp;
                buffer_.put(store_[0..Offset + uint.sizeof]);
            }
        }

        return this;
    }


    /// ditto
    ref Packer pack(T)(in T value) if (is(Unqual!T == ulong))
    {
        if (value < (1UL << 8)) {
            if (value < (1UL << 7)) {
                // fixnum
                buffer_.put(take8from!64(value));
            } else {
                // uint 8
                store_[0] = Format.UINT8;
                store_[1] = take8from!64(value);
                buffer_.put(store_[0..Offset + ubyte.sizeof]);
            }
        } else {
            if (value < (1UL << 16)) {
                // uint 16
                const temp = convertEndianTo!16(value);

                store_[0] = Format.UINT16;
                *cast(ushort*)&store_[Offset] = temp;
                buffer_.put(store_[0..Offset + ushort.sizeof]);
            } else if (value < (1UL << 32)){
                // uint 32
                const temp = convertEndianTo!32(value);

                store_[0] = Format.UINT32;
                *cast(uint*)&store_[Offset] = temp;
                buffer_.put(store_[0..Offset + uint.sizeof]);
            } else {
                // uint 64
                const temp = convertEndianTo!64(value);

                store_[0] = Format.UINT64;
                *cast(ulong*)&store_[Offset] = temp;
                buffer_.put(store_[0..Offset + ulong.sizeof]);
            }
        }

        return this;
    }


    /// ditto
    ref Packer pack(T)(in T value) if (is(Unqual!T == byte))
    {
        if (value < -(1 << 5)) {
            // int 8
            store_[0] = Format.INT8;
            store_[1] = take8from(value);
            buffer_.put(store_[0..Offset + byte.sizeof]);
        } else {
            // fixnum
            buffer_.put(take8from(value));
        }

        return this;
    }


    /// ditto
    ref Packer pack(T)(in T value) if (is(Unqual!T == short))
    {
        if (value < -(1 << 5)) {
            if (value < -(1 << 7)) {
                // int 16
                const temp = convertEndianTo!16(value);

                store_[0] = Format.INT16;
                *cast(short*)&store_[Offset] = temp;
                buffer_.put(store_[0..Offset + short.sizeof]);
            } else {
                // int 8
                store_[0] = Format.INT8;
                store_[1] = take8from!16(value);
                buffer_.put(store_[0..Offset + byte.sizeof]);
            }
        } else if (value < (1 << 7)) {
            // fixnum
            buffer_.put(take8from!16(value));
        } else {
            if (value < (1 << 8)) {
                // uint 8
                store_[0] = Format.UINT8;
                store_[1] = take8from!16(value);
                buffer_.put(store_[0..Offset + ubyte.sizeof]);                
            } else {
                // uint 16
                const temp = convertEndianTo!16(value);

                store_[0] = Format.UINT16;
                *cast(ushort*)&store_[Offset] = temp;
                buffer_.put(store_[0..Offset + ushort.sizeof]);
            }
        }

        return this;
    }


    /// ditto
    ref Packer pack(T)(in T value) if (is(Unqual!T == int))
    {
        if (value < -(1 << 5)) {
            if (value < -(1 << 15)) {
                // int 32
                const temp = convertEndianTo!32(value);

                store_[0] = Format.INT32;
                *cast(int*)&store_[Offset] = temp;
                buffer_.put(store_[0..Offset + int.sizeof]);
            } else if (value < -(1 << 7)) {
                // int 16
                const temp = convertEndianTo!16(value);

                store_[0] = Format.INT16;
                *cast(short*)&store_[Offset] = temp;
                buffer_.put(store_[0..Offset + short.sizeof]);
            } else {
                // int 8
                store_[0] = Format.INT8;
                store_[1] = take8from!32(value);
                buffer_.put(store_[0..Offset + byte.sizeof]);
            }
        } else if (value < (1 << 7)) {
            // fixnum
            buffer_.put(take8from!32(value));
        } else {
            if (value < (1 << 8)) {
                // uint 8
                store_[0] = Format.UINT8;
                store_[1] = take8from!32(value);
                buffer_.put(store_[0..Offset + ubyte.sizeof]);
            } else if (value < (1 << 16)) {
                // uint 16
                const temp = convertEndianTo!16(value);

                store_[0] = Format.UINT16;
                *cast(ushort*)&store_[Offset] = temp;
                buffer_.put(store_[0..Offset + ushort.sizeof]);
            } else {
                // uint 32
                const temp = convertEndianTo!32(value);

                store_[0] = Format.UINT32;
                *cast(uint*)&store_[Offset] = temp;
                buffer_.put(store_[0..Offset + uint.sizeof]);
            }
        }

        return this;
    }


    /// ditto
    ref Packer pack(T)(in T value) if (is(Unqual!T == long))
    {
        if (value < -(1L << 5)) {
            if (value < -(1L << 15)) {
                if (value < -(1L << 31)) {
                    // int 64
                    const temp = convertEndianTo!64(value);

                    store_[0] = Format.INT64;
                    *cast(long*)&store_[Offset] = temp;
                    buffer_.put(store_[0..Offset + long.sizeof]);
                } else {
                    // int 32
                    const temp = convertEndianTo!32(value);

                    store_[0] = Format.INT32;
                    *cast(int*)&store_[Offset] = temp;
                    buffer_.put(store_[0..Offset + int.sizeof]);
                }
            } else {
                if (value < -(1L << 7)) {
                    // int 16
                    const temp = convertEndianTo!16(value);

                    store_[0] = Format.INT16;
                    *cast(short*)&store_[Offset] = temp;
                    buffer_.put(store_[0..Offset + short.sizeof]);
                } else {
                    // int 8
                    store_[0] = Format.INT8;
                    store_[1] = take8from!64(value);
                    buffer_.put(store_[0..Offset + byte.sizeof]);
                }
            }
        } else if (value < (1L << 7)) {
            // fixnum
            buffer_.put(take8from!64(value));
        } else {
            if (value < (1L << 16)) {
                if (value < (1L << 8)) {
                    // uint 8
                    store_[0] = Format.UINT8;
                    store_[1] = take8from!64(value);
                    buffer_.put(store_[0..Offset + ubyte.sizeof]);
                } else {
                    // uint 16
                    const temp = convertEndianTo!16(value);

                    store_[0] = Format.UINT16;
                    *cast(ushort*)&store_[Offset] = temp;
                    buffer_.put(store_[0..Offset + ushort.sizeof]);
                }
            } else {
                if (value < (1L << 32)) {
                    // uint 32
                    const temp = convertEndianTo!32(value);

                    store_[0] = Format.UINT32;
                    *cast(uint*)&store_[Offset] = temp;
                    buffer_.put(store_[0..Offset + uint.sizeof]);
                } else {
                    // uint 64
                    const temp = convertEndianTo!64(value);

                    store_[0] = Format.UINT64;
                    *cast(ulong*)&store_[Offset] = temp;
                    buffer_.put(store_[0..Offset + ulong.sizeof]);
                }
            }
        }

        return this;
    }


    /// ditto
    ref Packer pack(T)(in T value) if (is(Unqual!T == float))
    {
        const temp = convertEndianTo!32(_f(value).i);

        store_[0] = Format.FLOAT;
        *cast(uint*)&store_[Offset] = temp;
        buffer_.put(store_[0..Offset + uint.sizeof]);

        return this;
    }


    /// ditto
    ref Packer pack(T)(in T value) if (is(Unqual!T == double))
    {
        const temp = convertEndianTo!64(_d(value).i);

        store_[0] = Format.DOUBLE;
        *cast(ulong*)&store_[Offset] = temp;
        buffer_.put(store_[0..Offset + ulong.sizeof]);

        return this;
    }


    /// ditto
    ref Packer pack(T)(in T value) if (is(Unqual!T == enum))
    {
        pack(cast(OriginalType!T)value);

        return this;
    }


    /// ditto
    ref Packer pack(T)(in T array) if (isArray!T)
    {
        alias typeof(T.init[0]) U;

        if (array is null)
            return packNil();

        // Raw bytes
        static if (isByte!(U) || isSomeChar!(U)) {
            ubyte[] raw = cast(ubyte[])array;

            packRaw(raw.length);
            buffer_.put(raw);
        } else {
            packArray(array.length);
            foreach (elem; array)
                pack(elem);
        }

        return this;
    }


    /// ditto
    ref Packer pack(T)(in T array) if (isAssociativeArray!T)
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


    /**
     * Serializes $(D_KEYWORD real) type and writes to buffer.
     *
     * This method is marked @system because $(D_KEYWORD real) type is D only!
     * MessagePack doesn't define $(D_KEYWORD real) type format.
     * Don't use this method if you communicate with other languages.
     *
     * Transfers pack!(double) if $(D_KEYWORD real) type on your environment equals $(D_KEYWORD double) type.
     *
     * Params:
     *  value = the content to serialize.
     *
     * Returns:
     *  this to method chain.
     */
    @system ref Packer pack(T)(in T value) if (is(Unqual!T == real))
    {
        static if (real.sizeof > double.sizeof) {
            store_[0..2]   = [Format.REAL, real.sizeof];
            const temp     = _r(value);
            const fraction = convertEndianTo!64(temp.fraction);
            const exponent = convertEndianTo!ES(temp.exponent);

            *cast(Unqual!(typeof(fraction))*)&store_[Offset + 1]                   = fraction;
            *cast(Unqual!(typeof(exponent))*)&store_[Offset + 1 + fraction.sizeof] = exponent;
            buffer_.put(store_[0..$]);
        } else {  // Non-x86 CPUs, real type equals double type.
            pack(cast(double)value);
        }

        return this;
    }


    /**
     * Serializes $(D_PARAM object) and writes to buffer.
     *
     * $(D_KEYWORD struct) and $(D_KEYWORD class) need to implement $(D mp_pack) method.
     * $(D mp_pack) signature is:
     * -----
     * void mp_pack(Packer)(ref Packer packer) const
     * -----
     * Assumes $(D std.typecons.Tuple) if $(D_KEYWORD struct) doens't implement $(D mp_pack).
     *
     * Params:
     *  object = the content to serialize.
     *
     * Returns:
     *  this to method chain.
     */
    ref Packer pack(T)(in T object) if (is(Unqual!T == class))
    {
        static if (!__traits(compiles, { T t; t.mp_pack(this); }))
            static assert(false, T.stringof ~ " is not a MessagePackable object");

        if (object is null)
            return packNil();

        object.mp_pack(this);

        return this;
    }


    /// ditto
    ref Packer pack(T)(auto ref T object) if (is(Unqual!T == struct))
    {
        static if (__traits(compiles, { T t; t.mp_pack(this); })) {
            object.mp_pack(this);
        } else {  // std.typecons.Tuple
            packArray(object.field.length);
            foreach (f; object.field)
                pack(f);
        }

        return this;
    }


    /**
     * Serializes $(D_PARAM Types) objects and writes to buffer.
     *
     * Params:
     *  objects = the contents to serialize.
     *
     * Returns:
     *  this to method chain.
     */
    template pack(Types...) if (Types.length > 1)
    {
        ref Packer pack(auto ref Types objects)
        {
            foreach (i, T; Types)
                pack(objects[i]);

            return this;
        }
    }
    /*
     * @@@BUG@@@ http://d.puremagic.com/issues/show_bug.cgi?id=2460
    ref Packer pack(Types...)(auto ref Types objects) if (Types.length > 1)
    { // do stuff }
    */


    /**
     * Serializes type-information to buffer.
     *
     * Params:
     *  length = the length of container.
     *
     * Returns:
     *  this to method chain.
     */
    ref Packer packArray(in size_t length)
    {
        if (length < 16) {
            const ubyte temp = Format.ARRAY | cast(ubyte)length;
            buffer_.put(take8from(temp));
        } else if (length < 65536) {
            const temp = convertEndianTo!16(length);

            store_[0] = Format.ARRAY16;
            *cast(ushort*)&store_[Offset] = temp;
            buffer_.put(store_[0..Offset + ushort.sizeof]);
        } else {
            const temp = convertEndianTo!32(length);

            store_[0] = Format.ARRAY32;
            *cast(uint*)&store_[Offset] = temp;
            buffer_.put(store_[0..Offset + uint.sizeof]);
        }

        return this;
    }


    /// ditto
    ref Packer packMap(in size_t length)
    {
        if (length < 16) {
            const ubyte temp = Format.MAP | cast(ubyte)length;
            buffer_.put(take8from(temp));
        } else if (length < 65536) {
            const temp = convertEndianTo!16(length);

            store_[0] = Format.MAP16;
            *cast(ushort*)&store_[Offset] = temp;
            buffer_.put(store_[0..Offset + ushort.sizeof]);
        } else {
            const temp = convertEndianTo!32(length);

            store_[0] = Format.MAP32;
            *cast(uint*)&store_[Offset] = temp;
            buffer_.put(store_[0..Offset + uint.sizeof]);
        }

        return this;
    }


    /// ditto
    ref Packer packRaw(in size_t length)
    {
        if (length < 32) {
            const ubyte temp = Format.RAW | cast(ubyte)length;
            buffer_.put(take8from(temp));
        } else if (length < 65536) {
            const temp = convertEndianTo!16(length);

            store_[0] = Format.RAW16;
            *cast(ushort*)&store_[Offset] = temp;
            buffer_.put(store_[0..Offset + ushort.sizeof]);
        } else {
            const temp = convertEndianTo!32(length);

            store_[0] = Format.RAW32;
            *cast(uint*)&store_[Offset] = temp;
            buffer_.put(store_[0..Offset + uint.sizeof]);
        }

        return this;
    }


    /**
     * Serializes the unique value.
     *
     * Returns:
     *  this to method chain.
     */
    ref Packer packNil()
    {
        buffer_.put(Format.NIL);
        return this;
    }


    /// ditto
    ref Packer packTrue()
    {
        buffer_.put(Format.TRUE);
        return this;
    }


    /// ditto
    ref Packer packFalse()
    {
        buffer_.put(Format.FALSE);
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
@safe Packer!(Buffer) packer(Buffer)(Buffer buffer)
{
    return typeof(return)(buffer);
}


version (unittest) 
{
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

        packer.packNil().packTrue().packFalse().pack(true, false);
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
        static if (real.sizeof == double.sizeof)
            alias TypeTuple!(float, double, double) FloatingTypes;
        else
            alias TypeTuple!(float, double, real) FloatingTypes;

        static struct FTest { ubyte format; real value; }

        static FTest[] tests = [
            {Format.FLOAT,  float.min},
            {Format.DOUBLE, double.max},
            {Format.REAL,   real.max},
        ];

        foreach (I, T; FloatingTypes) {
            mixin DefinePacker;

            packer.pack(cast(T)tests[I].value);
            assert(buffer.data[0] == tests[I].format);

            switch (I) {
            case 0:
                const answer = convertEndianTo!32(_f(cast(T)tests[I].value).i);
                assert(memcmp(&buffer.data[1], &answer, float.sizeof) == 0);
                break;
            case 1:
                const answer = convertEndianTo!64(_d(cast(T)tests[I].value).i);
                assert(memcmp(&buffer.data[1], &answer, double.sizeof) == 0);
                break;
            default:
                const t = _r(cast(T)tests[I].value);
                const f = convertEndianTo!64(t.fraction);
                const e = convertEndianTo!ES(t.exponent);
                assert(buffer.data[1] == real.sizeof);
                assert(memcmp(&buffer.data[2],            &f, f.sizeof) == 0);
                assert(memcmp(&buffer.data[2 + f.sizeof], &e, e.sizeof) == 0);
            }
        }
    }
    { // enum
        enum E : ubyte { A = ubyte.max }

        mixin DefinePacker; E e = E.A;

        packer.pack(e);
        assert(buffer.data[0] == Format.UINT8);

        auto answer = E.A;
        assert(memcmp(&buffer.data[1], &answer, (OriginalType!E).sizeof) == 0);
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
            mixin DefinePacker; auto test = tuple(true, false, uint.max);

            packer.pack(test);

            assert(buffer.data[0] == (Format.ARRAY | 3));
            assert(buffer.data[1] ==  Format.TRUE);
            assert(buffer.data[2] ==  Format.FALSE);
            assert(buffer.data[3] ==  Format.UINT32);
            assert(memcmp(&buffer.data[4], &test.field[2], uint.sizeof) == 0);
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


// deserializing routines


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


version (D_Ddoc)
{
    /**
     * Internal buffer and related operations for Unpacker
     *
     * Following Unpackers mixin this template. So, Unpacker can use following methods.
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
     *
     * This mixin template is a private.
     */
    mixin template InternalBuffer()
    {
        /**
         * Forwards to internal buffer.
         *
         * Returns:
         *  the reference of internal buffer.
         */
        @property @safe nothrow ubyte[] buffer();


        /**
         * Fills internal buffer with $(D_PARAM target).
         *
         * Params:
         *  target = new serialized buffer to deserialize.
         */
        /* @safe */ void feed(in ubyte[] target);


        /**
         * Consumes buffer. This method is helper for buffer property.
         * You must use this method if you write bytes to buffer directly.
         *
         * Params:
         *  size = the number of consuming.
         */
        @safe nothrow void bufferConsumed(in size_t size);


        /**
         * Removes unparsed buffer.
         */
        @safe nothrow void removeUnparsed();


        /**
         * Returns:
         *  the total size including unparsed buffer size.
         */
        @property @safe nothrow size_t size() const;


        /**
         * Returns:
         *  the parsed size of buffer.
         */
        @property @safe nothrow size_t parsedSize() const;


        /**
         * Returns:
         *  the unparsed size of buffer.
         */
        @property @safe nothrow size_t unparsedSize() const;
    }
}
else
{ 
    private mixin template InternalBuffer()
    {
      private:
        ubyte[] buffer_;  // internal buffer
        size_t  used_;    // index that buffer cosumed
        size_t  offset_;  // index that buffer parsed
        size_t  parsed_;  // total size of parsed message
        bool    hasRaw_;  // indicates whether Raw object has been deserialized


      public:
        @property @safe nothrow ubyte[] buffer()
        {
            return buffer_;
        }


        /* @safe */ void feed(in ubyte[] target)
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


        @safe nothrow void bufferConsumed(in size_t size)
        {
            if (used_ + size > buffer_.length)
                used_ = buffer_.length;
            else
                used_ += size;
        }


        @safe nothrow void removeUnparsed()
        {
            used_ = offset_;
        }


        @property @safe nothrow size_t size() const
        {
            return parsed_ - offset_ + used_;
        }


        @property @safe nothrow size_t parsedSize() const
        {
            return parsed_;
        }


        @property @safe nothrow size_t unparsedSize() const
        {
            return used_ - offset_;
        }


      private:
        @safe void initializeBuffer(in ubyte[] target, in size_t bufferSize = 8192)
        {
            const size = target.length;

            buffer_ = new ubyte[](size > bufferSize ? size : bufferSize); 
            used_   = size;
            buffer_[0..size] = target;
        }
    }
}


/**
 * Implementation types for template specialization
 */
enum UnpackerType
{
    DIRECT,  /// Direct-conversion deserializer
    STREAM   /// Stream deserializer
}


/**
 * This $(D Unpacker) is a $(D MessagePack) direct-conversion deserializer
 *
 * This implementation is suitable for fixed data.
 *
 * Example:
 * -----
 * // serializedData is [10, 0.1, false]
 * auto unpacker = unpacker!(UnpackerType.DIRECT)(serializedData);
 *
 * uint   n;
 * double d;
 * bool   b;
 *
 * auto size = unpacker.unpackArray();
 * if (size != 3)
 *     throw new Exception("Size is mismatched!");
 *
 * unpacker.unpack(n).unpack(d).unpack(b); // or unpacker.unpack(n, d, b)
 *
 * // or
 * Tuple!(uint, double, bool) record;
 * unpacker.unpack(record);  // record is [10, 0.1, false]
 * -----
 */
struct Unpacker(UnpackerType Type : UnpackerType.DIRECT)
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
    @safe this(in ubyte[] target, in size_t bufferSize = 8192)
    {
        initializeBuffer(target, bufferSize);
    }


    /**
     * Clears states for next deserialization.
     */
    @safe nothrow void clear()
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
    ref Unpacker unpack(T)(ref T value) if (isUnsigned!T)
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
    ref Unpacker unpack(T)(ref T value) if (isSigned!T && !isFloatingPoint!T)
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
    ref Unpacker unpack(T)(ref T value) if (isFloatingPoint!T)
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


    /// ditto
    ref Unpacker unpack(T)(ref T value) if (is(Unqual!T == enum))
    {
        OriginalType!T temp;

        unpack(temp);

        value = cast(T)temp;

        return this;
    }


    /**
     * Deserializes $(D_PARAM T) object and assigns to $(D_PARAM array).
     *
     * This is convenient method for array deserialization.
     * Rollback will be completely successful if you deserialize raw type((u)byte[] or string types).
     * But, Rollback will be one element(e.g. int) if you deserialize other types(e.g. int[], int[int])
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

        if (checkNil())
            return unpackNil(array);

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

        if (checkNil())
            return unpackNil(array);

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
     * Deserializes $(D_PARAM T) object and assigns to $(D_PARAM object).
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
     *  object = the reference of object to assign.
     *  args   = the arguments to class constructor(class only).
     *           This is used at new statement if $(D_PARAM object) is $(D_KEYWORD null).
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

            if (checkNil())
                return unpackNil(object);

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
     * auto unpacker = unpacker!(UnpackerType.DIRECT)(serializedData);
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


    /**
     * Next object is nil?
     *
     * Returns:
     *  true if next object is nil.
     */
    bool checkNil()
    {
        canRead(Offset, 0);

        return buffer_[offset_] == Format.NIL;
    }


  private:
    /*
     * Reading test.
     *
     * Params:
     *  size   = the size to read.
     *  offset = the offset to subtract when doesn't read from buffer.
     *
     * Throws:
     *  UnpackException when doesn't read from buffer.
     */
    @safe void canRead(in size_t size, in size_t offset = Offset)
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
    @safe ubyte read()
    {
        return buffer_[offset_++];
    }


    /*
     * Reads value from buffer and advances offset.
     */
    @safe ubyte[] read(in size_t size)
    {
        auto result = buffer_[offset_..offset_ + size];

        offset_ += size;

        return result;
    }


    /*
     * Rollbacks offset and throws exception.
     */
    @safe void rollback(in size_t size)
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

        auto unpacker = unpacker!(UnpackerType.DIRECT)(packer.buffer.data);
        unpacker.unpack(result);

        assert(test == result);
    }
    { // uint *
        mixin DefinePacker;

        Tuple!(ubyte, ushort, uint, ulong) result,
            test = tuple(cast(ubyte)ubyte.max, cast(ushort)ushort.max,
                         cast(uint)uint.max,   cast(ulong)ulong.max);

        packer.pack(test);

        auto unpacker = unpacker!(UnpackerType.DIRECT)(packer.buffer.data);
        unpacker.unpack(result);

        assert(test == result);
    }
    { // int *
        mixin DefinePacker;

        Tuple!(byte, short, int, long) result,
            test = tuple(cast(byte)byte.min, cast(short)short.min,
                         cast(int)int.min,   cast(long)long.min);

        packer.pack(test);

        auto unpacker = unpacker!(UnpackerType.DIRECT)(packer.buffer.data);
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

        auto unpacker = unpacker!(UnpackerType.DIRECT)(packer.buffer.data);
        unpacker.unpack(result);

        assert(test == result);
    }
    { // enum
        enum   : float { D = 0.5 }
        enum E : ulong { U = 100 }

        mixin DefinePacker;

        float f = D,   resultF;
        E     e = E.U, resultE;

        packer.pack(D, e);

        auto unpacker = unpacker!(UnpackerType.DIRECT)(packer.buffer.data);
        unpacker.unpack(resultF, resultE);

        assert(f == resultF);
        assert(e == resultE);
    }
    { // container
        mixin DefinePacker;

        Tuple!(ulong[], double[uint], string, bool[2]) result,
            test = tuple([1UL, 2], [3U:4.0, 5:6.0, 7:8.0],
                         "MessagePack is nice!", [true, false]);

        packer.pack(test);

        auto unpacker = unpacker!(UnpackerType.DIRECT)(packer.buffer.data);
        unpacker.unpack(result);

        assert(test == result);
    }
    { // user defined
        {
            static struct S
            {
                uint num;

                void mp_pack(P)(ref P p) const { p.packArray(1); p.pack(num); }
                void mp_unpack(ref Unpacker!(UnpackerType.DIRECT) u)
                { 
                    assert(u.unpackArray == 1);
                    u.unpack(num);
                }
            }

            mixin DefinePacker; S result, test = S(uint.max);

            packer.pack(test);

            auto unpacker = unpacker!(UnpackerType.DIRECT)(packer.buffer.data);
            unpacker.unpack(result);

            assert(test.num == result.num);
        }
        {
            static class C
            {
                uint num;

                this(uint n) { num = n; }

                void mp_pack(P)(ref P p) const { p.packArray(1); p.pack(num - 1); }
                void mp_unpack(ref Unpacker!(UnpackerType.DIRECT) u)
                {
                    assert(u.unpackArray == 1);
                    u.unpack(num);
                }
            }

            mixin DefinePacker; C result, test = new C(ushort.max);

            packer.pack(test);

            auto unpacker = unpacker!(UnpackerType.DIRECT)(packer.buffer.data);
            unpacker.unpack(result, ushort.max);

            assert(test.num == result.num + 1);
        }
    }
    { // variadic
        mixin DefinePacker;

        Tuple!(uint, long, double) test = tuple(uint.max, long.min, double.max);

        packer.pack(test);

        auto unpacker = unpacker!(UnpackerType.DIRECT)(packer.buffer.data);

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

        foreach (n, d, s; &unpacker!(UnpackerType.DIRECT)(data).scan!(int, double, string)) {
            assert(n == 1);
            assert(d == 0.5);
            assert(s == "Hi!");
        }
    }
}


// Static resolution routines for Stream deserializer


/**
 * $(D MessagePack) object type
 *
 * See_Also:
 *  $(LINK2 http://redmine.msgpack.org/projects/msgpack/wiki/FormatSpec, MessagePack Specificaton)
 */
enum mp_Type
{
    NIL,               /// nil
    BOOLEAN,           /// true, false
    POSITIVE_INTEGER,  /// positive fixnum, uint 8, uint 16, uint 32, uint 64
    NEGATIVE_INTEGER,  /// negative fixnum, int 8, int 16, int 32, int 64
    FLOAT,             /// float, double, real
    ARRAY,             /// fix array, array 16, array 32
    MAP,               /// fix map, map 16, map 32
    RAW                /// fix raw, raw 16, raw 32
}


/**
 * $(D InvalidTypeException) is thrown on type errors
 */
class InvalidTypeException : Exception
{
    this(string message)
    {
        super(message);
    }
}


/**
 * $(D mp_Object) is a $(D MessagePack) Object representation
 */
struct mp_Object
{
    /**
     * msgpack value representation
     */
    static union Value
    {
        bool          boolean;   /// corresponding to mp_Type.BOOLEAN
        ulong         uinteger;  /// corresponding to mp_Type.POSITIVE_INTEGER
        long          integer;   /// corresponding to mp_Type.NEGATIVE_INTEGER
        real          floating;  /// corresponding to mp_Type.FLOAT
        mp_Object[]   array;     /// corresponding to mp_Type.ARRAY
        mp_KeyValue[] map;       /// corresponding to mp_Type.MAP
        ubyte[]       raw;       /// corresponding to mp_Type.RAW
    }


    mp_Type type;  /// represents object type 
    Value   via;   /// represents real value


    /**
     * Constructs a $(D mp_Object) with arguments.
     *
     * Params:
     *  value   = the real content.
     *  mp_type = the type of object.
     */
    @safe this(mp_Type mp_type = mp_Type.NIL)
    {
        type = mp_type;
    }


    /// ditto
    @safe this(bool value, mp_Type mp_type = mp_Type.BOOLEAN)
    {
        this(mp_type);
        via.boolean = value;
    }


    /// ditto
    @safe this(ulong value, mp_Type mp_type = mp_Type.POSITIVE_INTEGER)
    {
        this(mp_type);
        via.uinteger = value;
    }


    /// ditto
    @safe this(long value, mp_Type mp_type = mp_Type.NEGATIVE_INTEGER)
    {
        this(mp_type);
        via.integer = value;
    }


    /// ditto
    @safe this(real value, mp_Type mp_type = mp_Type.FLOAT)
    {
        this(mp_type);
        via.floating = value;
    }


    /// ditto
    @safe this(mp_Object[] value, mp_Type mp_type = mp_Type.ARRAY)
    {
        this(mp_type);
        via.array = value;
    }


    /// ditto
    @safe this(mp_KeyValue[] value, mp_Type mp_type = mp_Type.MAP)
    {
        this(mp_type);
        via.map = value;
    }


    /// ditto
    @safe this(ubyte[] value, mp_Type mp_type = mp_Type.RAW)
    {
        this(mp_type);
        via.raw = value;
    }


    /**
     * Converts object value to $(D_PARAM T) type.
     *
     * Returns:
     *  converted value.
     *
     * Throws:
     *  InvalidTypeException if type is mismatched.
     *
     * NOTE:
     *  Current implementation uses cast.
     */
    @property @safe T as(T)() if (is(T == bool))
    {
        if (type != mp_Type.BOOLEAN)
            onCastError();

        return cast(bool)via.boolean;
    }


    /// ditto
    @property @safe T as(T)() if (isIntegral!T)
    {
        if (type == mp_Type.POSITIVE_INTEGER)
            return cast(T)via.uinteger;

        if (type == mp_Type.NEGATIVE_INTEGER)
            return cast(T)via.integer;

        onCastError();

        assert(false);
    }


    /// ditto
    @property @safe T as(T)() if (isFloatingPoint!T)
    {
        if (type != mp_Type.FLOAT)
            onCastError();

        return cast(T)via.floating;
    }


    /// ditto
    @property @safe T as(T)() if (is(Unqual!T == enum))
    {
        return cast(T)as!(OriginalType!T);
    }


    /// ditto
    @property @safe T as(T)() if (isArray!T)
    {
        if (type == mp_Type.NIL)
            return null;

        static if (isSomeString!T) {
            if (type != mp_Type.RAW)
                onCastError();

            return cast(T)via.raw;
        } else {
            alias typeof(T.init[0]) V;

            if (type != mp_Type.ARRAY)
                onCastError();

            V[] array;

            foreach (elem; via.array)
                array ~= elem.as!(V);

            return array;
        }
    }


    /// ditto
    @property @safe T as(T)() if (isAssociativeArray!T)
    {
        alias typeof(T.init.keys[0])   K;
        alias typeof(T.init.values[0]) V;

        if (type == mp_Type.NIL)
            return null;

        if (type != mp_Type.MAP)
            onCastError();

        V[K] map;

        foreach (elem; via.map)
            map[elem.key.as!(K)] = elem.value.as!(V);

        return map;
    }


    /**
     * Converts to $(D_PARAM T) type.
     *
     * $(D_KEYWORD struct) and $(D_KEYWORD class) need to implement $(D mp_unpack) method.
     * $(D mp_unpack) signature is:
     * -----
     * void mp_unpack(mp_Object object)
     * -----
     * Assumes $(D std.typecons.Tuple) if $(D_KEYWORD struct) doens't implement $(D mp_unpack).
     *
     * Params:
     *  args = arguments to class constructor(class only).
     *
     * Returns:
     *  converted value.
     */
    @property /* @safe */ T as(T, Args...)(Args args) if (is(T == class))
    {
        static if (!__traits(compiles, { T t; t.mp_unpack(this); }))
            static assert(false, T.stringof ~ " is not a MessagePackable object");

        if (type == mp_Type.NIL)
            return null;

        T object = new T(args);

        object.mp_unpack(this);

        return object;
    }


    /// ditto
    @property /* @safe */ T as(T)() if (is(T == struct))
    {
        T obj;

        static if (__traits(compiles, { T t; t.mp_unpack(this); })) {
            obj.mp_unpack(this);
        } else {
            foreach (i, Type; T.Types)
                obj.field[i] = via.array[i].as!(Type);
        }

        return obj;
    }


    /**
     * Special method called by $(D Packer).
     *
     * Params:
     *  packer = a serializer.
     */
    void mp_pack(Packer)(ref Packer packer) const
    {
        final switch (type) {
        case mp_Type.NIL:
            packer.packNil();
            break;
        case mp_Type.BOOLEAN:
            packer.pack(via.boolean);
            break;
        case mp_Type.POSITIVE_INTEGER:
            packer.pack(via.uinteger);
            break;
        case mp_Type.NEGATIVE_INTEGER:
            packer.pack(via.integer);
            break;
        case mp_Type.FLOAT:
            packer.pack(via.floating);
            break;
        case mp_Type.RAW:
            packer.pack(via.raw);
            break;
        case mp_Type.ARRAY:
            packer.packArray(via.array.length);
            foreach (elem; via.array)
                elem.mp_pack(packer);
            break;
        case mp_Type.MAP:
            packer.packMap(via.map.length);
            foreach (kv; via.map) {
                kv.key.mp_pack(packer);
                kv.value.mp_pack(packer);
            }
            break;
        }
    }


    /**
     * Comparison for equality.
     */
    @safe bool opEquals(Tdummy = void)(ref const mp_Object other) const
    {
        if (type != other.type)
            return false;

        final switch (type) {
        case mp_Type.NIL:              return true;
        case mp_Type.BOOLEAN:          return via.boolean  == other.via.boolean;
        case mp_Type.POSITIVE_INTEGER: return via.uinteger == other.via.uinteger;
        case mp_Type.NEGATIVE_INTEGER: return via.integer  == other.via.integer;
        case mp_Type.FLOAT:            return via.floating == other.via.floating;
        case mp_Type.RAW:              return via.raw      == other.via.raw;
        case mp_Type.ARRAY:            return via.array    == other.via.array;
        case mp_Type.MAP:              return via.map      == other.via.map;
        }
    }


    /// ditto
    @safe bool opEquals(T : bool)(in T other) const
    {
        if (type != mp_Type.BOOLEAN)
            return false;

        return via.boolean == other;
    }


    /// ditto
    @safe bool opEquals(T : ulong)(in T other) const
    {
        static if (__traits(isUnsigned, T)) {
            if (type != mp_Type.POSITIVE_INTEGER)
                return false;

            return via.uinteger == other;
        } else {
            if (type != mp_Type.NEGATIVE_INTEGER)
                return false;

            return via.integer == other;
        }
    }


    /// ditto
    @safe bool opEquals(T : real)(in T other) const
    {
        if (type != mp_Type.FLOAT)
            return false;

        return via.floating == other;
    }


    /// ditto
    @safe bool opEquals(T : mp_Object[])(in T other) const
    {
        if (type != mp_Type.ARRAY)
            return false;

        return via.array == other;
    }


    /// ditto
    @safe bool opEquals(T : mp_KeyValue[])(in T other) const
    {
        if (type != mp_Type.MAP)
            return false;

        return via.map == other;
    }


    /// ditto
    @safe bool opEquals(T : ubyte[])(in T other) const
    {
        if (type != mp_Type.RAW)
            return false;

        return via.raw == other;
    }
}


/**
 * $(D mp_KeyValue) is a $(D MessagePack) Map Object representation
 */
struct mp_KeyValue
{
    mp_Object key;    /// represents key of Map
    mp_Object value;  /// represents value of Map


    /**
     * Comparison for equality.
     */
    @safe bool opEquals(ref const mp_KeyValue other) const
    {
        return key == other.key && value == other.value;
    }
}


private:


@safe void onCastError()
{
    throw new InvalidTypeException("Attempt to cast with another type");
}


unittest
{
    // nil
    mp_Object object = mp_Object();
    mp_Object other  = mp_Object();

    assert(object      == other);
    assert(object.type == mp_Type.NIL);

    // boolean
    object = mp_Object(true);
    other  = mp_Object(false);

    assert(object           != other);
    assert(object.type      == mp_Type.BOOLEAN);
    assert(object.as!(bool) == true);
    assert(other            == false);

    try {
        auto b = object.as!(uint);
        assert(false);
    } catch (InvalidTypeException e) { }

    // unsigned integer
    object = mp_Object(10UL);
    other  = mp_Object(10UL);

    assert(object           == other);
    assert(object.type      == mp_Type.POSITIVE_INTEGER);
    assert(object.as!(uint) == 10);
    assert(other            == 10UL);

    // signed integer
    object = mp_Object(-20L);
    other  = mp_Object(-10L);

    assert(object          != other);
    assert(object.type     == mp_Type.NEGATIVE_INTEGER);
    assert(object.as!(int) == -20);
    assert(other           == -10L);

    // floating point
    object = mp_Object(0.1e-10L);
    other  = mp_Object(0.1e-20L);

    assert(object           != other);
    assert(object.type      == mp_Type.FLOAT);
    assert(object.as!(real) == 0.1e-10L);
    assert(other            == 0.1e-20L);

    // enum
    enum E : real { F = 0.1e-10L }

    E e1 = object.as!(E);
    E e2 = other.as!(E);

    assert(e1 == E.F);
    assert(e2 != E.F);

    // raw
    object = mp_Object(cast(ubyte[])[72, 105, 33]);
    other  = mp_Object(cast(ubyte[])[72, 105, 33]);

    assert(object             == other);
    assert(object.type        == mp_Type.RAW);
    assert(object.as!(string) == "Hi!");
    assert(other              == cast(ubyte[])[72, 105, 33]);

    // array
    auto t = mp_Object(cast(ubyte[])[72, 105, 33]);
    object = mp_Object([t]);
    other  = mp_Object([t]);

    assert(object               == other);
    assert(object.type          == mp_Type.ARRAY);
    assert(object.as!(string[]) == ["Hi!"]);
    assert(other                == [t]);

    // map
    object = mp_Object([mp_KeyValue(mp_Object(1L), mp_Object(2L))]);
    other  = mp_Object([mp_KeyValue(mp_Object(1L), mp_Object(1L))]);

    assert(object               != other);
    assert(object.type          == mp_Type.MAP);
    assert(object.as!(int[int]) == [1:2]);
    assert(other                == [mp_KeyValue(mp_Object(1L), mp_Object(1L))]);

    object = mp_Object(10UL);

    // struct
    static struct S
    {
        ulong num;

        void mp_unpack(mp_Object object) { num = object.via.uinteger; }
    }

    S s = object.as!(S);
    assert(s.num == 10);

    // class
    static class C
    {
        ulong num;

        void mp_unpack(mp_Object object) { num = object.via.uinteger; }
    }

    C c = object.as!(C);
    assert(c.num == 10);

    // std.typecons.Tuple
    object = mp_Object([mp_Object(true), mp_Object(1UL), mp_Object(cast(ubyte[])"Hi!")]);

    auto tuple = object.as!(Tuple!(bool, uint, string));
    assert(tuple.field[0] == true);
    assert(tuple.field[1] == 1u);
    assert(tuple.field[2] == "Hi!");

    /* 
     * non-MessagePackable object is stopped by static assert
     * static struct NonMessagePackable {}
     * auto nonMessagePackable = object.as!(NonMessagePackable);
     */
}


/**
 * $(D Unpacked) is a $(D InputRange) wrapper for stream deserialization result
 */
struct Unpacked
{
    mp_Object object;  /// deserialized object

    alias object this;


    /**
     * Constructs a $(D Unpacked) with argument.
     *
     * Params:
     *  object = a deserialized object.
     */
    @safe this(mp_Object object)
    {
        this.object = object;
    }


    /**
     * Range primitive operation that checks iteration state.
     *
     * Returns:
     *  true if there are no more elements to be iterated.
     */
    @property @safe nothrow bool empty() const  // std.array.empty isn't nothrow function
    {
        return (object.type == mp_Type.ARRAY) && !object.via.array.length;
    }


    /**
     * Range primitive operation that returns the currently iterated element.
     *
     * Returns:
     *  the deserialized $(D mp_Object).
     */
    @property /* @safe */ ref mp_Object front()
    {
        return object.via.array.front;
    }


    /**
     * Range primitive operation that advances the range to its next element.
     */
    /* @safe */ void popFront()
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
 *         // do stuff (obj is a mp_Object)
 *     }
 * }
 * 
 * if (unpacker.size)
 *     throw new Exception("Message is too large");
 * -----
 */
struct Unpacker(UnpackerType Type : UnpackerType.STREAM)
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
    @safe this(in ubyte[] target, in size_t bufferSize = 8192)
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
    @property @safe Unpacked unpacked()
    {
        return Unpacked(context_.stack[0].object);
    }


    /**
     * Clears some states for next deserialization.
     */
    @safe nothrow void clear()
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
    @safe Unpacked purge()
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
    /*
     * initializes internal stack environment.
     */
    @safe nothrow void initializeContext()
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
 */
@safe Unpacker!(Type) unpacker(UnpackerType Type = UnpackerType.STREAM)(in ubyte[] target, in size_t bufferSize = 8192)
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


/*
 * Sets object type and value.
 *
 * Params:
 *  object = the object to set
 *  value  = the content to set
 */
@safe void callbackUInt(ref mp_Object object, ulong value)
{
    object.type         = mp_Type.POSITIVE_INTEGER;
    object.via.uinteger = value;
}


/// ditto
@safe void callbackInt(ref mp_Object object, long value)
{
    object.type        = mp_Type.NEGATIVE_INTEGER;
    object.via.integer = value;
}


/// ditto
@safe void callbackFloat(ref mp_Object object, real value)
{
    object.type         = mp_Type.FLOAT;
    object.via.floating = value;
}


/// ditto
@safe void callbackRaw(ref mp_Object object, ubyte[] raw)
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
@safe void callbackNil(ref mp_Object object)
{
    object.type = mp_Type.NIL;
}


/// ditto
@safe void callbackBool(ref mp_Object object, bool value)
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
@safe void onInvalidType()
{
    throw new InvalidTypeException("Attempt to unpack with non-compatible type");
}


/*
 * A callback for finding unknown-format in deserialization process.
 */
@safe void onUnknownType()
{
    throw new UnpackException("Unknown type");
}


public:


// Convenient functions


/**
 * Serializes $(D_PARAM args).
 *
 * Assumes single object if the length of $(D_PARAM args) == 1,
 * otherwise array object.
 *
 * Params:
 *  args = the contents to serialize.
 *
 * Returns:
 *  a serialized data.
 */
ubyte[] pack(Args...)(in Args args)
{
    auto packer = packer(SimpleBuffer());

    static if (Args.length == 1) {
        packer.pack(args[0]);
    } else {
        packer.packArray(Args.length);
        packer.pack(args);
    }

    return packer.buffer.data;
}


unittest
{
    auto serialized = pack(false);

    assert(serialized[0] == Format.FALSE);

    auto deserialized = unpack(pack(1, true, "Foo"));

    assert(deserialized.type == mp_Type.ARRAY);
    assert(deserialized.via.array[0].type == mp_Type.POSITIVE_INTEGER);
    assert(deserialized.via.array[1].type == mp_Type.BOOLEAN);
    assert(deserialized.via.array[2].type == mp_Type.RAW);
}


/**
 * Deserializes $(D_PARAM buffer) using stream deserializer.
 *
 * Params:
 *  buffer = the buffer to deserialize.
 *
 * Returns:
 *  a $(D Unpacked) contains deserialized object.
 *
 * Throws:
 *  UnpackException if deserialization doesn't succeed.
 */
Unpacked unpack(Tdummy = void)(in ubyte[] buffer)
{
    auto unpacker = unpacker(buffer);

    if (!unpacker.execute())
        throw new UnpackException("Deserialization failure");

    return unpacker.unpacked;
}


/**
 * Deserializes $(D_PARAM buffer) using direct conversion deserializer.
 *
 * Assumes single object if the length of $(D_PARAM args) == 1,
 * otherwise array object.
 *
 * Params:
 *  buffer = the buffer to deserialize.
 *  args   = the references of values to assign.
 */
void unpack(Args...)(in ubyte[] buffer, ref Args args)
{
    auto unpacker = unpacker!(UnpackerType.DIRECT)(buffer);

    static if (Args.length == 1) {
        unpacker.unpack(args[0]);
    } else {
        unpacker.unpackArray();
        unpacker.unpack(args);
    }
}


unittest
{
    { // stream
        auto result = unpack(pack(false));

        assert(result.via.boolean == false);
    }
    { // direct conversion
        Tuple!(uint, string) result, test = tuple(1, "Hi!");
        
        unpack(pack(test), result);

        assert(result == test);

        test.field[0] = 2;
        test.field[1] = "Hey!";

        unpack(pack(test.field[0], test.field[1]), result.field[0], result.field[1]);

        assert(result == test);
    }
}


// Utilities template


/**
 * Handy helper for creating MessagePackable object.
 *
 * mp_pack/mp_unpack are special methods for serialization/deserialization.
 * This template provides those methods to struct/class.
 *
 * Example:
 * -----
 * struct S
 * {
 *     int num; string str;
 *
 *     // http://d.puremagic.com/issues/show_bug.cgi?id = 1099
 *     mixin MessagePackable;  // all members
 *     // mixin MessagePackable!("num");  // num only
 * }
 * -----
 *
 * Defines those methods manually if you treat complex data-structure.
 */
mixin template MessagePackable(Members...)
{
    static if (Members.length == 0) {
        /**
         * Serializes members using $(D_PARAM packer).
         *
         * Params:
         *  packer = the serializer to pack.
         */
        void mp_pack(Packer)(ref Packer packer) const
        {
            packer.packArray(this.tupleof.length);
            foreach (member; this.tupleof)
                packer.pack(member);
        }


        /**
         * Deserializes $(D MessagePack) object to members for stream deserializer.
         *
         * Params:
         *  object = the MessagePack object to unpack.
         *
         * Throws:
         *  InvalidTypeException if $(D_PARAM object) is not Array type.
         */
        void mp_unpack(mp_Object object)
        {
            if (object.type != mp_Type.ARRAY)
                throw new InvalidTypeException("mp_Object must be Array type");

            foreach (i, member; this.tupleof)
                this.tupleof[i] = object.via.array[i].as!(typeof(member));
        }


        /**
         * Deserializes $(D MessagePack) object to members for direct-conversion deserializer.
         *
         * Params:
         *  object = the reference to direct-conversion deserializer.
         *
         * Throws:
         *  InvalidTypeException if deserialized object size is mismatched.
         */
        void mp_unpack(ref Unpacker!(UnpackerType.DIRECT) unpacker)
        {
            auto length = unpacker.unpackArray();
            if (length != this.tupleof.length)
                throw new InvalidTypeException("Deserialized object size is mismatched");

            foreach (i, member; this.tupleof)
                unpacker.unpack(this.tupleof[i]);
        }
    } else {
        /**
         * Member selecting version of mp_pack.
         */
        void mp_pack(Packer)(ref Packer packer) const
        {
            packer.packArray(Members.length);
            foreach (member; Members)
                packer.pack(mixin(member));
        }


        /**
         * Member selecting version of mp_unpack for stream deserializer.
         */
        void mp_unpack(mp_Object object)
        {
            if (object.type != mp_Type.ARRAY)
                throw new InvalidTypeException("mp_Object must be Array type");

            foreach (i, member; Members)
                mixin(member ~ "= object.via.array[i].as!(typeof(" ~ member ~ "));");
        }


        /**
         * Member selecting version of mp_unpack for direct-converion deserializer.
         */
        void mp_unpack(ref Unpacker!(UnpackerType.DIRECT) unpacker)
        {
            auto length = unpacker.unpackArray();
            if (length != Members.length)
                throw new InvalidTypeException("Deserialized object size is mismatched");

            foreach (member; Members)
                unpacker.unpack(mixin(member));
        }
    }
}


unittest
{
    { // all members
        static struct S
        {
            uint num; string str;
            mixin MessagePackable;
        }

        mixin DefinePacker;

        S orig = S(10, "Hi!"); orig.mp_pack(packer);

        { // stream
            auto unpacker = unpacker(packer.buffer.data); unpacker.execute();

            S result; result.mp_unpack(unpacker.unpacked);

            assert(result.num == 10);
            assert(result.str == "Hi!");
        }
        { // direct conversion
            auto unpacker = unpacker!(UnpackerType.DIRECT)(packer.buffer.data);

            S result; unpacker.unpack(result);

            assert(result.num == 10);
            assert(result.str == "Hi!");
        }
    }
    { // member select
        static class C
        {
            uint num; string str;

            this() {}
            this(uint n, string s) { num = n; str = s; }

            mixin MessagePackable!("num");
        }

        mixin DefinePacker;

        C orig = new C(10, "Hi!"); orig.mp_pack(packer);

        { // stream
            auto unpacker = unpacker(packer.buffer.data); unpacker.execute();

            C result = new C; result.mp_unpack(unpacker.unpacked);

            assert(result.num == 10);
        }
        { // direct conversion
            auto unpacker = unpacker!(UnpackerType.DIRECT)(packer.buffer.data);

            C result; unpacker.unpack(result);

            assert(result.num == 10);
        }
    }
}


private:


// Common and system dependent operations


/*
 * MessagePack type-information format
 *
 * See_Also:
 *  $(LINK2 http://redmine.msgpack.org/projects/msgpack/wiki/FormatSpec, MessagePack Specificaton)
 */
enum Format : ubyte
{
    // unsinged integer
    UINT8  = 0xcc,  // ubyte
    UINT16 = 0xcd,  // ushort
    UINT32 = 0xce,  // uint
    UINT64 = 0xcf,  // ulong

    // signed integer
    INT8  = 0xd0,   // byte
    INT16 = 0xd1,   // short
    INT32 = 0xd2,   // int
    INT64 = 0xd3,   // long

    // floating point
    FLOAT  = 0xca,  // float
    DOUBLE = 0xcb,  // double

    // raw byte
    RAW   = 0xa0,
    RAW16 = 0xda,
    RAW32 = 0xdb,

    // array
    ARRAY   = 0x90,
    ARRAY16 = 0xdc,
    ARRAY32 = 0xdd,

    // map
    MAP   = 0x80,
    MAP16 = 0xde,
    MAP32 = 0xdf,

    // other
    NIL   = 0xc0,   // null
    TRUE  = 0xc3,
    FALSE = 0xc2,

    // real (This format is D only!)
    REAL = 0xd4
}


/*
 * For float type serialization / deserialization
 */
union _f
{
    float f;
    uint  i;
}


/*
 * For double type serialization / deserialization
 */
union _d
{
    double f;
    ulong  i;
}


static if (real.sizeof == 16) {
    /*
     * For real type serialization / deserialization on 128bit environment
     */
    union _r
    {
        real f;

        struct
        {
            ulong fraction;
            ulong exponent;  // includes sign
        }
    }

    enum ES = ulong.sizeof * 8;  // exponent size as bits
} else static if (real.sizeof == 12) {
    /*
     * For real type serialization / deserialization on 96bit environment
     */
    union _r
    {
        real f;

        struct
        {
            ulong fraction;
            uint  exponent;  // includes sign
        }
    }

    enum ES = uint.sizeof * 8;  // exponent size as bits
} else {
    /*
     * For real type serialization / deserialization on 80bit environment
     */
    union _r
    {
        real f;

        struct
        {
            ulong  fraction;
            ushort exponent;  // includes sign
        }
    }

    enum ES = ushort.sizeof * 8;  // exponent size as bits
}


/**
 * Detects whether $(D_PARAM T) is a built-in byte type.
 */
template isByte(T)
{
    enum isByte = staticIndexOf!(Unqual!T, byte, ubyte) >= 0;
}


unittest
{
    static assert(isByte!(byte));
    static assert(isByte!(const(byte)));
    static assert(isByte!(ubyte));
    static assert(isByte!(immutable(ubyte)));
    static assert(!isByte!(short));
    static assert(!isByte!(char));
    static assert(!isByte!(string));
}


version (LittleEndian)
{
    /*
     * Converts $(value) to different Endian.
     *
     * Params:
     *  value = the LittleEndian value to convert.
     *
     * Returns:
     *  the converted value.
     */
    ushort convertEndianTo(size_t Bit, T)(in T value) if (Bit == 16)
    {
        return ntohs(cast(ushort)value);
    }


    // ditto
    uint convertEndianTo(size_t Bit, T)(in T value) if (Bit == 32)
    {
        return ntohl(cast(uint)value);
    }


    // ditto
    ulong convertEndianTo(size_t Bit, T)(in T value) if (Bit == 64)
    {
        // dmd has convert function?
        return ((((cast(ulong)value) << 56) & 0xff00000000000000UL) |
                (((cast(ulong)value) << 40) & 0x00ff000000000000UL) |
                (((cast(ulong)value) << 24) & 0x0000ff0000000000UL) |
                (((cast(ulong)value) <<  8) & 0x000000ff00000000UL) |
                (((cast(ulong)value) >>  8) & 0x00000000ff000000UL) |
                (((cast(ulong)value) >> 24) & 0x0000000000ff0000UL) |
                (((cast(ulong)value) >> 40) & 0x000000000000ff00UL) |
                (((cast(ulong)value) >> 56) & 0x00000000000000ffUL));
    }


    unittest
    {
        assert(convertEndianTo!16(0x0123)             == 0x2301);
        assert(convertEndianTo!32(0x01234567)         == 0x67452301);
        assert(convertEndianTo!64(0x0123456789abcdef) == 0xefcdab8967452301);
    }


    /*
     * Comapatible for BigEndian environment.
     */
    ubyte take8from(size_t bit = 8, T)(T value)
    {
        static if (bit == 8 || bit == 16 || bit == 32 || bit == 64)
            return (cast(ubyte*)&value)[0];
        else
            static assert(false, bit.stringof ~ " is not support bit width.");
    }


    unittest
    {
        foreach (Integer; TypeTuple!(ubyte, ushort, uint, ulong)) {
            assert(take8from!8 (cast(Integer)0x01)               == 0x01);
            assert(take8from!16(cast(Integer)0x0123)             == 0x23);
            assert(take8from!32(cast(Integer)0x01234567)         == 0x67);
            assert(take8from!64(cast(Integer)0x0123456789abcdef) == 0xef);
        }
    }
}
else
{
    /*
     * Comapatible for LittleEndian environment.
     */
    ushort convertEndianTo(size_t Bit, T)(in T value) if (Bit == 16)
    {
        return cast(ushort)value;
    }


    // ditto
    uint convertEndianTo(size_t Bit, T)(in T value) if (Bit == 32)
    {
        return cast(uint)value;
    }


    // ditto
    ulong convertEndianTo(size_t Bit, T)(in T value) if (Bit == 64)
    {
        return cast(ulong)value;
    }


    unittest
    {
        assert(convertEndianTo!16(0x0123)       == 0x0123);
        assert(convertEndianTo!32(0x01234567)   == 0x01234567);
        assert(convertEndianTo!64(0x0123456789) == 0x0123456789);
    }


    /*
     * Takes 8bit from $(D_PARAM value)
     *
     * Params:
     *  value = the content to take.
     *
     * Returns:
     *  the 8bit value corresponding $(D_PARAM bit) width.
     */
    ubyte take8from(size_t bit = 8, T)(T value)
    {
        static if (bit == 8)
            return (cast(ubyte*)&value)[0];
        else static if (bit == 16)
            return (cast(ubyte*)&value)[1];
        else static if (bit == 32)
            return (cast(ubyte*)&value)[3];
        else static if (bit == 64)
            return (cast(ubyte*)&value)[7];
        else
            static assert(false, bit.stringof ~ " is not support bit width.");
    }


    unittest
    {
        foreach (Integer; TypeTuple!(ubyte, ushort, uint, ulong)) {
            assert(take8from!8 (cast(Integer)0x01)               == 0x01);
            assert(take8from!16(cast(Integer)0x0123)             == 0x23);
            assert(take8from!32(cast(Integer)0x01234567)         == 0x67);
            assert(take8from!64(cast(Integer)0x0123456789abcdef) == 0xef);
        }
    }
}


/*
 * Loads $(D_PARAM T) type value from $(D_PARAM buffer).
 *
 * Params:
 *  buffer = the serialized contents.
 *
 * Returns:
 *  the Endian-converted value.
 */
T load16To(T)(ubyte[] buffer)
{
    return cast(T)(convertEndianTo!16(*cast(ushort*)buffer.ptr));
}


// ditto
T load32To(T)(ubyte[] buffer)
{
    return cast(T)(convertEndianTo!32(*cast(uint*)buffer.ptr));
}


// ditto
T load64To(T)(ubyte[] buffer)
{
    return cast(T)(convertEndianTo!64(*cast(ulong*)buffer.ptr));
}
