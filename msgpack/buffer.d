// Written in the D programming language.

/**
 * MessagePack for D, some buffer implementation
 *
 * Buffer
 *  - SimpleBuffer
 *  - DeflationBuffer
 *  - VRefBuffer
 *  - FileBuffer
 *
 * Copyright: Copyright Masahiro Nakagawa 2010.
 * License:   <a href = "http://www.boost.org/LICENSE_1_0.txt">Boost License 1.0</a>.
 * Authors:   Masahiro Nakagawa
 */
module msgpack.buffer;

import std.stdio : File;
import std.zlib  : ZlibException;

import etc.c.zlib;

version(Posix)
{
    public import core.sys.posix.sys.uio : iovec;
}
else
{
    /**
     * from core.sys.posix.sys.uio.iovec.
     */
    struct iovec
    {
        void*  iov_base;
        size_t iov_len;
    }
}


/**
 * $(D Buffer) is a interface for byte stream
 */
interface Buffer
{
    /**
     * Forwards to buffer content.
     *
     * Returns:
     *  the available content of buffer.
     */
    @property ubyte[] data();

    /**
     * Writes $(D_PARAM value) to buffer.
     *
     * Params:
     *  value = the content to write.
     */
    void write(in ubyte value);

    /**
     * ditto
     */
    void write(in ubyte[] values)
    in
    {
        assert(values);
    }
}


/**
 * $(D SimpleBuffer) is a wrapper for ubyte array
 */
class SimpleBuffer : Buffer
{
  private:
    ubyte[] data_;  // internal buffer


  public:
    @property nothrow ubyte[] data()
    {
        return data_;
    }

    void write(in ubyte value)
    {
        data_ ~= value;
    }

    void write(in ubyte[] values)
    {
        data_ ~= values;
    }
}


unittest
{
    auto buffer = new SimpleBuffer;

    ubyte[] tests = [1, 2];

    foreach (v; tests)
        buffer.write(v);
    assert(buffer.data == tests);

    buffer.write(tests);
    assert(buffer.data ==  tests ~ tests);
}


/**
 * $(D DeflationBuffer) deflates buffer content using Zlib
 *
 * NOTE:
 *  $(D DeflationBuffer) corresponds to zbuffer of original msgpack.
 */
class DeflationBuffer : Buffer
{
  private:
    enum uint RESERVE_SIZE = 512;

    ubyte[]  data_;    // interface buffer
    z_stream stream_;  // zlib stream for deflation


  public:
    /**
     * Constructs a buffer.
     *
     * Params:
     *  level      = Compression level for deflation.
     *  bufferSize = Initial-value of buffer content.
     *
     * Throws:
     *  ZlibException, if initialization of deflation-stream failed.
     */
    this(in int level = Z_DEFAULT_COMPRESSION, in size_t bufferSize = RESERVE_SIZE * 4)
    in
    {
        assert(level == Z_NO_COMPRESSION   ||
               level == Z_BEST_SPEED       ||
               level == Z_BEST_COMPRESSION ||
               level == Z_DEFAULT_COMPRESSION);
    }
    body
    {
        check(deflateInit(&stream_, level));

        data_.length      = bufferSize;
        stream_.next_out  = data_.ptr;
        stream_.avail_out = bufferSize;
    }

    /**
     * Destructs a buffer.
     */
    ~this()
    {
        deflateEnd(&stream_);
    }

    @property nothrow ubyte[] data()
    {
        return data_[0..stream_.next_out - data_.ptr];
    }

    /**
     * Flushes the deflation stream.
     *
     * Returns:
     *  the buffer content if succeed, otherwise null.
     */
    ubyte[] flush()
    {
        while (true) {
            switch (deflate(&stream_, Z_FINISH)) {
            case Z_STREAM_END:
                return data;
            case Z_OK:
                expand();
                break;
            default:
                return null;
            }
        }
    }

    /**
     * Resets the deflation stream, but some state will keep.
     *
     * Throws:
     *  ZlibException, if reset of deflation-stream failed.
     */
    void reset()
    {
        check(deflateReset(&stream_));

        stream_.avail_out += stream_.next_out - data_.ptr;
        stream_.next_out   = data_.ptr;
    }

    /**
     * Writes $(D_PARAM value) to buffer with deflation.
     *
     * Params:
     *  value = the content to write.
     *
     * Throws:
     *  ZlibException, if deflation failed.
     */
    void write(in ubyte value)
    {
        ubyte[1] values = [value];
        write(values);
    }

    /**
     * ditto
     */
    void write(in ubyte[] values)
    {
        stream_.next_in  = cast(ubyte*)values.ptr;
        stream_.avail_in = values.length;

        do {
            if (stream_.avail_out < RESERVE_SIZE)
                expand();

            check(deflate(&stream_, Z_NO_FLUSH));
        } while (stream_.avail_in > 0)
    }


  private:
    /**
     * Checks stream status.
     *
     * Params:
     *  status = return code from zlib function.
     *
     * Throws:
     *  ZlibException, if $(D_PARAM status) isn't $(D_Z_OK).
     */
    void check(in int status)
    {
        if (status != Z_OK)
            throw new ZlibException(status);
    }

    /**
     * Expands internal buffer.
     */
    void expand()
    {
        const used = stream_.next_out - data_.ptr;

        data_.length *= 2;

        stream_.next_out  = data_.ptr    + used;
        stream_.avail_out = data_.length - used;
    }
}


unittest
{
    void check(in int status)
    {
        if (status != Z_OK && status != Z_STREAM_END)
            throw new ZlibException(status);
    }

    scope buffer = new DeflationBuffer;

    // deflation
    ubyte[] tests = [1, 2];
    foreach (v; tests)
        buffer.write(v);
    buffer.write(tests);
    buffer.flush;

    // inflation
    z_stream stream;
    ubyte[]  result = new ubyte[](4);

    check(inflateInit(&stream));

    stream.next_in   = buffer.data.ptr;
    stream.avail_in  = buffer.data.length;
    stream.next_out  = result.ptr;
    stream.avail_out = result.length;

    check(inflate(&stream, Z_FINISH));

    inflateEnd(&stream);

    assert(result == tests ~ tests);
}


/**
 * $(D VRefBuffer) is a zero copy buffer for more efficient
 *
 * See_Also:
 *  $(LINK http://msgpack.sourceforge.net/doc:introduction#zerocopy_serialization)
 *
 * NOTE:
 *  Current implementation is dynamic-array based allocation.
 *  This is better but not fastest? malloc(3) has fastbins at 72 byte align,
 *  but dynamic-array relies on GC.
 */
class VRefBuffer : Buffer
{
  private:
    immutable size_t RefSize, ChunkSize;

    // for writeCopy
    ubyte[][] chunk_;  // memory chunk for buffer
    size_t[]  uList_;  // used size list for chunk
    size_t    index_;  // index for cunrrent chunk

    // for writeRef
    iovec[] vector_;   // referece to large data or copied data.


  public:
    /**
     * Constructs a buffer.
     *
     * Params:
     *  refSize   = the threshold for writing value or reference to buffer.
     *  chunkSize = the default size of chunk for allocation.
     */
    this(in size_t refSize = 32, in size_t chunkSize = 8192)
    {
        RefSize   = refSize;
        ChunkSize = chunkSize;

        uList_.length = 1;
        chunk_.length = 1;
        chunk_[index_].length = chunkSize;
    }

    /**
     * Forwards to buffer contents excluding references.
     *
     * Returns:
     *  the non-contiguous copied contents.
     */
    @property nothrow ubyte[] data()
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
     *  the array of iovec struct that references the contents.
     */
    @property nothrow iovec[] vector()
    {
        return vector_;
    }

    void write(in ubyte value)
    {
        ubyte[1] values = [value];
        writeCopy(values);
    }

    void write(in ubyte[] values)
    {
        if (values.length < RefSize)
            writeCopy(values);
        else
            writeRef(values);
    }


  private:
    /**
     * Writes reference of $(D_PARAM values) to buffer.
     *
     * Params:
     *  values = the content to write.
     */
    void writeRef(in ubyte[] values)
    {
        vector_.length += 1;
        vector_[$ - 1]  = iovec(cast(void*)values.ptr, values.length);
    }

    /**
     * Writes $(D_PARAM values) to buffer and appends to reference.
     *
     * Params:
     *  values = the contents to write.
     */
    void writeCopy(in ubyte[] values)
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
        if (vector_.length && data.ptr == (vector_[$ - 1].iov_base +
                                           vector_[$ - 1].iov_len))
            vector_[$ - 1].iov_len += size;
        else
            writeRef(data);
    }

    /**
     * Not implemented bacause use case is rarity.
     *
    void migrate(VRefBuffer to);
     */
}


unittest
{
    auto buffer = new VRefBuffer(2, 4);

    ubyte[] tests = [1, 2];
    foreach (v; tests)
        buffer.write(v);
    buffer.write(tests);

    assert(buffer.data == tests, "writeCopy failed");

    iovec[] vector = buffer.vector;
    ubyte[] result;

    assert(vector.length == 2, "Optimization failed");

    foreach (v; vector)
        result ~= (cast(ubyte*)v.iov_base)[0..v.iov_len];

    assert(result == tests ~ tests);
}


/**
 * FileBuffer is a wrapper for std.stdio.File
 *
 * Phobos doesn't have integrated stream(std.stream will be eliminated?).
 * I strongly want the stream implemented Range(File, Socket, etc...).
 */
class FileBuffer : Buffer
{
  private:
    File*        file_;     // stream to write
    bool         isCache_;  // indicates whether caches content
    SimpleBuffer cache_;    // buffer for cache


  public:
    /**
     * Constructs a buffer.
     *
     * Params:
     *  file    = the pointer to File.
     *  isCache = caches content if true.
     */
    this(File* file, bool isCache = false)
    {
        file_    = file;
        isCache_ = isCache;
        if (isCache)
            cache_ = new SimpleBuffer;
    }

    /**
     * Forwards to cache contents.
     *
     * Returns:
     *  the cache contents if isCache is true, otherwise null.
     */
    @property nothrow ubyte[] data()
    {
        return isCache_ ? cache_.data : null;
    }

    void write(in ubyte value)
    {
        ubyte[1] values = [value];
        write(values);
    }

    void write(in ubyte[] values)
    {
        if (isCache_)
            cache_.write(values);

        file_.rawWrite(values);
    }
}


version(unittest) import std.file : remove;

unittest
{
    auto    name  = "deleteme";
    ubyte[] tests = [1, 2];

    { // output to name file
        auto output = File(name, "wb");
        auto buffer = new FileBuffer(&output, true);

        foreach (v; tests)
            buffer.write(v);
        buffer.write(tests);

        assert(buffer.data == tests ~ tests);
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
 * $(D SocketBuffer) is a wrapper for Socket
class SocketBuffer : Buffer {}
 */
