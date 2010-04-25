// Written in the D programming language.

/**
 * MessagePack for D, some buffer implementation
 *
 * Buffer list:
 * $(UL
 *  $(LI SimpleBuffer)
 *  $(LI DeflationBuffer)
 *  $(LI VRefBuffer)
 *  $(LI BinaryFileWriter)
 * )
 *
 * Some helper functions avoid $(LINK http://d.puremagic.com/issues/show_bug.cgi?id=3438).
 *
 * Copyright: Copyright Masahiro Nakagawa 2010.
 * License:   <a href = "http://www.boost.org/LICENSE_1_0.txt">Boost License 1.0</a>.
 * Authors:   Masahiro Nakagawa
 */
module msgpack.buffer;

import std.array;
import std.stdio;
import std.zlib : ZlibException;  // avoiding Z_* symbols conflict

import etc.c.zlib;

version(Posix)
{
    import core.sys.posix.sys.uio : iovec;
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

version(unittest) import std.file : remove;


@trusted:


/**
 * This alias provides clear name for simple buffer.
 */
alias Appender!(ubyte[]) SimpleBuffer;


/**
 * $(D DeflationBuffer) deflates buffer content using Zlib
 *
 * NOTE:
 *  $(D DeflationBuffer) corresponds to zbuffer of original msgpack.
 */
struct DeflationBuffer
{
  private:
    enum uint RESERVE_SIZE = 512;

    ubyte[]  data_;    // interface buffer
    z_stream stream_;  // zlib-stream for deflation


  public:
    /**
     * Constructs a buffer.
     *
     * Params:
     *  level      = Compression level for deflation.
     *  bufferSize = Initial-value of buffer content.
     *
     * Throws:
     *  $(D ZlibException) if initialization of deflation-stream failed.
     */
    this(in int level = Z_DEFAULT_COMPRESSION, in size_t bufferSize = 8192)
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


    /**
     * Returns available buffer content.
     *
     * Returns:
     *  the slice of deflated buffer.
     */
    @property nothrow ubyte[] data()
    {
        return data_[0..stream_.next_out - data_.ptr];
    }


    /**
     * Flushes the deflation-stream.
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
     * Resets the deflation-stream, but some state will keep.
     *
     * Throws:
     *  $(D ZlibException) if reset of deflation-stream failed.
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
     *  $(D ZlibException) if deflation failed.
     */
    void put(in ubyte value)
    {
        ubyte[1] values = [value];
        put(values);
    }


    /// ditto
    void put(in ubyte[] values)
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
     *  $(D ZlibException) if $(D_PARAM status) isn't $(D Z_OK).
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


/**
 * Helper for $(D DeflationBuffer) construction.
 *
 * Params:
 *  level      = Compression level for deflation.
 *  bufferSize = Initial-value of buffer content.
 *
 * Returns:
 *  a $(D DeflationBuffer) object instantiated and initialized according to the arguments.
 *
 * Throws:
 *  $(D ZlibException) if initialization of deflation-stream failed.
 */
DeflationBuffer deflationBuffer(in int level = Z_DEFAULT_COMPRESSION, in size_t bufferSize = 8192)
{
    return typeof(return)(level, bufferSize);
}


unittest
{
    void check(in int status)
    {
        if (status != Z_OK && status != Z_STREAM_END)
            throw new ZlibException(status);
    }

    scope buffer = deflationBuffer();

    // deflation
    ubyte[] tests = [1, 2];

    foreach (v; tests)
        buffer.put(v);
    buffer.put(tests);
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
struct VRefBuffer
{
  private:
    immutable size_t RefSize, ChunkSize;

    // for putCopy
    ubyte[][] chunk_;  // memory chunk for buffer
    size_t[]  uList_;  // used size list for chunk
    size_t    index_;  // index for cunrrent chunk

    // for putRef
    iovec[] vecList_;   // referece to large data or copied data.


  public:
    /**
     * Constructs a buffer.
     *
     * Params:
     *  refSize   = the threshold of writing value or stores reference.
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
     * Returns the buffer contents excluding references.
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
     *  the array of iovec struct that stores references.
     */
    @property nothrow iovec[] vector()
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


    /*
     * Not implemented yet bacause use case is rarity.
     *
    void migrate(VRefBuffer to);
     */
}


/**
 * Helper for $(D VRefBuffer) construction.
 *
 * Params:
 *  refSize   = the threshold of writing value or storing reference.
 *  chunkSize = the default size of chunk for allocation.
 *
 * Returns:
 *  a $(D VRefBuffer) object instantiated and initialized according to the arguments.
 */
VRefBuffer vrefBuffer(in size_t refSize = 32, in size_t chunkSize = 8192)
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
 *
 * Phobos doesn't have integrated stream($(D std.stream) will be eliminated?).
 * I strongly want the stream implemented Range.
 */
struct BinaryFileWriter
{
  private:
    File         file_;     // stream to write
    bool         isCache_;  // indicates whether caches content
    SimpleBuffer cache_;    // buffer for cache


  public:
    /**
     * Constructs a buffer.
     *
     * Params:
     *  file    = the pointer to $(D File).
     *  isCache = caching content if true.
     */
    this(ref File file, bool isCache = false)
    {
        file_    = file;
        isCache_ = isCache;
    }


    /**
     * Forwards to cache contents.
     *
     * Returns:
     *  the cache contents if isCache is true, otherwise null.
     */
    @property /* nothrow */ ubyte[] data() // data method of Appender isn't nothrow
    {
        return isCache_ ? cache_.data : null;
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
        if (isCache_)
            cache_.put(values);

        if (file_.isOpen)
            file_.rawWrite(values);
        else
            throw new StdioException("File has been closed", 5);  // EIO
    }
}


/**
 * Helper for $(D BinaryFileWriter) construction.
 *
 * Params:
 *  file    = the pointer to $(D File).
 *  isCache = caching content if true.
 *
 * Returns:
 *  a $(D BinaryFileWriter) object instantiated and initialized according to the arguments.
 */
BinaryFileWriter binaryFileWriter(ref File file, bool isCache = false)
{
    return typeof(return)(file, isCache);
}


unittest
{
    auto    name  = "deleteme";
    ubyte[] tests = [1, 2];

    { // output to name file
        auto output = File(name, "wb");
        auto buffer = binaryFileWriter(output, true);

        foreach (v; tests)
            buffer.put(v);
        buffer.put(tests);

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


/*
 * $(D SocketWriter) is a wrapper for $(D Socket).
 *
 * Phobos's socket is broken!
 */
