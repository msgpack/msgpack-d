// Written in the D programming language.

/**
 * MessagePack for D, some buffer implementation
 *
 * Implementation list:
 * $(UL
 *  $(LI SimpleBuffer)
 *  $(LI VRefBuffer)
 *  $(LI BinaryFileWriter)
 *  $(LI DeflationFilter)
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
import std.range;
import std.stdio;
import std.traits;
import std.zlib : ZlibException;  // avoiding Z_* symbols conflict

import etc.c.zlib;  // for DeflationFilter

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
    iovec[] vecList_;  // referece to large data or copied data.


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
 * This filter compresses data using Deflate algorithm.
 *
 * This implementation uses etc.c.zlib module. This filter is a OutputRange
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
        @property nothrow ref Buffer buffer()
        {
            return buffer_;
        }
    } else {
        @property nothrow Buffer buffer()
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
 * This filter uncompresses data using Deflate algorithm.
 *
 * This implementation uses etc.c.zlib module. This filter is a InputRange.
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
        @property nothrow ref Buffer buffer()
        {
            return buffer_;
        }
    } else {
        @property nothrow Buffer buffer()
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
