// Written in the D programming language.

/**
 * DeflateFilter usage
 */

import std.contracts;
import std.file;
import std.stdio;
import std.typecons;

import msgpack.buffer;
import msgpack.packer;
import msgpack.unpacker;


/**
 * Range that reads a chunk at a time. Instead of chunks.
 */
struct ByChunk
{
  private:
    File    file_;
    ubyte[] chunk_;
 
 
  public:
    this(File file, size_t size)
    in    {
        assert(size, "size must be larger than 0");
    }
    body
        {
            file_   =  file;
            chunk_  =  new ubyte[](size);
 
            popFront();
        }
 
    /// Range primitive operations.
    @property bool empty() const
    {
        return !file_.isOpen;
    }
 
    /// ditto
    @property ubyte[] front()
    {
        return chunk_;
    }
 
    /// ditto
    void popFront()
    {
        enforce(file_.isOpen);
 
        chunk_  =  file_.rawRead(chunk_);
        if (!chunk_.length)
            file_.detach();
    }
}


void main()
{
    auto name = "deleteme";
    auto data = tuple(true, "Hi!", 10, [1:1]);;

    { // serialize
        auto deflater = deflateFilter(BinaryFileWriter(name, true));
        auto packer   = packer(deflater);

        packer.pack(data);

        deflater.flush();
        deflater.buffer.close();
    }
    { // deserialize
        ubyte[] buffer;
        auto inflater = deflateFilter(ByChunk(File(name, "rb"), 5));

        foreach (inflated; inflater)
            buffer ~= inflated;

        typeof(data) deserialized;

        unpacker!(UnpackerType.DIRECT)(buffer).unpack(deserialized);

        assert(data == deserialized);
    }

    remove(name);
}
