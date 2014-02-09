[![Build Status](https://travis-ci.org/msgpack/msgpack-d.png)](https://travis-ci.org/msgpack/msgpack-d)

# MessagePack for D

MessagePack is a binary-based JSON-like serialization library.

MessagePack for D is a pure D implementation of MessagePack.

# Features

* Small and High performance
* Zero copy serialization / deserialization
* Stream deserializer / Direct-conversion deserializer
* Support D features(Range, Tuple, real type)

real type is D only. Don't use real-type to communicate other languages.
In addition, Unpacker raises exception if loss of precision occures.

## Limitations

* No circular references support

# Install

msgpack-d is only one file. Please copy src/msgpack.d onto your project or use dub.

```sh
% dub install msgpack-d
```

# Usage

Actual codes are in the example directory and DDoc is [here](http://msgpack.github.io/msgpack-d/)

## pack / unpack

msgpack-d is very simple to use. `pack` for serialization and `unpack` for deserialization:

```D
import std.file;
import msgpack;

struct S { int x; float y; string z; }

void main()
{
    S input = S(10, 25.5, "message");

    // serialize data
    ubyte[] inData = pack(input);

    // write data to a file
    write("file.dat", inData);

    // read data from file
    ubyte[] outData = cast(ubyte[])read("file.dat");

    // unserialize data
    S target = outData.unpack!S();

    // verify data is the same
    assert(target.x == input.x);
    assert(target.y == input.y);
    assert(target.z == input.z);
}
```

### Skip specific field in `pack` / `unpack`.

Use `@nonPacked` attribute.

```d
struct Foo
{
    string f1;
    @nonPacked int f2;  // pack / unpack ignore f2 field
}
```

### Use own (de)serialization routine for class and struct

msgpack-d provide `registerPackHandler` / `registerUnpackHandler` functions.
It is useful for derived class through reference to base class serialization.

```d
class A { }
class C : A 
{
    int num;
    // ...
}

void cPackHandler(ref Packer p, ref C c)
{
    p.pack(c.num);
}

void cUnpackHandler(ref Unpacker u, ref C c)
{
    u.unpack(c.num);
}

// Set cPackHandler and cUnpackHandler for C instance
registerPackHandler!(C, cPackHandler);
registerUnpackHandler!(C, cUnpackHandler);

// can (de)serialize C instance via base class reference
A c = new C(1000);
auto data = pack(c);
A c2 = new C(1);
unpack(data, c2); // c2.num is 1000
```

## Packer / Unpacker / StreaminUnpacker

These classes are used in `pack` and `unpack` internally.

See DDoc of [Packer](http://msgpack.github.io/msgpack-d/#Packer), [Unpacker](http://msgpack.github.io/msgpack-d/#Unpacker) and [StreamingUnpacker](http://msgpack.github.io/msgpack-d/#StreamingUnpacker) for more detail.

# Link

* [The MessagePack Project](http://msgpack.org/)

  MessagePack official site

* [MessagePack's issues](https://github.com/msgpack/msgpack-d/issues)

  Github issue

* [MessagePack's Github](http://github.com/msgpack/)

  Other language versions are here

# Copyright

    Copyright (c) 2010- Masahiro Nakagawa

Distributed under the Boost Software License, Version 1.0.
