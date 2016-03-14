module msgpack.attribute;

import std.typetuple; // will use std.meta
import std.traits;


/**
 * Attribute for specifying non pack/unpack field.
 * This is an alternative approach of MessagePackable mixin.
 *
 * Example:
 * -----
 * struct S
 * {
 *     int num;
 *     // Packer/Unpacker ignores this field;
 *     @nonPacked string str;
 * }
 * -----
 */
struct nonPacked {}


package template isPackedField(alias field)
{
    enum isPackedField = (staticIndexOf!(nonPacked, __traits(getAttributes, field)) == -1) && (!isSomeFunction!(typeof(field)));
}
