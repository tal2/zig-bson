const std = @import("std");
const builtin = @import("builtin");
const bson = @import("bson.zig");
const utils = @import("utils.zig");
const datetime = @import("datetime.zig");
const Decimal128 = @import("binary_coded_decimal");
const bson_types = @import("bson-types.zig");

const Allocator = std.mem.Allocator;
const BsonDocument = bson.BsonDocument;
const assert = std.debug.assert;
const Writer = std.io.Writer;
const Reader = std.io.Reader;

const native_endian = builtin.cpu.arch.endian();

const ElementType = bson_types.BsonElementType;
const BsonSubType = bson_types.BsonSubType;
const BsonObjectId = bson_types.BsonObjectId;
const BsonDecimal128 = bson_types.BsonDecimal128;

pub const NullIgnoredFieldNames = union(enum) {
    pub const name_as_field = "null_ignored_field_names";

    all_optional_fields: void,
    named_optional_fields: []const []const u8,
};

pub const BsonAppendError = std.fmt.BufPrintError || Allocator.Error || Writer.Error || error{InvalidObjectId};


pub fn writeToBson(comptime T: type, obj: T, allocator: Allocator) BsonAppendError!*BsonDocument {
    var data_writer: std.io.Writer.Allocating = try .initCapacity(allocator, 1024); // TODO: capacity
    errdefer data_writer.deinit();

    const writer = &data_writer.writer;
    try appendDocumentToBson(T, obj, writer);
    const bson_doc = try allocator.create(BsonDocument);

    bson_doc.raw_data = try data_writer.toOwnedSlice();
    bson_doc.len = bson_doc.raw_data.len;
    return bson_doc;
}

pub fn appendDocumentToBson(comptime T: type, obj: T, data_writer: *Writer) BsonAppendError!void {
    try writeToBsonWriter(T, obj, data_writer);
}

pub fn writeToBsonWriter(comptime T: type, obj: T, writer: *Writer) !void {
    const type_info = @typeInfo(T);

    switch (type_info) {
        .@"struct" => {
            // continue
        },
        .pointer => {
            return writeToBsonWriter(type_info.pointer.child, obj.*, writer);
        },
        .optional => {
            const value = obj.?;
            return writeToBsonWriter(@TypeOf(value), value, writer);
        },
        .@"union" => {
            @compileLog(@typeName(T));
            @compileError("not yet implemented");
        },
        else => {
            @compileLog("not supported: " ++ @typeName(T));
            @compileError("type not supported");
        },
    }

    if (T == bson.BsonDocument) {
        try writer.writeAll(obj.raw_data);
        return;
    }

    const start_pos = writer.end;

    try appendDocumentLenPlaceholder(writer);

    inline for (type_info.@"struct".fields) |field| {
        if (!std.mem.eql(u8, field.name, NullIgnoredFieldNames.name_as_field)) {
            const field_value_pre = @field(obj, field.name);
            const field_type_info = comptime @typeInfo(field.type);
            const is_optional = comptime field_type_info == .optional;

            if (is_optional and field_value_pre == null) {
                const keep_null_field = comptime keep_null_field: {
                    if (@hasDecl(T, NullIgnoredFieldNames.name_as_field)) {
                        const null_ignored_field_names: NullIgnoredFieldNames = @field(T, NullIgnoredFieldNames.name_as_field);

                        switch (null_ignored_field_names) {
                            .all_optional_fields => {
                                break :keep_null_field false;
                            },
                            .named_optional_fields => |named_fields| {
                                for (named_fields) |null_ignored_field_name| {
                                    if (std.mem.eql(u8, null_ignored_field_name, field.name)) {
                                        break :keep_null_field false;
                                    }
                                }
                            },
                        }
                    }
                    break :keep_null_field true;
                };

                if (keep_null_field) {
                    try appendElementType(writer, ElementType.null);
                    const field_name = field.name;
                    try appendString(writer, field_name, false, true);
                }
            } else {
                const field_value = if (is_optional) field_value_pre.? else field_value_pre;
                const field_name = field.name;

                if (comptime @typeInfo(@TypeOf(field_value)) == .@"union") {
                    const field_value_type_info = comptime @typeInfo(field_type_info.optional.child);
                    inline for (field_value_type_info.@"union".fields) |union_field_info| {
                        if (std.mem.eql(u8, @tagName(field_value), union_field_info.name)) {
                            const union_value = @field(field_value, union_field_info.name);
                            const union_field_element_type = comptime ElementType.typeToElementType(union_field_info.type);
                            try appendElementType(writer, union_field_element_type);
                            try appendString(writer, field_name, false, true);
                            try appendElementValue(writer, union_field_element_type, union_field_info.type, union_value);
                        }
                    }
                } else {
                    const field_element_type = comptime ElementType.typeToElementType(field.type);

                    try appendElementType(writer, field_element_type);
                    try appendString(writer, field_name, false, true);
                    try appendElementValue(writer, field_element_type, field.type, field_value);
                }
            }
        }
    }
    try appendNullTerminator(writer);
    const document_len = writer.end - start_pos;
    updateLenMarker(writer.buffer, start_pos, document_len);
}

pub inline fn appendElementType(writer: *Writer, element_type: ElementType) Writer.Error!void {
    const value: i8 = @intFromEnum(element_type);

    const value_byte: u8 = @bitCast(if (native_endian == .little) value else @byteSwap(value)); // TODO: verify
    try writer.writeByte(value_byte);
}

pub inline fn appendElementValue(writer: *Writer, field_element_type: ElementType, FieldType: type, field_value: anytype) BsonAppendError!void {
    switch (field_element_type) {
        .int32 => {
            if (@TypeOf(field_value) == usize or @TypeOf(field_value) == u32) {
                try appendInt32(writer, @as(i32, @intCast(field_value)));
            } else {
                try appendInt32(writer, field_value);
            }
        },
        .int64 => {
            if (@TypeOf(field_value) == usize or @TypeOf(field_value) == u64) {
                try appendInt64(writer, @as(i64, @intCast(field_value)));
            } else {
                try appendInt64(writer, field_value);
            }
        },
        .double => {
            //TODO: test
            try appendDouble(writer, field_value);
        },
        .decimal128 => {
            try appendDecimal128(writer, field_value);
        },
        .string => {
            try appendString(writer, field_value, true, true);
        },
        .binary => {
            try appendBinary(writer, field_value.sub_type, field_value.value);
        },
        .document => {
            try appendDocumentToBson(FieldType, field_value, writer);
        },
        .array => {
            const start_pos = writer.end;
            try appendDocumentLenPlaceholder(writer);
            for (field_value, 0..) |item, item_index| {
                const array_item_type = comptime ElementType.typeToElementType(@TypeOf(item));
                try appendElementType(writer, array_item_type);

                try appendIntAsString(usize, writer, item_index, false, true);
                try appendElementValue(writer, array_item_type, @TypeOf(item), item);
            }
            try appendNullTerminator(writer);
            const end_pos = writer.end;
            const len = end_pos - start_pos;
            updateLenMarker(writer.buffer, start_pos, len);
        },
        .timestamp => {
            try appendUint64(writer, field_value.value);
        },
        .utc_date_time => {
            try appendInt64(writer, field_value.value);
        },
        .min_key, .max_key, .null => {
            // no value
        },
        .boolean => {
            try appendByte(writer, @as(u8, if (field_value) 1 else 0));
        },
        .object_id => {
            try writer.writeAll(&field_value.value);
        },
        else => {
            @panic("Unsupported type: " ++ @typeName(FieldType));
        },
    }
}

pub inline fn appendNullTerminator(writer: *Writer) Writer.Error!void {
    try writer.writeByte(0);
}

pub inline fn appendByte(writer: *Writer, value: u8) Writer.Error!void {
    var value_bytes = std.mem.toBytes(value);
    value_bytes = @bitCast(if (native_endian == .little) value_bytes else @byteSwap(value_bytes));

    try writer.writeAll(&value_bytes);
}

pub inline fn appendInt32(writer: *Writer, value: i32) Writer.Error!void {
    try appendNumber(writer, value);
}

pub inline fn appendInt64(writer: *Writer, value: i64) Writer.Error!void {
    try appendNumber(writer, value);
}

pub inline fn appendUint64(writer: *Writer, value: u64) Writer.Error!void {
    try appendNumber(writer, value);
}

pub inline fn appendDouble(writer: *Writer, value: f64) Writer.Error!void {
    try appendNumber(writer, value);
}

inline fn appendNumber(writer: *Writer, value: anytype) Writer.Error!void {
    comptime utils.assertIsNumber(@TypeOf(value));
    var value_bytes = std.mem.toBytes(value);
    value_bytes = @bitCast(if (native_endian == .little) value_bytes else @byteSwap(value_bytes));

    _ = try writer.write(&value_bytes);
}

pub inline fn appendObjectId(writer: *Writer, value: []const u8) BsonAppendError!void {
    if (value.len != BsonObjectId.bson_object_id_as_string_size) {
        return BsonAppendError.InvalidObjectId;
    }
    var buffer: [BsonObjectId.bson_object_id_size]u8 = undefined;
    const value_bytes = std.fmt.hexToBytes(&buffer, value) catch return BsonAppendError.InvalidObjectId;
    try writer.writeAll(value_bytes);
}

pub inline fn appendNumberFromString(writer: *Writer, value: []const u8) (Writer.Error || std.fmt.ParseFloatError)!ElementType {
    const i32_bytes_len: usize = (if (value[0] == '-') 11 else 10);
    if (value.len <= i32_bytes_len) {
        const num = std.fmt.parseInt(i32, value, 10) catch {
            const num = try std.fmt.parseFloat(f64, value);
            try appendNumber(writer, num);
            return ElementType.double;
        };

        try appendInt32(writer, num);
        return ElementType.int32;
    } else {
        const num = std.fmt.parseInt(i64, value, 10) catch {
            const num = try std.fmt.parseFloat(f64, value);
            try appendNumber(writer, num);
            return ElementType.double;
        };
        try appendInt64(writer, num);
        return ElementType.int64;
    }
    unreachable;
}

pub inline fn appendDecimal128(writer: *Writer, value: *BsonDecimal128) Writer.Error!void {
    try value.writeAsBytes(writer);
}

pub fn appendIntAsString(comptime T: type, writer: *Writer, value: T, comptime add_len_prefix: bool, comptime add_null_terminator: bool) Writer.Error!void {
    comptime utils.assertIsInt(T);
    const max_len = @sizeOf(T) * 2;
    var buf: [max_len]u8 = undefined;
    const value_as_string = std.fmt.bufPrint(&buf, "{d}", .{value}) catch unreachable;
    return try appendString(writer, value_as_string, add_len_prefix, add_null_terminator);
}

pub fn appendString(writer: *Writer, value: []const u8, comptime add_len_prefix: bool, comptime add_null_terminator: bool) Writer.Error!void {
    if (add_len_prefix) {
        const value_len_as_int32 = @as(i32, @intCast(value.len + if (add_null_terminator) 1 else 0));
        try appendInt32(writer, value_len_as_int32);
    }

    if (value.len > 0) {
        try writer.writeAll(value);
    }
    if (add_null_terminator) {
        try appendNullTerminator(writer);
    }
}

pub fn appendBinary(writer: *Writer, sub_type: BsonSubType, value: []const u8) Writer.Error!void {
    const value_len_as_int32 = @as(i32, @intCast(value.len));
    const sub_type_value: u8 = @intFromEnum(sub_type);

    if (sub_type == .binary_old) { // see spec notes: https://bsonspec.org/spec.html
        @branchHint(.unlikely);
        try appendInt32(writer, value_len_as_int32 + @sizeOf(i32));
        try writer.writeByte(sub_type_value);
        try appendInt32(writer, value_len_as_int32);
        try writer.writeAll(value);

        return;
    }

    try appendInt32(writer, value_len_as_int32);
    try writer.writeByte(sub_type_value);
    try writer.writeAll(value);
}

pub fn appendBinaryDecodedBase64(writer: *Writer, sub_type: BsonSubType, encoded_value: []const u8) (Writer.Error || std.base64.Error)!void {
    const expected_size = try std.base64.standard.Decoder.calcSizeForSlice(encoded_value);
    const value_len_as_int32 = @as(i32, @intCast(expected_size));
    const sub_type_value: u8 = @intFromEnum(sub_type);

    if (sub_type == .binary_old) { // see spec notes: https://bsonspec.org/spec.html
        @branchHint(.unlikely);
        try appendInt32(writer, value_len_as_int32 + @sizeOf(i32));
        try writer.writeByte(sub_type_value);
        try appendInt32(writer, value_len_as_int32);
        const expected_data = try writer.writableSlice(expected_size);
        try std.base64.standard.Decoder.decode(expected_data, encoded_value);
        return;
    }

    try appendInt32(writer, value_len_as_int32);
    try writer.writeByte(sub_type_value);
    const expected_data = try writer.writableSlice(expected_size);
    try std.base64.standard.Decoder.decode(expected_data, encoded_value);
}

pub inline fn appendDocumentLenPlaceholder(writer: *Writer) Writer.Error!void {
    try appendInt32(writer, 0);
}

pub fn overwriteDocumentLenPlaceholder(raw_data: []u8) void {
    return updateLenMarker(raw_data, 0, raw_data.len);
}

pub fn updateLenMarker(raw_data: []u8, at: usize, len: usize) void {
    var bson_new_len_marker = std.mem.toBytes(@as(i32, @intCast(len)));
    bson_new_len_marker = @bitCast(if (native_endian == .little) bson_new_len_marker else @byteSwap(bson_new_len_marker));

    @memcpy(raw_data[at .. at + @sizeOf(i32)], &bson_new_len_marker);
}

pub fn overwriteElementType(raw_data: []u8, at: usize, element_type: ElementType) void {
    var element_type_bytes = std.mem.toBytes(@as(i8, @intFromEnum(element_type)));
    element_type_bytes = @bitCast(if (native_endian == .little) element_type_bytes else @byteSwap(element_type_bytes));

    @memcpy(raw_data[at .. at + @sizeOf(i8)], &element_type_bytes);
}
