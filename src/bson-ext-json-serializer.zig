const std = @import("std");
const bson = @import("bson.zig");
const utils = @import("utils.zig");
const datetime = @import("datetime.zig");
const Decimal128 = @import("binary_coded_decimal");
const bson_types = @import("bson-types.zig");

const ArrayWriter = std.ArrayList(u8).Writer;
const Writer = std.io.Writer;
const Reader = std.io.Reader;
const Limit = std.Io.Limit;

const assert = std.debug.assert;

const ElementType = bson_types.BsonElementType;
const BsonSubType = bson_types.BsonSubType;
const regexp_max_len = 1024;

pub const WriteJsonStringError = error{
    InvalidBooleanValue,
    BinaryFieldRequiresStrictExtJson,
    IncompleteDocument,
};
pub const FloatFormatCanonicalExtendedJsonError = Writer.Error || std.fmt.float.Error || error{OutOfMemory};

pub fn toJsonString(doc: *const bson.BsonDocument, allocator: std.mem.Allocator, comptime is_strict_ext_json: bool) ![]const u8 {
    var allocating_writer = try Writer.Allocating.initCapacity(allocator, 1024);
    errdefer allocating_writer.deinit();

    var reader = Reader.fixed(doc.raw_data);

    const writer = &allocating_writer.writer;
    try appendDocumentToJsonString(&reader, writer, is_strict_ext_json);

    return allocating_writer.toOwnedSlice();
}

pub fn appendDocumentToJsonString(reader: *Reader, writer: *Writer, comptime is_strict_ext_json: bool) anyerror!void {
    const offset = @as(i32, @intCast(reader.seek));

    const total_document_size = try reader.takeInt(i32, .little);
    if (total_document_size == 0) {
        try writer.writeAll("{}");
        try skipDocumentTerminatingByte(reader);
        return;
    }

    var i: usize = 0;
    const eof_pos = reader.end;

    try writer.writeByte('{');

    const end_doc_pos = offset + total_document_size - 1;
    if (end_doc_pos >= eof_pos) {
        return WriteJsonStringError.IncompleteDocument;
    }

    while (reader.seek < end_doc_pos) : (i += 1) {
        if (i > 0) {
            try writer.writeByte(',');
        }

        const element_type = try readElementTypeFromBson(reader);
        try appendENameToJsonString(writer, reader);

        try appendElementValueToJsonString(writer, reader, element_type, is_strict_ext_json);
    }
    try writer.writeByte('}');

    try skipDocumentTerminatingByte(reader);
}

fn appendENameToJsonString(writer: *Writer, reader: *Reader) !void {
    try writer.writeByte('"');
    _ = try reader.streamDelimiterLimit(writer, 0x00, Limit.limited(1024)); // TODO: handle error
    const last_byte = try reader.takeByte();
    assert(last_byte == 0);

    try writer.writeAll("\":");
}

fn appendElementValueToJsonString(writer: *Writer, reader: *Reader, element_type: ElementType, comptime is_strict_ext_json: bool) !void {
    switch (element_type) {
        .string => try appendStringToJsonString(writer, reader),
        .int32 => try appendInt32ToJsonString(writer, reader, is_strict_ext_json),
        .int64 => try appendInt64ToJsonString(writer, reader, is_strict_ext_json),
        .double => try appendDoubleToJsonString(writer, reader, is_strict_ext_json),
        .decimal128 => try appendDecimal128ToJsonString(writer, reader),
        .timestamp => try appendTimestampToJsonString(writer, reader),
        .utc_date_time => try appendUtcDateTimeToJsonString(writer, reader, is_strict_ext_json),
        .null => try appendNullToJsonString(writer),
        .boolean => try appendBooleanToJsonString(writer, reader),
        .document => try appendDocumentToJsonString(reader, writer, is_strict_ext_json),
        .array => try appendArrayToJsonString(writer, reader, is_strict_ext_json),
        .binary => if (is_strict_ext_json)
            try appendBinaryToJsonString(writer, reader)
        else
            return WriteJsonStringError.BinaryFieldRequiresStrictExtJson,
        .object_id => try appendObjectIdToJsonString(writer, reader),
        .min_key => try appendMinKeyToJsonString(writer),
        .max_key => try appendMaxKeyToJsonString(writer),
        .regexp => try appendRegexpToJsonString(writer, reader),
        else => {
            return error.ElementTypeNotSupported;
        },
    }
}

fn appendArrayToJsonString(writer: *Writer, reader: *Reader, comptime is_strict_ext_json: bool) anyerror!void {
    const sub_offset = @as(i32, @intCast(reader.seek));
    const sub_document_size = try reader.takeInt(i32, .little);
    try writer.writeByte('[');
    if (sub_document_size > 0) {
        const end_pos_of_last_array_item = sub_document_size + sub_offset - 1;
        var item_index: usize = 0;
        while (reader.seek < end_pos_of_last_array_item) : (item_index += 1) {
            if (item_index > 0) {
                try writer.writeByte(',');
            }
            const array_item_element_type = try readElementTypeFromBson(reader);
            try skipArrayItemName(reader);

            try appendElementValueToJsonString(writer, reader, array_item_element_type, is_strict_ext_json);
        }

        try skipArrayTerminatingByte(reader);
    }
    try writer.writeByte(']');
}

fn appendStringToJsonString(writer: *Writer, reader: *Reader) !void {
    const num = try reader.takeInt(i32, .little);
    const str_expected_len = @as(usize, @intCast(num));
    assert(str_expected_len > 0);

    const str = try reader.take(str_expected_len - 1);
    const b = try reader.takeByte();
    assert(b == 0);

    try std.json.Stringify.encodeJsonString(str, .{ .escape_unicode = false }, writer);
}

fn appendInt32ToJsonString(writer: *Writer, reader: *Reader, comptime is_strict_ext_json: bool) !void {
    const num = try reader.takeInt(i32, .little);
    if (is_strict_ext_json) {
        try writer.print("{{\"$numberInt\":\"{d}\"}}", .{num});
    } else {
        try writer.print("{d}", .{num});
    }
}

fn appendInt64ToJsonString(writer: *Writer, reader: *Reader, comptime is_strict_ext_json: bool) !void {
    const num = try reader.takeInt(i64, .little);
    if (is_strict_ext_json) {
        try writer.print("{{\"$numberLong\":\"{d}\"}}", .{num});
    } else {
        try writer.print("{d}", .{num});
    }
}

fn appendDecimal128ToJsonString(writer: *Writer, reader: *Reader) !void {
    try writer.writeAll("{\"$numberDecimal\":\"");
    try Decimal128.readAndEncode(reader, writer);
    try writer.writeAll("\"}");
}

fn appendDoubleToJsonString(writer: *Writer, reader: *Reader, comptime is_strict_ext_json: bool) !void {
    const num_bytes = try reader.take(@sizeOf(f64));
    const num = std.mem.bytesToValue(f64, num_bytes);
    if (is_strict_ext_json) {
        try writer.writeAll("{\"$numberDouble\":\"");
    }
    try writeFloatAsCanonicalExtendedJsonString(writer, num);

    if (is_strict_ext_json) {
        try writer.writeAll("\"}");
    }
}

fn writeFloatAsCanonicalExtendedJsonString(writer: *Writer, num: f64) FloatFormatCanonicalExtendedJsonError!void {
    if (num == 0) {
        const is_negative: bool = @as(u64, @bitCast(num)) > 0;
        if (is_negative) {
            try writer.writeByte('-');
        }
        try writer.writeAll("0.0");
        return;
    }

    if (std.math.isInf(num)) {
        if (std.math.isNegativeInf(num)) {
            try writer.writeByte('-');
        }
        try writer.writeAll("Infinity");
        return;
    }

    var buf: [64]u8 = undefined;
    const num_string = try std.fmt.float.render(&buf, num, .{ .mode = .scientific });

    if (std.math.isNan(num)) {
        if (num_string[0] == '-') {
            try writer.writeByte('-');
        }
        try writer.writeAll("NaN");
        return;
    }

    if (std.mem.endsWith(u8, num_string, "e0")) {
        try writer.writeAll(buf[0 .. num_string.len - 2]);
        const len: usize = if (num > 0) 3 else 4;
        if (num_string.len == len) {
            try writer.writeAll(".0");
        }
        return;
    }

    var parts = std.mem.splitAny(u8, num_string, "e");
    const first_part = parts.next().?;
    const second_part = parts.next();
    try writer.writeAll(first_part);
    if (second_part) |second_part_value| {
        try writer.writeAll("E+");
        try writer.writeAll(second_part_value);
    }
}

fn appendTimestampToJsonString(writer: *Writer, reader: *Reader) !void {
    const num = try reader.takeInt(u64, .little);
    const num_high = @as(u32, @intCast(num >> 32));
    const num_low = @as(u32, @truncate(num));
    try writer.print("{{\"$timestamp\":{{\"t\":{d},\"i\":{d}}}}}", .{ num_high, num_low });
}

fn appendRegexpToJsonString(writer: *Writer, reader: *Reader) !void {
    try writer.writeAll("{\"$regularExpression\":{\"pattern\":\"");

    const pattern_bytes_slice = try reader.takeDelimiterExclusive(0x00);


    try std.json.Stringify.encodeJsonStringChars(pattern_bytes_slice, .{ .escape_unicode = true }, writer);

    try writer.writeAll("\",\"options\":\"");
    _ = try reader.streamDelimiterLimit(writer, 0x00, Limit.limited(5)); // TODO: handle error
    const last_byte = try reader.takeByte();
    assert(last_byte == 0);
    try writer.writeAll("\"}}");
}

fn appendUtcDateTimeToJsonString(writer: *Writer, reader: *Reader, comptime is_strict_ext_json: bool) !void {
    const num = try reader.takeInt(i64, .little);
    if (is_strict_ext_json or num < 0 or num >= datetime.DATETIME_MAX_I64) {
        try writer.print("{{\"$date\":{{\"$numberLong\":\"{d}\"}}}}", .{num});
    } else {
        var buffer: [24]u8 = undefined;
        const date_time_string = try datetime.dateTimeToUtcString(num, &buffer);
        try writer.print("{{\"$date\":\"{s}\"}}", .{date_time_string});
    }
}

fn appendBooleanToJsonString(writer: *Writer, reader: *Reader) !void {
    const num = try reader.takeByte();
    switch (num) {
        0 => try writer.writeAll("false"),
        1 => try writer.writeAll("true"),
        else => return WriteJsonStringError.InvalidBooleanValue,
    }
}

inline fn appendNullToJsonString(writer: *Writer) !void {
    try writer.writeAll("null");
}

fn appendBinaryToJsonString(writer: *Writer, reader: *Reader) !void {
    const expected_len = try reader.takeInt(i32, .little);
    const binary_expected_len = @as(usize, @intCast(expected_len));

    const sub_type_byte = try reader.takeByte();
    const sub_type: BsonSubType = @enumFromInt(sub_type_byte);

    try writer.writeAll("{\"$binary\":{\"base64\":\"");
    if (sub_type == .binary_old) {
        @branchHint(.unlikely);
        _ = try reader.discard(Limit.limited(@sizeOf(i32)));
        try utils.encodeFromReaderToWriter(&std.base64.standard.Encoder, writer, reader, binary_expected_len - @sizeOf(i32));
    } else {
        try utils.encodeFromReaderToWriter(&std.base64.standard.Encoder, writer, reader, binary_expected_len);
    }
    try writer.print("\",\"subType\":\"{x:02}\"}}}}", .{sub_type_byte});
}

fn appendObjectIdToJsonString(writer: *Writer, reader: *Reader) !void {
    const bytes_array = try reader.takeArray(12);
    const hex = std.fmt.bytesToHex(bytes_array, .lower);

    try writer.print("{{\"$oid\":\"{s}\"}}", .{hex});
}

fn appendMinKeyToJsonString(writer: *Writer) !void {
    try writer.writeAll("{\"$minKey\":1}");
}

fn appendMaxKeyToJsonString(writer: *Writer) !void {
    try writer.writeAll("{\"$maxKey\":1}");
}

inline fn skipArrayItemName(reader: *Reader) !void {
    _ = try reader.discardDelimiterInclusive(0x00);
}

inline fn skipArrayTerminatingByte(reader: *Reader) !void {
    try skipDocumentTerminatingByte(reader);
}

inline fn skipDocumentTerminatingByte(reader: *Reader) !void {
    const b = try reader.takeByte();
    assert(b == 0);
}

inline fn readElementTypeFromBson(reader: *Reader) (Reader.Error || bson_types.ElementTypeError)!ElementType {
    const value = try reader.takeInt(i8, .little);
    if (value < 0 and value != -1) {
        return error.UnknownElementType;
    }
    return @enumFromInt(value);
}
