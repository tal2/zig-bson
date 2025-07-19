const std = @import("std");
const builtin = @import("builtin");
const bson = @import("bson.zig");
const utils = @import("utils.zig");
const datetime = @import("datetime.zig");
const Decimal128 = @import("binary_coded_decimal");
const bson_types = @import("bson-types.zig");
const BsonWriter = @import("bson-writer.zig");

const Allocator = std.mem.Allocator;
const BsonDocument = bson.BsonDocument;
const assert = std.debug.assert;

const native_endian = builtin.cpu.arch.endian();

const ElementType = bson_types.BsonElementType;
const BsonSubType = bson_types.BsonSubType;
const BsonDecimal128 = bson_types.BsonDecimal128;
const BsonObjectId = bson_types.BsonObjectId;
const BsonRegexpOptions = bson_types.RegexpOptions;

pub const JsonParsingRegExpError = error{
    InvalidRegExpValue,
    InvalidRegExpOptions,
};

pub const JsonParseError = std.json.ParseFromValueError || JsonParsingRegExpError || error{InvalidDecimal128Value};

const appendInt32 = BsonWriter.appendInt32;
const appendInt64 = BsonWriter.appendInt64;
const appendDouble = BsonWriter.appendDouble;
const appendDecimal128 = BsonWriter.appendDecimal128;
const appendObjectId = BsonWriter.appendObjectId;
const appendString = BsonWriter.appendString;
const appendBinary = BsonWriter.appendBinary;
const appendNullTerminator = BsonWriter.appendNullTerminator;
const updateLenMarker = BsonWriter.updateLenMarker;
const appendIntAsString = BsonWriter.appendIntAsString;
const appendByte = BsonWriter.appendByte;
const appendNumberFromString = BsonWriter.appendNumberFromString;
const appendUint64 = BsonWriter.appendUint64;
const appendElementType = BsonWriter.appendElementType;
const overwriteElementType = BsonWriter.overwriteElementType;
const appendDocumentLenPlaceholder = BsonWriter.appendDocumentLenPlaceholder;
const overwriteDocumentLenPlaceholder = BsonWriter.overwriteDocumentLenPlaceholder;

pub fn jsonStringToBson(allocator: Allocator, json_string: []const u8) !*BsonDocument {
    const FixedBufferStreamJsonReader = std.io.FixedBufferStream([]const u8).Reader;
    const JsonReader = std.json.Reader(0x1000, FixedBufferStreamJsonReader); // TODO: configure buffer size

    var stream = std.io.fixedBufferStream(json_string);
    const stream_reader = stream.reader();
    var reader = JsonReader.init(allocator, stream_reader);
    defer reader.deinit();

    return try jsonReaderToBson(allocator, &reader, true);
}

pub fn jsonReaderToBson(allocator: Allocator, reader: anytype, comptime is_source_single_object: bool) !*BsonDocument {
    var data_writer = std.ArrayList(u8).init(allocator);
    defer data_writer.deinit();

    var stack = std.ArrayList(usize).init(allocator);
    defer stack.deinit();

    try parseJsonToBson(&data_writer, reader, &stack, null, is_source_single_object);

    const bson_data = try allocator.create(BsonDocument);
    const raw_data = try data_writer.toOwnedSlice();

    bson_data.raw_data = raw_data;
    bson_data.len = raw_data.len;
    return bson_data;
}

pub fn parseJsonToBson(data_writer: *std.ArrayList(u8), data_reader: anytype, stack: *std.ArrayList(usize), last_element_type_pos: ?usize, comptime is_source_single_object: bool) !void {
    // TODO: verify reader is valid JsonReader

    var current_token: std.json.Token = try data_reader.next();
    loop: while (true) : (current_token = try data_reader.next()) {
        const token = current_token;
        switch (token) {
            .object_begin => {
                try stack.append(data_writer.items.len);
                try appendDocumentLenPlaceholder(data_writer);

                continue :loop;
            },
            .object_end => {
                try appendNullTerminator(data_writer);
                const start_pos = stack.pop() orelse @panic("stack is empty");
                const end_pos = data_writer.items.len;
                const len = end_pos - start_pos;
                updateLenMarker(data_writer.items, start_pos, len);

                if (!is_source_single_object) return;
            },

            .array_end => {
                unreachable;
            },
            .end_of_document => {
                assert(stack.items.len == 0);
                overwriteDocumentLenPlaceholder(data_writer.items);

                break :loop;
            },
            .string => |e_name| {
                if (try appendJsonExtValueToBson(e_name, data_writer, data_reader, stack, last_element_type_pos)) {
                    return;
                }

                const element = try data_reader.next();

                const current_element_type_pos = data_writer.items.len;
                try appendElementType(data_writer, ElementType.fromToken(element));
                try appendString(data_writer, e_name, false, true);

                try parseJsonValueToBson(data_writer, data_reader, stack, element, current_element_type_pos);
            },

            else => {
                std.debug.print("else: {any}\n", .{token});
                @panic("unexpected token");
            },
        }
    }
}

fn parseJsonValueToBson(data_writer: *std.ArrayList(u8), reader: anytype, stack: *std.ArrayList(usize), element: std.json.Token, last_element_type_pos: ?usize) anyerror!void {
    switch (element) {
        .object_begin => {
            try stack.append(data_writer.items.len);
            try appendDocumentLenPlaceholder(data_writer);

            try parseJsonToBson(data_writer, reader, stack, last_element_type_pos, false);
        },
        .object_end => {
            unreachable;
        },
        .array_begin => {
            try parseArrayToBson(data_writer, reader, stack);
        },
        .array_end => {
            unreachable;
        },
        .string => |value| {
            try appendString(data_writer, value, true, true);
        },
        .partial_string => {
            try appendPartialString(data_writer, reader, element, true);
        },
        .number => |value| {
            _ = try appendNumberFromString(data_writer, value);
        },
        .true => {
            try appendByte(data_writer, 1);
        },
        .false => {
            try appendByte(data_writer, 0);
        },
        .null => {
            // nothing to append
        },

        else => {
            // debug.print("else: {any}\n", .{element});
            @panic("unexpected token");
        },
    }
}

fn parseArrayToBson(data_writer: *std.ArrayList(u8), reader: anytype, stack: *std.ArrayList(usize)) anyerror!void {
    const array_start_pos = data_writer.items.len;
    try appendDocumentLenPlaceholder(data_writer);

    var array_item_index: i32 = 0;
    var has_more: bool = true;
    var token: std.json.Token = try reader.next();
    while (has_more) : (token = try reader.next()) {
        has_more = try parseArrayItemToBson(data_writer, reader, stack, array_item_index, token);
        if (!has_more) {
            break;
        }
        array_item_index += 1;
    }

    try appendNullTerminator(data_writer);
    const len = data_writer.items.len - array_start_pos;
    updateLenMarker(data_writer.items, array_start_pos, len);
}

fn parseArrayItemToBson(data_writer: *std.ArrayList(u8), reader: anytype, stack: *std.ArrayList(usize), array_item_index: i32, element: std.json.Token) !bool {
    if (element == .array_end) {
        return false;
    }
    if (element == .array_begin) {
        @panic("not tested");
    }

    const last_element_type_pos = data_writer.items.len;

    const element_type = ElementType.fromToken(element);
    try appendElementType(data_writer, element_type);
    try appendIntAsString(i32, data_writer, array_item_index, false, true);

    try parseJsonValueToBson(data_writer, reader, stack, element, last_element_type_pos);

    return true;
}

fn appendPartialString(data_writer: *std.ArrayList(u8), reader: anytype, starting_token: std.json.Token, comptime add_len_prefix: bool) !void {
    if (starting_token == .string) {
        try appendString(data_writer, starting_token.string, add_len_prefix, true);
        return;
    }
    const current_pos = data_writer.items.len;
    if (add_len_prefix) {
        try appendDocumentLenPlaceholder(data_writer);
    }

    var next_token = starting_token;
    while (true) : (next_token = try reader.next()) {
        switch (next_token) {
            .string => |value| {
                try data_writer.appendSlice(value);
                try appendNullTerminator(data_writer);
                break;
            },
            .partial_number, .partial_string => |slice| {
                try data_writer.appendSlice(slice);
            },
            .partial_string_escaped_1 => |buf| {
                try data_writer.appendSlice(buf[0..]);
            },
            .partial_string_escaped_2 => |buf| {
                try data_writer.appendSlice(buf[0..]);
            },
            .partial_string_escaped_3 => |buf| {
                try data_writer.appendSlice(buf[0..]);
            },
            .partial_string_escaped_4 => |buf| {
                try data_writer.appendSlice(buf[0..]);
            },
            else => @panic("unexpected token"),
        }
    }

    if (add_len_prefix) {
        const len = data_writer.items.len - @sizeOf(i32) - current_pos;
        updateLenMarker(data_writer.items, current_pos, len);
    }
}

fn appendJsonExtValueToBson(e_name: []const u8, data_writer: *std.ArrayList(u8), reader: anytype, stack: *std.ArrayList(usize), last_element_type_pos: ?usize) !bool {
    const element_type_optional = ElementType.fromExtJsonKey(e_name);
    if (element_type_optional == null) {
        return false;
    }
    const element_type = element_type_optional.?;

    if (element_type == .timestamp) {
        const timestamp_token_begin = try reader.next();
        if (timestamp_token_begin != .object_begin) return JsonParseError.UnexpectedToken;

        const timestamp_token_t = try reader.next();
        if (timestamp_token_t != .string) return JsonParseError.UnexpectedToken;
        const timestamp_token_t_value = try reader.next();
        if (timestamp_token_t_value != .number) return JsonParseError.UnexpectedToken;

        const timestamp_token_i = try reader.next();
        if (timestamp_token_i != .string) return JsonParseError.UnexpectedToken;
        const timestamp_token_i_value = try reader.next();
        if (timestamp_token_i_value != .number) return JsonParseError.UnexpectedToken;

        const timestamp_token_end = try reader.next();
        if (timestamp_token_end != .object_end) return JsonParseError.UnexpectedToken;

        const t_value = try std.fmt.parseInt(u32, timestamp_token_t_value.number, 10);
        const i_value = try std.fmt.parseInt(u32, timestamp_token_i_value.number, 10);

        const timestamp_value: u64 =
            if (timestamp_token_t.string[0] == 't')
                @as(u64, @intCast(t_value)) << 32 | @as(u64, @intCast(i_value))
            else // if keys are reversed
                @as(u64, @intCast(i_value)) << 32 | @as(u64, @intCast(t_value));

        const start_pos = stack.pop() orelse @panic("stack is empty");
        data_writer.shrinkRetainingCapacity(start_pos);

        try appendUint64(data_writer, timestamp_value);

        if (last_element_type_pos) |last_element_type_pos_value| {
            overwriteElementType(data_writer.items, last_element_type_pos_value, element_type);
        } else {
            @panic("last_element_type_pos is null");
        }

        const next_token = try reader.next();
        if (next_token != .object_end) return JsonParseError.UnexpectedToken;

        return true;
    }
    if (element_type == .regexp) {
        const regexp_token_begin = try reader.next();
        if (regexp_token_begin != .object_begin) return JsonParseError.UnexpectedToken;

        const regexp_token_pattern = try reader.next();
        if (regexp_token_pattern != .string or !std.mem.eql(u8, regexp_token_pattern.string, "pattern")) return JsonParseError.UnexpectedToken;

        const regexp_token_pattern_value = try reader.next();
        var temp_data_writer = std.ArrayList(u8).init(data_writer.allocator);
        defer temp_data_writer.deinit();

        try appendPartialString(&temp_data_writer, reader, regexp_token_pattern_value, false);
        const regexp_pattern_value = try temp_data_writer.toOwnedSlice();
        try verifyValidRegExpValue(regexp_pattern_value);

        const regexp_token_options = try reader.next();
        if (regexp_token_options != .string or !std.mem.eql(u8, regexp_token_options.string, "options")) return JsonParseError.UnexpectedToken;

        const regexp_token_options_value = try reader.next();

        try appendPartialString(&temp_data_writer, reader, regexp_token_options_value, false);
        const regexp_options_value = try temp_data_writer.toOwnedSlice();
        if (!BsonRegexpOptions.isValidOptions(regexp_options_value)) {
            return JsonParsingRegExpError.InvalidRegExpOptions;
        }
        if (!BsonRegexpOptions.isRegexpOptionsOrderCorrect(regexp_options_value)) {
            BsonRegexpOptions.sortRegexpOptions(regexp_options_value);
        }

        const regexp_token_end = try reader.next();
        if (regexp_token_end != .object_end) return JsonParseError.UnexpectedToken;

        const start_pos_regexp_element = stack.pop() orelse @panic("stack is empty");
        data_writer.shrinkRetainingCapacity(start_pos_regexp_element);

        try appendString(data_writer, regexp_pattern_value, false, false);
        try appendString(data_writer, regexp_options_value, false, false);

        if (last_element_type_pos) |last_element_type_pos_value| {
            BsonWriter.overwriteElementType(data_writer.items, last_element_type_pos_value, element_type);
        } else {
            @panic("last_element_type_pos is null");
        }

        const next_token = try reader.next();
        if (next_token != .object_end) return JsonParseError.UnexpectedToken;
        return true;
    }
    if (element_type == .binary) {
        const binary_token_begin = try reader.next();
        if (binary_token_begin != .object_begin) return JsonParseError.UnexpectedToken;

        const binary_token_base64 = try reader.next();
        if (binary_token_base64 != .string) return JsonParseError.UnexpectedToken;
        if (!std.mem.eql(u8, binary_token_base64.string, "base64")) return JsonParseError.UnexpectedToken;

        const binary_token_base64_token = try reader.next();
        if (binary_token_base64_token != .string) return JsonParseError.UnexpectedToken;

        const binary_token_base64_value = binary_token_base64_token.string;

        const binary_token_sub_type = try reader.next();
        if (binary_token_sub_type != .string) return JsonParseError.UnexpectedToken;
        if (!std.mem.eql(u8, binary_token_sub_type.string, "subType")) return JsonParseError.UnexpectedToken;

        const binary_token_sub_type_value = try reader.next();
        if (binary_token_sub_type_value != .string) return JsonParseError.UnexpectedToken;

        const end_element_token = try reader.next();
        if (end_element_token != .object_end) return JsonParseError.UnexpectedToken;

        const sub_type: BsonSubType = @enumFromInt(try std.fmt.parseInt(u8, binary_token_sub_type_value.string, 16));

        const start_pos_binary_element = stack.pop() orelse @panic("stack is empty");
        data_writer.shrinkRetainingCapacity(start_pos_binary_element);

        const binary_token_base64_value_decoded = try utils.bytesFromBase64(data_writer.allocator, binary_token_base64_value);
        defer data_writer.allocator.free(binary_token_base64_value_decoded);

        try appendBinary(data_writer, sub_type, binary_token_base64_value_decoded);

        if (last_element_type_pos) |last_element_type_pos_value| {
            BsonWriter.overwriteElementType(data_writer.items, last_element_type_pos_value, element_type);
        } else {
            @panic("last_element_type_pos is null");
        }

        const next_token = try reader.next();
        if (next_token != .object_end) return JsonParseError.UnexpectedToken;
        return true;
    }

    const token = try reader.next();

    if (token == .object_begin) {
        const next_e_name = try reader.next();
        if (next_e_name != .string) return JsonParseError.UnexpectedToken;
        const result = try appendJsonExtValueToBson(next_e_name.string, data_writer, reader, stack, last_element_type_pos);
        if (!result) return JsonParseError.UnexpectedToken;
    } else {
        const start_pos = stack.pop() orelse @panic("stack is empty");
        data_writer.shrinkRetainingCapacity(start_pos);

        switch (element_type) {
            .int32 => {
                const value_as_string = token.string;

                const value = try std.fmt.parseInt(i32, value_as_string, 10);
                try appendInt32(data_writer, value);
            },
            .int64 => {
                const value_as_string = token.string;

                const value = try std.fmt.parseInt(i64, value_as_string, 10);
                try appendInt64(data_writer, value);
            },
            .double => {
                const value_as_string = token.string;
                const value = try std.fmt.parseFloat(f64, value_as_string);
                try appendDouble(data_writer, value);
            },
            .decimal128 => {
                const value_as_string = token.string;
                var value = try BsonDecimal128.fromNumericString(value_as_string);
                if (value.signal.inexact) return JsonParseError.InvalidDecimal128Value;

                try appendDecimal128(data_writer, &value);
            },
            .object_id => {
                const value_as_string = token.string;
                try appendObjectId(data_writer, value_as_string);
            },
            .utc_date_time => {
                const value_as_string = token.string;

                if (value_as_string[value_as_string.len - 1] == 'Z') {
                    const value = try datetime.parseUtcDateTimeISO8601(value_as_string);
                    try appendInt64(data_writer, value);
                } else {
                    var value: i64 = undefined;
                    value = try std.fmt.parseInt(i64, value_as_string, 10);
                    try appendInt64(data_writer, value);
                }
            },
            .timestamp => {
                unreachable;
            },
            .regexp => {
                unreachable;
            },
            .min_key, .max_key => {
                // nothing to append
                assert(token == .number);
            },
            else => {
                @panic("unexpected element type");
            },
        }
    }

    if (last_element_type_pos) |last_element_type_pos_value| {
        BsonWriter.overwriteElementType(data_writer.items, last_element_type_pos_value, element_type);
    } else {
        return JsonParseError.InvalidNumber; // TODO: use better error
    }

    const next_token = try reader.next();
    if (next_token != .object_end) return JsonParseError.UnexpectedToken;

    return true;
}

fn verifyValidRegExpValue(value: []const u8) JsonParsingRegExpError!void {
    // https://github.com/mongodb/specifications/blob/master/source/bson-corpus/bson-corpus.md#1-prohibit-null-bytes-in-null-terminated-strings-when-encoding-bson
    if (std.mem.indexOfScalar(u8, value, 0)) |null_index| {
        if (null_index < value.len - 1) {
            return JsonParsingRegExpError.InvalidRegExpValue;
        }
    }
}
