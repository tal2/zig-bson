const std = @import("std");
const builtin = @import("builtin");
const datetime = @import("datetime.zig");
const utils = @import("utils.zig");
const colors = @import("colors.zig");
const bson_types = @import("bson-types.zig");
// const decimal128_utils = @import("decimal128-utils.zig");

const testing = std.testing;
const Allocator = std.mem.Allocator;
const native_endian = builtin.cpu.arch.endian();
const debug = std.debug;
const assert = std.debug.assert;

const ElementType = bson_types.BsonElementType;
const BsonSubType = bson_types.BsonSubType;
const BsonObjectIdError = bson_types.BsonObjectIdError;
const BsonBinary = bson_types.BsonBinary;
const BsonObjectId = bson_types.BsonObjectId;
const BsonUtcDatetime = bson_types.BsonUtcDatetime;
const BsonTimestamp = bson_types.BsonTimestamp;
const BsonRegexpOptions = bson_types.RegexpOptions;

const FixedBufferStreamJsonReader = std.io.FixedBufferStream([]const u8).Reader;
const JsonReader = std.json.Reader(0x1000, FixedBufferStreamJsonReader);

pub const BsonDocument = struct {
    len: usize,
    raw_data: []const u8,

    pub fn deinit(self: *BsonDocument, allocator: Allocator) void {
        allocator.free(self.raw_data);
    }

    pub fn writeToBsonWriter(comptime T: type, obj: T, writer: *std.ArrayList(u8)) !void {
        const type_info = @typeInfo(T);

        if (type_info != .@"struct") {
            @compileLog("compile log: " ++ @typeName(T));
            @compileError("T must be a struct " ++ @typeName(T));
        }

        inline for (type_info.@"struct".fields) |field| {
            const field_value_pre = @field(obj, field.name);

            const is_optional = comptime @typeInfo(field.type) == .optional;

            if (is_optional and field_value_pre == null) {
                try appendElementType(writer, ElementType.null);

                const field_name = field.name;
                try appendString(writer, field_name, false, true);
            } else {
                const field_value = if (is_optional) field_value_pre.? else field_value_pre;
                const field_element_type = comptime ElementType.typeToElementType(field.type);

                try appendElementType(writer, field_element_type);

                const field_name = field.name;
                try appendString(writer, field_name, false, true);
                try appendElementValue(writer, field_element_type, field.type, field_value);
            }
        }
        try appendNullTerminator(writer);
    }

    pub fn writeToBson(comptime T: type, obj: T, allocator: Allocator) BsonAppendError!*BsonDocument {
        var data_writer = std.ArrayList(u8).init(allocator);
        defer data_writer.deinit();

        try appendDocumentToBson(T, obj, &data_writer);

        const bson_doc = try allocator.create(BsonDocument);

        bson_doc.raw_data = try data_writer.toOwnedSlice();
        bson_doc.len = bson_doc.raw_data.len;
        return bson_doc;
    }

    fn appendDocumentToBson(comptime T: type, obj: T, data_writer: *std.ArrayList(u8)) BsonAppendError!void {
        const pos = data_writer.items.len;
        try appendDocumentLenPlaceholder(data_writer);
        try writeToBsonWriter(T, obj, data_writer);
        const document_len = data_writer.items.len - pos;
        updateLenMarker(data_writer.items, pos, document_len);
    }

    inline fn appendElementValue(writer: *std.ArrayList(u8), field_element_type: ElementType, FieldType: type, field_value: anytype) BsonAppendError!void {
        switch (field_element_type) {
            .int32 => {
                try appendInt32(writer, field_value);
            },
            .int64 => {
                try appendInt64(writer, field_value);
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
                const start_pos = writer.items.len;
                try appendDocumentLenPlaceholder(writer);
                for (field_value, 0..) |item, item_index| {
                    const array_item_type = comptime ElementType.typeToElementType(@TypeOf(item));
                    try appendElementType(writer, array_item_type);

                    try appendIntAsString(usize, writer, item_index, false, true);
                    try appendElementValue(writer, array_item_type, @TypeOf(item), item);
                }
                try appendNullTerminator(writer);
                const end_pos = writer.items.len;
                const len = end_pos - start_pos;
                updateLenMarker(writer.items, start_pos, len);
            },
            .timestamp => {
                try appendUnsignedInt64(writer, field_value.value);
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
                try writer.appendSlice(field_value.value);
            },
            else => {
                @panic("Unsupported type: " ++ @typeName(FieldType));
            },
        }
    }

    inline fn appendDocumentLenPlaceholder(writer: *std.ArrayList(u8)) std.mem.Allocator.Error!void {
        try appendInt32(writer, 0);
    }

    pub fn overwriteDocumentLenPlaceholder(raw_data: []u8) void {
        return updateLenMarker(raw_data, 0, raw_data.len);
    }

    fn updateLenMarker(raw_data: []u8, at: usize, len: usize) void {
        var bson_new_len_marker = std.mem.toBytes(@as(i32, @intCast(len)));
        bson_new_len_marker = @bitCast(if (native_endian == .little) bson_new_len_marker else @byteSwap(bson_new_len_marker));

        std.mem.copyForwards(u8, raw_data[at .. at + @sizeOf(i32)], &bson_new_len_marker);
    }

    fn overwriteElementType(raw_data: []u8, at: usize, element_type: ElementType) void {
        var element_type_bytes = std.mem.toBytes(@as(i8, @intFromEnum(element_type)));
        element_type_bytes = @bitCast(if (native_endian == .little) element_type_bytes else @byteSwap(element_type_bytes));

        std.mem.copyForwards(u8, raw_data[at .. at + @sizeOf(i8)], &element_type_bytes);
    }

    pub fn jsonStringToBson(json_string: []const u8, allocator: Allocator) !*BsonDocument {
        var data_writer = std.ArrayList(u8).init(allocator);
        defer data_writer.deinit();

        var stream = std.io.fixedBufferStream(json_string);
        const stream_reader = stream.reader();
        var reader = JsonReader.init(allocator, stream_reader);
        defer reader.deinit();

        var stack = std.ArrayList(usize).init(allocator);
        defer stack.deinit();

        try parseJsonStringToBson(&data_writer, &reader, &stack, null);

        const bson_data = try allocator.create(BsonDocument);
        const raw_data = try data_writer.toOwnedSlice();

        bson_data.raw_data = raw_data;
        bson_data.len = raw_data.len;
        return bson_data;
    }

    fn parseJsonStringToBson(data_writer: *std.ArrayList(u8), data_reader: *JsonReader, stack: *std.ArrayList(usize), last_element_type_pos: ?usize) !void {
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
                    debug.print("else: {any}\n", .{token});
                    @panic("unexpected token");
                },
            }
        }
    }

    fn parseJsonValueToBson(data_writer: *std.ArrayList(u8), reader: *JsonReader, stack: *std.ArrayList(usize), element: std.json.Token, last_element_type_pos: ?usize) anyerror!void {
        switch (element) {
            .object_begin => {
                try stack.append(data_writer.items.len);
                try appendDocumentLenPlaceholder(data_writer);

                try parseJsonStringToBson(data_writer, reader, stack, last_element_type_pos);
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
                debug.print("else: {any}\n", .{element});
                @panic("unexpected token");
            },
        }
    }

    fn parseArrayToBson(data_writer: *std.ArrayList(u8), reader: *JsonReader, stack: *std.ArrayList(usize)) anyerror!void {
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

    fn parseArrayItemToBson(data_writer: *std.ArrayList(u8), reader: *JsonReader, stack: *std.ArrayList(usize), array_item_index: i32, element: std.json.Token) !bool {
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

    fn appendPartialString(data_writer: *std.ArrayList(u8), reader: *JsonReader, starting_token: std.json.Token, comptime add_len_prefix: bool) !void {
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

    fn appendJsonExtValueToBson(e_name: []const u8, data_writer: *std.ArrayList(u8), reader: *JsonReader, stack: *std.ArrayList(usize), last_element_type_pos: ?usize) !bool {
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
                overwriteElementType(data_writer.items, last_element_type_pos_value, element_type);
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
                overwriteElementType(data_writer.items, last_element_type_pos_value, element_type);
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
                    try appendNumber(data_writer, value);
                },
                .decimal128 => {
                    @panic("not implemented yet");
                    // const value_as_string = token.string;
                    // const value = decimal128_utils.parseDecimal128(value_as_string);
                    // try appendDecimal128(data_writer, value);
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
            overwriteElementType(data_writer.items, last_element_type_pos_value, element_type);
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

    inline fn appendNullTerminator(writer: *std.ArrayList(u8)) std.mem.Allocator.Error!void {
        try writer.append(@as(u8, 0));
    }

    inline fn appendByte(writer: *std.ArrayList(u8), value: u8) std.mem.Allocator.Error!void {
        var value_bytes = std.mem.toBytes(value);
        value_bytes = @bitCast(if (native_endian == .little) value_bytes else @byteSwap(value_bytes));

        try writer.appendSlice(&value_bytes);
    }

    inline fn appendInt32(writer: *std.ArrayList(u8), value: i32) std.mem.Allocator.Error!void {
        try appendNumber(writer, value);
    }

    inline fn appendInt64(writer: *std.ArrayList(u8), value: i64) std.mem.Allocator.Error!void {
        try appendNumber(writer, value);
    }

    inline fn appendUint64(writer: *std.ArrayList(u8), value: u64) std.mem.Allocator.Error!void {
        try appendNumber(writer, value);
    }

    inline fn appendNumber(writer: *std.ArrayList(u8), value: anytype) std.mem.Allocator.Error!void {
        comptime utils.assertIsNumber(@TypeOf(value));
        var value_bytes = std.mem.toBytes(value);
        value_bytes = @bitCast(if (native_endian == .little) value_bytes else @byteSwap(value_bytes));

        try writer.appendSlice(&value_bytes);
    }

    inline fn appendObjectId(writer: *std.ArrayList(u8), value: []const u8) BsonAppendError!void {
        if (value.len != BsonObjectId.bson_object_id_as_string_size) {
            return BsonAppendError.InvalidObjectId;
        }
        var buffer: [BsonObjectId.bson_object_id_size]u8 = undefined;
        const value_bytes = std.fmt.hexToBytes(&buffer, value) catch return BsonAppendError.InvalidObjectId;
        try writer.appendSlice(value_bytes);
    }

    inline fn appendNumberFromString(writer: *std.ArrayList(u8), value: []const u8) (std.mem.Allocator.Error || std.fmt.ParseFloatError)!ElementType {
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

    inline fn appendUnsignedInt64(writer: *std.ArrayList(u8), value: u64) std.mem.Allocator.Error!void {
        var value_bytes = std.mem.toBytes(value);
        value_bytes = @bitCast(if (native_endian == .little) value_bytes else @byteSwap(value_bytes));

        try writer.appendSlice(&value_bytes);
    }

    inline fn appendDecimal128(writer: *std.ArrayList(u8), value: f128) std.mem.Allocator.Error!void {
        var value_bytes = std.mem.toBytes(value);
        value_bytes = @bitCast(if (native_endian == .little) value_bytes else @byteSwap(value_bytes));

        try writer.appendSlice(&value_bytes);
    }

    fn appendIntAsString(comptime T: type, writer: *std.ArrayList(u8), value: T, comptime add_len_prefix: bool, comptime add_null_terminator: bool) std.mem.Allocator.Error!void {
        comptime utils.assertIsInt(T);
        const max_len = @sizeOf(T) * 2;
        var buf: [max_len]u8 = undefined;
        const value_as_string = std.fmt.bufPrint(&buf, "{d}", .{value}) catch unreachable;
        return try appendString(writer, value_as_string, add_len_prefix, add_null_terminator);
    }

    fn appendString(writer: *std.ArrayList(u8), value: []const u8, comptime add_len_prefix: bool, comptime add_null_terminator: bool) std.mem.Allocator.Error!void {
        if (add_len_prefix) {
            const value_len_as_int32 = @as(i32, @intCast(value.len + if (add_null_terminator) 1 else 0));
            try appendInt32(writer, value_len_as_int32);
        }

        if (value.len > 0) {
            try writer.appendSlice(value);
        }
        if (add_null_terminator) {
            try appendNullTerminator(writer);
        }
    }

    fn appendBinary(writer: *std.ArrayList(u8), sub_type: BsonSubType, value: []const u8) std.mem.Allocator.Error!void {
        const value_len_as_int32 = @as(i32, @intCast(value.len));
        const sub_type_value: u8 = @intFromEnum(sub_type);

        if (sub_type == .binary_old) { // see spec notes: https://bsonspec.org/spec.html
            @branchHint(.unlikely);
            try appendInt32(writer, value_len_as_int32 + @sizeOf(i32));
            try writer.append(sub_type_value);
            try appendInt32(writer, value_len_as_int32);
            try writer.appendSlice(value);

            return;
        }

        try appendInt32(writer, value_len_as_int32);
        try writer.append(sub_type_value);
        try writer.appendSlice(value);
    }

    inline fn appendElementType(writer: *std.ArrayList(u8), element_type: ElementType) std.mem.Allocator.Error!void {
        const value: i8 = @intFromEnum(element_type);

        const value_byte: u8 = @bitCast(if (native_endian == .little) value else @byteSwap(value)); // TODO: verify
        try writer.append(value_byte);
    }

    pub fn toJsonString(self: *BsonDocument, allocator: std.mem.Allocator, comptime is_strict_ext_json: bool) ![]const u8 {
        var list = std.ArrayList(u8).init(allocator);
        errdefer list.deinit();

        var fixed_buffer_stream = std.io.fixedBufferStream(self.raw_data);
        const reader = fixed_buffer_stream.reader();

        const writer = list.writer();

        try appendDocumentToJsonString(reader, writer, is_strict_ext_json);

        return list.toOwnedSlice();
    }

    pub const WriteJsonStringError = error{
        InvalidBooleanValue,
        BinaryFieldRequiresStrictExtJson,
    };

    pub fn appendDocumentToJsonString(reader: anytype, writer: anytype, comptime is_strict_ext_json: bool) anyerror!void {
        const offset = @as(i32, @intCast(reader.context.pos));

        const total_document_size = try reader.readInt(i32, .little);

        if (total_document_size == 0) {
            try writer.writeAll("{}");
            try skipDocumentTerminatingByte(reader);
            return;
        }

        var i: usize = 0;
        const eof_pos = try reader.context.getEndPos();

        try writer.writeByte('{');

        const end_doc_pos = offset + total_document_size - 1;
        if (end_doc_pos >= eof_pos) {
            return BsonDocumentError.IncompleteDocument;
        }

        while (reader.context.pos < end_doc_pos) : (i += 1) {
            if (i > 0) {
                try writer.writeByte(',');
            }

            const element_type = try readElementTypeFromBson(&reader);

            try appendENameToJsonString(writer, reader);

            try appendElementValueToJsonString(writer, reader, element_type, is_strict_ext_json);
        }
        try writer.writeByte('}');

        try skipDocumentTerminatingByte(reader);
    }

    inline fn appendENameToJsonString(writer: anytype, reader: anytype) !void {
        try writer.writeByte('"');
        try reader.streamUntilDelimiter(writer, 0x00, 1024); // TODO: handle error
        try writer.writeAll("\":");
    }

    fn appendElementValueToJsonString(writer: anytype, reader: anytype, element_type: ElementType, comptime is_strict_ext_json: bool) !void {
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
                @panic("unsupported element type");
            },
        }
    }

    fn appendArrayToJsonString(writer: anytype, reader: anytype, comptime is_strict_ext_json: bool) anyerror!void {
        const sub_offset = @as(i32, @intCast(reader.context.pos));
        const sub_document_size = try reader.readInt(i32, .little);
        try writer.writeByte('[');
        if (sub_document_size > 0) {
            const end_pos_of_last_array_item = sub_document_size + sub_offset - 1;
            var item_index: usize = 0;
            while (reader.context.pos < end_pos_of_last_array_item) : (item_index += 1) {
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

    fn appendStringToJsonString(writer: anytype, reader: anytype) !void {
        const num = try reader.readInt(i32, .little);
        const str_expected_len = @as(usize, @intCast(num));
        assert(str_expected_len > 0);

        const buf: []u8 = writer.context.allocator.alloc(u8, str_expected_len) catch unreachable; // TODO: allocator
        defer writer.context.allocator.free(buf);
        const bytes_read = try reader.readAtLeast(buf, str_expected_len);
        assert(bytes_read == str_expected_len);

        try std.json.encodeJsonString(buf[0 .. bytes_read - 1], .{ .escape_unicode = false }, writer);
    }

    fn appendInt32ToJsonString(writer: anytype, reader: anytype, comptime is_strict_ext_json: bool) !void {
        const num = try reader.readInt(i32, .little);
        if (is_strict_ext_json) {
            try writer.print("{{\"$numberInt\":\"{d}\"}}", .{num});
        } else {
            try writer.print("{d}", .{num});
        }
    }

    fn appendInt64ToJsonString(writer: anytype, reader: anytype, comptime is_strict_ext_json: bool) !void {
        const num = try reader.readInt(i64, .little);
        if (is_strict_ext_json) {
            try writer.print("{{\"$numberLong\":\"{d}\"}}", .{num});
        } else {
            try writer.print("{d}", .{num});
        }
    }

    fn appendDecimal128ToJsonString(writer: anytype, reader: anytype) !void {
        _ = writer;
        _ = reader;
        @panic("not implemented yet");
        // const num: f128 = try std.fmt.parseFloat(f128, reader.context.buffer[reader.context.pos .. reader.context.pos + @sizeOf(f128)]);
        // reader.context.pos += @sizeOf(f128);
        // // try writer.print("{{\"$numberDecimal\":\"{d}\"}}", .{num});
        // try writer.print("{{\"$numberDecimal\":\"", .{});
        // try decimal128_utils.writeDecimal128AsCanonicalExtendedJsonString(writer, num);
        // try writer.print("\"}}", .{});
    }

    fn appendDoubleToJsonString(writer: anytype, reader: anytype, comptime is_strict_ext_json: bool) !void {
        const num_bytes = try reader.readBoundedBytes(@sizeOf(f64));
        const num = std.mem.bytesToValue(f64, num_bytes.slice());
        if (is_strict_ext_json) {
            try writer.writeAll("{\"$numberDouble\":\"");
        }
        try writeFloatAsCanonicalExtendedJsonString(writer, num);

        if (is_strict_ext_json) {
            try writer.writeAll("\"}");
        }
    }

    fn writeFloatAsCanonicalExtendedJsonString(writer: anytype, num: f64) FloatFormatCanonicalExtendedJsonError!void {
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
        const num_string = try std.fmt.formatFloat(&buf, num, .{ .mode = .scientific });

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

    fn appendTimestampToJsonString(writer: anytype, reader: anytype) !void {
        const num = try reader.readInt(u64, .little);
        const num_high = @as(u32, @intCast(num >> 32));
        const num_low = @as(u32, @truncate(num));
        try writer.print("{{\"$timestamp\":{{\"t\":{d},\"i\":{d}}}}}", .{ num_high, num_low });
    }

    fn appendRegexpToJsonString(writer: anytype, reader: anytype) !void {
        try writer.writeAll("{\"$regularExpression\":{\"pattern\":\"");
        var pattern_bytes = std.ArrayList(u8).init(writer.context.allocator);
        defer pattern_bytes.deinit();
        try reader.streamUntilDelimiter(pattern_bytes.writer(), 0x00, 1024); // TODO: handle error
        const pattern_bytes_slice = try pattern_bytes.toOwnedSlice();

        try std.json.encodeJsonStringChars(pattern_bytes_slice, .{ .escape_unicode = true }, writer);

        try writer.writeAll("\",\"options\":\"");
        try reader.streamUntilDelimiter(writer, 0x00, 5); // TODO: handle error
        try writer.writeAll("\"}}");
    }

    fn appendUtcDateTimeToJsonString(writer: anytype, reader: anytype, comptime is_strict_ext_json: bool) !void {
        const num = try reader.readInt(i64, .little);
        if (is_strict_ext_json or num < 0 or num >= datetime.DATETIME_MAX_I64) {
            try writer.print("{{\"$date\":{{\"$numberLong\":\"{d}\"}}}}", .{num});
        } else {
            var buffer: [24]u8 = undefined;
            const date_time_string = try datetime.dateTimeToUtcString(num, &buffer);
            try writer.print("{{\"$date\":\"{s}\"}}", .{date_time_string});
        }
    }

    fn appendBooleanToJsonString(writer: anytype, reader: anytype) !void {
        const num = try reader.readByte();
        switch (num) {
            0 => try writer.writeAll("false"),
            1 => try writer.writeAll("true"),
            else => return WriteJsonStringError.InvalidBooleanValue,
        }
    }

    inline fn appendNullToJsonString(writer: anytype) !void {
        try writer.writeAll("null");
    }

    fn appendBinaryToJsonString(writer: anytype, reader: anytype) !void {
        const expected_len = try reader.readInt(i32, .little);
        const binary_expected_len = @as(usize, @intCast(expected_len));

        const sub_type_byte = try reader.readByte();
        const sub_type: BsonSubType = @enumFromInt(sub_type_byte);

        try writer.writeAll("{\"$binary\":{\"base64\":\"");
        if (sub_type == .binary_old) {
            @branchHint(.unlikely);
            try reader.skipBytes(@sizeOf(i32), .{});
            try utils.encodeFromReaderToWriter(&std.base64.standard.Encoder, writer, reader, binary_expected_len - @sizeOf(i32));
        } else {
            try utils.encodeFromReaderToWriter(&std.base64.standard.Encoder, writer, reader, binary_expected_len);
        }
        try writer.print("\",\"subType\":\"{x:02}\"}}}}", .{sub_type_byte});
    }

    fn appendObjectIdToJsonString(writer: anytype, reader: anytype) !void {
        const bytes_array = try reader.readBoundedBytes(12);
        const bytes = bytes_array.slice()[0..12];
        const hex = std.fmt.bytesToHex(bytes, .lower);

        try writer.print("{{\"$oid\":\"{s}\"}}", .{hex});
    }

    fn appendMinKeyToJsonString(writer: anytype) !void {
        try writer.writeAll("{\"$minKey\":1}");
    }

    fn appendMaxKeyToJsonString(writer: anytype) !void {
        try writer.writeAll("{\"$maxKey\":1}");
    }

    inline fn skipArrayItemName(reader: anytype) !void {
        try reader.skipUntilDelimiterOrEof(0x00);
    }

    inline fn skipArrayTerminatingByte(reader: anytype) !void {
        try skipDocumentTerminatingByte(reader);
    }

    inline fn skipDocumentTerminatingByte(reader: anytype) !void {
        const b = try reader.readByte();
        assert(b == 0);
    }

    inline fn readElementTypeFromBson(reader: anytype) (error{EndOfStream} || bson_types.ElementTypeError)!ElementType {
        const value = try reader.readInt(i8, .little);
        if (value < 0 and value != -1) {
            return error.UnknownElementType;
        }
        return @enumFromInt(value);
    }
};

pub const BsonAppendError = std.fmt.BufPrintError || Allocator.Error || error{InvalidObjectId};
pub const BsonDocumentError = error{
    IncompleteDocument,
};

pub const JsonParsingRegExpError = error{
    InvalidRegExpValue,
    InvalidRegExpOptions,
};

pub const JsonParseError = std.json.ParseFromValueError || JsonParsingRegExpError || error{InvalidDecimal128Value};

pub const FloatFormatCanonicalExtendedJsonError = std.fmt.format_float.FormatError || error{OutOfMemory};

test {
    _ = datetime;
    _ = @import("bson-tests.zig");
    _ = @import("bson-corpus-tests.zig");
}
