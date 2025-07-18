const std = @import("std");
const Decimal128 = @import("binary_coded_decimal");
const datetime = @import("datetime.zig");

const Allocator = std.mem.Allocator;
pub const JsonParseError = error{UnexpectedToken} || std.json.Scanner.NextError;

pub const ElementTypeError = error{
    UnknownElementType,
};

pub const BsonElementType = enum(i8) {
    empty = 0,
    double = 1,
    string = 2,
    document = 3,
    array = 4,
    binary = 5,
    undefined = 6,
    object_id = 7,
    boolean = 8,
    utc_date_time = 9,
    null = 10,
    regexp = 11,
    db_pointer = 12,
    javascript = 13,
    symbol = 14,
    javascript_with_scope = 15,
    int32 = 16,
    timestamp = 17,
    int64 = 18,
    decimal128 = 19,
    min_key = -1,
    max_key = 127,
    _,

    pub fn typeToElementType(comptime T: type) BsonElementType {
        const type_info = @typeInfo(T);
        return switch (type_info) {
            .optional => typeToElementType(type_info.optional.child),
            // .array => .array,
            .@"struct" => switch (T) {
                BsonObjectId => .object_id,
                BsonTimestamp => .timestamp,
                BsonUtcDatetime => .utc_date_time,
                BsonBinary => .binary,
                else => .document,
            },
            else => switch (T) {
                u8 => .binary,
                i8 => .int32,
                i16 => .int32,
                i32 => .int32,
                comptime_int => .int32,
                f32 => .double,
                f64 => .double,
                comptime_float => .double,
                i64 => .int64,
                f128 => .decimal128,
                bool => .boolean,
                []u8 => .string,
                []const u8 => .string,
                [:0]const u8 => .string,
                else => {
                    if (type_info == .pointer) {
                        return switch (type_info.pointer.size) {
                            .many, .slice => .array,
                            .one => {
                                const type_info_child = @typeInfo(type_info.pointer.child);

                                switch (type_info_child) {
                                    .array => {
                                        if (type_info_child.array.child == u8) {
                                            // TODO: verify that this can also be deserialized
                                            return .string;
                                        }
                                    },
                                    else => {
                                        @compileLog("unexpected pointer type size: one");
                                        @compileLog(@typeName(T));
                                        @compileLog(@typeName(type_info.pointer.child));
                                        @panic("unexpected pointer type");
                                    },
                                }
                                @panic("unexpected pointer type");
                            },
                            else => {
                                @panic("unexpected pointer type");
                            },
                        };
                    }
                    @panic("Unsupported type");
                },
            },
        };
    }

    pub fn fromToken(token: std.json.Token) BsonElementType {
        switch (token) {
            .string => return .string,
            .partial_string => return .string,
            .number => {
                if (token.number.ptr[0] == '-') {
                    return if (token.number.len <= 11) .int32 else .int64;
                }

                return if (token.number.len <= 10) .int32 else .int64;
            },
            .object_begin => return .document,
            .array_begin => return .array,
            .true => return .boolean,
            .false => return .boolean,
            .null => return .null,
            else => {
                std.debug.print("Type: {any}\n", .{token});
                @panic("Unexpected token");
            },
        }
    }

    pub fn fromExtJsonKey(key: []const u8) ?BsonElementType {
        if (key.len < 4 or key[0] != '$') return null;

        switch (key[1]) {
            'n' => {
                if (std.mem.eql(u8, key, "$numberInt")) {
                    return .int32;
                }
                if (std.mem.eql(u8, key, "$numberLong")) {
                    return .int64;
                }
                if (std.mem.eql(u8, key, "$numberDouble")) {
                    return .double;
                }
                if (std.mem.eql(u8, key, "$numberDecimal")) {
                    return .decimal128;
                }
                return null;
            },
            'm' => {
                if (std.mem.eql(u8, key, "$minKey")) {
                    return .min_key;
                }
                if (std.mem.eql(u8, key, "$maxKey")) {
                    return .max_key;
                }
                return null;
            },
            'o' => {
                if (std.mem.eql(u8, key, "$oid")) {
                    return .object_id;
                }
                return null;
            },
            'd' => {
                if (std.mem.eql(u8, key, "$date")) {
                    return .utc_date_time;
                }
                return null;
            },
            't' => {
                if (std.mem.eql(u8, key, "$timestamp")) {
                    return .timestamp;
                }
                return null;
            },
            'r' => {
                if (std.mem.eql(u8, key, "$regularExpression")) {
                    return .regexp;
                }
                return null;
            },
            'b' => {
                if (std.mem.eql(u8, key, "$binary")) {
                    return .binary;
                }
                return null;
            },
            else => return null,
        }
    }
};

pub const BsonSubType = enum(u8) {
    generic = 0,
    function = 1,
    binary_old = 2,
    uuid_old = 3,
    uuid = 4,
    md5 = 5,
    encrypted_bson_value = 6,
    compressed_bson_column = 7,
    sensitive = 8,
    vector = 9,
    user_defined = 128,
    _,
};

pub const BsonObjectIdError = Allocator.Error || error{ ValueSizeNot24Bytes, InvalidCharacter, UnexpectedToken };

pub const BsonObjectId = struct {
    pub const bson_object_id_size = 12;
    pub const bson_object_id_as_string_size = bson_object_id_size * 2;

    value: []u8,

    pub fn isEqualTo(self: *const BsonObjectId, b: *const BsonObjectId) bool {
        return std.mem.eql(u8, self.value, b.value);
    }

    pub fn fromString(allocator: Allocator, value: []const u8) BsonObjectIdError!BsonObjectId {
        if (value.len != bson_object_id_as_string_size) {
            return BsonObjectIdError.ValueSizeNot24Bytes;
        }

        const value_buf = try allocator.alloc(u8, bson_object_id_size);
        return BsonObjectId{ .value = std.fmt.hexToBytes(value_buf, value) catch unreachable };
    }

    pub fn jsonParse(allocator: Allocator, source: *std.json.Scanner, options: std.json.ParseOptions) JsonParseError!BsonObjectId {
        _ = options;

        var token = try source.next();
        if (token != .object_begin) return error.UnexpectedToken;

        token = try source.next();
        if (token != .string or !std.mem.eql(u8, token.string, "$oid")) {
            return error.UnexpectedToken;
        }

        token = try source.next();
        if (token != .string) return error.UnexpectedToken;

        const value = token.string;
        if (value.len != bson_object_id_as_string_size) {
            return error.UnexpectedToken;
        }

        token = try source.next();
        if (token != .object_end) return error.UnexpectedToken;

        return BsonObjectId.fromString(allocator, value) catch return error.UnexpectedToken;
    }
};

pub const BsonTimestamp = struct {
    value: u64,

    pub fn fromInt64(value: i64) BsonTimestamp {
        return BsonTimestamp{
            .value = @as(u64, @intCast(value)),
        };
    }

    pub fn jsonParse(allocator: Allocator, source: *std.json.Scanner, options: std.json.ParseOptions) JsonParseError!BsonTimestamp {
        _ = options;

        _ = allocator;
        _ = source;
        @panic("not implemented");
    }
};

pub const BsonUtcDatetime = struct {
    value: i64,

    pub fn fromInt64(value: i64) BsonUtcDatetime {
        return BsonUtcDatetime{
            .value = value,
        };
    }

    pub fn jsonParse(allocator: Allocator, source: *std.json.Scanner, options: std.json.ParseOptions) JsonParseError!BsonUtcDatetime {
        _ = allocator;
        _ = options;

        var token = try source.next();
        if (token != .object_begin) return error.UnexpectedToken;

        token = try source.next();
        if (token != .string or !std.mem.eql(u8, token.string, "$date")) {
            return error.UnexpectedToken;
        }
        const value_token = try source.next();

        token = try source.next();
        if (token != .object_end) return error.UnexpectedToken;

        const value = switch (value_token) {
            .string => datetime.parseUtcDateTimeISO8601(value_token.string) catch return error.UnexpectedToken,
            .number => std.fmt.parseInt(i64, value_token.number, 10) catch return error.UnexpectedToken,
            else => return error.UnexpectedToken,
        };

        return BsonUtcDatetime.fromInt64(value);
    }
};

pub const BsonBinary = struct {
    value: []const u8,
    sub_type: BsonSubType,

    pub fn fromBytes(comptime T: type, value: []const T, sub_type: BsonSubType) BsonBinary {
        return BsonBinary{
            .value = value,
            .sub_type = sub_type,
        };
    }

    pub fn jsonParse(allocator: Allocator, source: *std.json.Scanner, options: std.json.ParseOptions) !BsonBinary {
        _ = options;

        _ = allocator;
        _ = source;
        @panic("not implemented");
    }
};

pub const RegexpOptions = enum(u8) {
    i = 'i',
    m = 'm',
    x = 'x',
    s = 's',
    u = 'u',
    _,

    pub fn isValid(value: u8) bool {
        const option: RegexpOptions = @enumFromInt(value);

        switch (option) {
            .i, .m, .x, .s, .u => return true,
            else => return false,
        }
    }

    pub fn isValidOptions(value: []const u8) bool {
        if (value.len == 0) return true;
        for (0..value.len - 1) |i| {
            const c = value[i];
            if (!RegexpOptions.isValid(c)) return false;
        }
        return true;
    }

    pub fn isRegexpOptionsOrderCorrect(regexp_options: []const u8) bool {
        var last_c: u8 = 0;
        for (regexp_options) |c| {
            if (c <= last_c) return false;
            last_c = c;
        }
        return true;
    }

    pub fn sortRegexpOptions(regexp_options: []u8) void {
        std.sort.heap(u8, regexp_options[0 .. regexp_options.len - 1], {}, std.sort.asc(u8));
    }
};

pub const BsonDecimal128 = Decimal128;
