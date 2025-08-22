const std = @import("std");
const bson_types = @import("./bson-types.zig");

const Allocator = std.mem.Allocator;
const BsonDocument = @import("./bson.zig").BsonDocument;

pub const BsonDocumentIterator = struct {
    allocator: Allocator,
    document: *const BsonDocument,
    current_element: ?*BsonElement,

    pub fn init(allocator: Allocator, document: *const BsonDocument) !BsonDocumentIterator {
        return .{
            .allocator = allocator,
            .document = document,
            .current_element = null,
        };
    }

    pub fn deinit(self: *const BsonDocumentIterator) void {
        if (self.current_element) |current_element| {
            current_element.deinit(self.allocator);
        }
    }

    /// Returns the next key in the document.
    /// Returns null if the end of the document is reached.
    pub fn next(self: *BsonDocumentIterator) !?*BsonElement {
        var start_pos = if (self.current_element) |current_element| current_element.endPos() else 0;
        if (self.current_element) |current_element| {
            self.allocator.destroy(current_element);
        }
        if (start_pos >= self.document.len - 1) {
            return null;
        }

        var fbs = std.io.fixedBufferStream(self.document.raw_data);
        fbs.pos = start_pos;

        var reader = fbs.reader();

        if (start_pos == 0) {
            start_pos += @sizeOf(i32);
            try fbs.seekBy(@sizeOf(i32));
        }
        const element_type_raw = try reader.readByte();
        const element_type: bson_types.BsonElementType = @enumFromInt(element_type_raw);
        start_pos += @sizeOf(u8);

        var e_name_array_list = try std.ArrayList(u8).initCapacity(self.allocator, 16);
        defer e_name_array_list.deinit();
        const e_name_writer = e_name_array_list.writer();

        try reader.streamUntilDelimiter(e_name_writer, 0x0, self.document.len - start_pos - 1);

        start_pos += e_name_array_list.items.len + @sizeOf(u8);

        const doc_size = blk: switch (element_type) {
            .document, .binary, .array => {
                break :blk try reader.readInt(i32, .little);
            },
            .string => {
                start_pos += @sizeOf(i32);
                break :blk try reader.readInt(i32, .little);
            },
            .object_id => bson_types.BsonObjectId.bson_object_id_size,
            .int32 => @sizeOf(i32),
            .int64 => @sizeOf(i64),
            .double => @sizeOf(f64),
            .boolean => @sizeOf(bool),
            .utc_date_time => @sizeOf(i64),
            .timestamp => @sizeOf(u64),
            .null => @sizeOf(void),
            .decimal128 => bson_types.BsonDecimal128.size_in_bytes,
            else => {
                return error.InvalidElementType;
            },
        };

        if (self.current_element) |current_element| {
            self.allocator.destroy(current_element);
        }

        const current_element = try self.allocator.create(BsonElement);
        errdefer self.allocator.destroy(current_element);

        current_element.* = BsonElement{
            .name = try e_name_array_list.toOwnedSlice(),
            .pos = start_pos,
            .size = @as(usize, @intCast(doc_size)),
            .type = element_type,
            .parent_bytes = self.document.raw_data,
        };
        self.current_element = current_element;

        return current_element;
    }

    /// Finds the first element with the given key in the document, only works for top level elements.
    pub fn findElement(self: *const BsonDocumentIterator, key: []const u8) !?*BsonElement {
        var fbs = std.io.fixedBufferStream(self.document.raw_data);
        var reader = fbs.reader();

        const parent_document_size = @as(usize, @intCast(try reader.readInt(i32, .little)));

        var e_name_array_list = try std.ArrayList(u8).initCapacity(self.allocator, 16);
        defer e_name_array_list.deinit();
        const e_name_writer = e_name_array_list.writer();

        while (reader.context.pos < parent_document_size - 1) {
            const element_type_raw = try reader.readByte();
            const element_type: bson_types.BsonElementType = @enumFromInt(element_type_raw);

            try reader.streamUntilDelimiter(e_name_writer, 0x0, parent_document_size - reader.context.pos);
            var pos = reader.context.pos;
            const doc_size = blk: switch (element_type) {
                .document, .binary, .array => {
                    break :blk try reader.readInt(i32, .little);
                },
                .string => {
                    pos += @sizeOf(i32);
                    break :blk try reader.readInt(i32, .little);
                },
                .object_id => bson_types.BsonObjectId.bson_object_id_size,
                .int32 => @sizeOf(i32),
                .int64 => @sizeOf(i64),
                .double => @sizeOf(f64),
                .boolean => @sizeOf(bool),
                .utc_date_time => @sizeOf(i64),
                .timestamp => @sizeOf(u64),
                .null => @sizeOf(void),
                .decimal128 => bson_types.BsonDecimal128.size_in_bytes,
                else => {
                    return error.InvalidElementType;
                },
            };

            if (std.mem.eql(u8, e_name_array_list.items, key)) {
                const current_element = try self.allocator.create(BsonElement);
                errdefer self.allocator.destroy(current_element);

                current_element.* = BsonElement{
                    .name = try e_name_array_list.toOwnedSlice(),
                    .pos = pos,
                    .size = @as(usize, @intCast(doc_size)),
                    .type = element_type,
                    .parent_bytes = self.document.raw_data,
                };

                return current_element;
            } else {
                if (element_type == .document or element_type == .binary or element_type == .array) {
                    try fbs.seekBy(@as(i64, @intCast(doc_size - @sizeOf(i32))));
                } else {
                    try fbs.seekBy(@as(i64, @intCast(doc_size)));
                }
                e_name_array_list.clearRetainingCapacity();
            }
        }

        return null;
    }
};

pub const BsonElement = struct {
    name: ?[]const u8 = null,
    pos: usize,
    size: usize,
    type: bson_types.BsonElementType,
    parent_bytes: []const u8,

    pub fn dupe(self: *const BsonElement, allocator: Allocator) !*BsonElement {
        const new_element = try allocator.create(BsonElement);
        errdefer allocator.destroy(new_element);
        new_element.name = if (self.name) |name| try allocator.dupe(u8, name) else null;
        new_element.pos = self.pos;
        new_element.size = self.size;
        new_element.type = self.type;
        new_element.parent_bytes = self.parent_bytes;
        return new_element;
    }

    pub fn deinit(self: *const BsonElement, allocator: Allocator) void {
        if (self.name) |name| allocator.free(name);
        allocator.destroy(self);
    }

    /// caller owns the document memory
    pub fn getAsDocumentElement(self: *const BsonElement, allocator: Allocator) !*BsonDocument {
        const bytes = self.parent_bytes[self.pos .. self.pos + self.size];
        return try BsonDocument.loadFromBytes(allocator, bytes);
    }

    pub fn getValueAsDocument(self: *const BsonElement, allocator: Allocator) !*BsonDocument {
        return try BsonDocument.loadFromBytes(allocator, self.getValueBytes());
    }

    pub fn getValueAsArray(self: *const BsonElement) !BsonValArray {
        return try BsonValArray.fromBytes(self.getValueBytes());
    }
    pub fn getValueAsArrayOf(self: *const BsonElement, allocator: Allocator, T: type) ![]T {
        var array_list = std.ArrayList(T).init(allocator);
        defer array_list.deinit();

        var array = try BsonValArray.fromBytes(allocator, self.getValueBytes());
        if (comptime @typeInfo(T) == .@"struct" or (@typeInfo(T) == .pointer and @typeInfo(@typeInfo(T).pointer.child) == .@"struct")) {
            while (try array.next()) |item| {
                if (item.name) |name| allocator.free(name);

                const value = try item.getValueAsWithAllocator(allocator, T);
                try array_list.append(value);
            }
        } else {
            while (try array.next()) |item| {
                if (item.name) |name| allocator.free(name);

                const value = try item.getValueAs(T);
                try array_list.append(value);
            }
        }

        return array_list.toOwnedSlice();
    }

    pub fn getValueWithAllocator(self: *const BsonElement, allocator: Allocator) !BsonValue {
        if (self.type == .null) return .{ .null = {} };
        if (self.type == .document) {
            return .{ .document = try BsonDocument.loadFromBytes(allocator, self.getValueBytes()) };
        }
        if (self.type == .array) {
            return .{ .array = try BsonValArray.fromBytes(allocator, self.getValueBytes()) };
        }

        return getValue(self) catch |err| {
            if (err == error.UnexpectedType) {
                unreachable;
            }
            return err;
        };
    }

    pub fn getValueAsWithAllocator(self: *const BsonElement, allocator: Allocator, T: type) !T {
        if (self.type == .null) {
            return error.UnexpectedType;
        }
        if (self.type == .document) {
            if (T == *BsonDocument) {
                const doc = try BsonDocument.loadFromBytes(allocator, self.getValueBytes());
                errdefer doc.deinit(allocator);
                return doc;
            } else {
                const raw_data = self.getValueBytes();
                const doc = BsonDocument{
                    .len = raw_data.len,
                    .raw_data = raw_data[0..],
                };
                if (comptime @typeInfo(T) == .pointer) {
                    if (comptime @typeInfo(T).pointer.child == u8) {
                        unreachable;
                    }
                    return try doc.toObject(allocator, @typeInfo(T).pointer.child, .{ .ignore_unknown_fields = true });
                } else {
                    return try doc.toObject(allocator, T, .{ .ignore_unknown_fields = true });
                }
            }
        }
        if (self.type == .array) {
            if (T == *BsonValArray) {
                return try BsonValArray.fromBytes(self.getValueBytes());
            } else {
                return error.UnknownElementType;
            }
        }

        return getValueAs(self, T);
    }

    pub fn getValueAs(self: *const BsonElement, T: type) !T {
        if (self.type == .null) {
            if (T != void) return error.UnexpectedType;
            return null;
        }
        if (self.type == .document) return error.UnexpectedType;
        if (self.type == .array) return error.UnexpectedType;
        if (@typeInfo(T) == .optional) {
            return try self.getValueAs(@typeInfo(T).optional.child);
        }

        if (self.type == .string) {
            if (T == []const u8) {
                const bytes = self.getValueBytes();
                return bytes[0 .. bytes.len - 1];
            }
            if (T == [:0]const u8) return self.getValueBytesZ();
            return error.UnexpectedType;
        }

        const value_bytes = self.getValueBytes();

        switch (self.type) {
            .double => {
                if (T != f64) return error.UnexpectedType;
                return std.mem.bytesToValue(T, value_bytes);
            },
            .string => {
                unreachable;
            },
            .int32 => {
                switch (T) {
                    i32 => {
                        return std.mem.bytesToValue(i32, value_bytes);
                    },
                    i64 => {
                        const num = std.mem.bytesToValue(i32, value_bytes);
                        return @as(i64, @intCast(num));
                    },
                    f64 => {
                        return std.mem.bytesToValue(f64, value_bytes);
                    },
                    else => {
                        return error.UnexpectedType;
                    },
                }
            },
            .int64 => {
                switch (T) {
                    i64, f64 => {
                        return std.mem.bytesToValue(T, value_bytes);
                    },
                    else => {
                        return error.UnexpectedType;
                    },
                }
            },
            .boolean => {
                if (T != bool) return error.UnexpectedType;
                return value_bytes[0] == 0x1;
            },
            .utc_date_time => {
                if (T == bson_types.BsonUtcDatetime) {
                    const date = std.mem.bytesToValue(i64, value_bytes);
                    return bson_types.BsonUtcDatetime.fromInt64(date);
                }
                if (T == i64) {
                    return std.mem.bytesToValue(i64, value_bytes);
                }
                return error.UnexpectedType;
            },
            .timestamp => {
                const timestamp_value = std.mem.bytesToValue(i64, value_bytes);
                const timestamp = bson_types.BsonTimestamp.fromInt64(timestamp_value);
                switch (T) {
                    bson_types.BsonTimestamp => {
                        return timestamp;
                    },
                    u64 => {
                        return timestamp.value;
                    },
                    else => {
                        return error.UnexpectedType;
                    },
                }
            },
            .binary => {
                if (T != []const u8) return error.UnexpectedType;
                return value_bytes;
            },
            .document, .array, .null => {
                unreachable;
            },
            .object_id => {
                if (T != bson_types.BsonObjectId) {
                    return error.UnexpectedType;
                }
                return try bson_types.BsonObjectId.fromBytes(value_bytes);
            },
            .decimal128 => {
                if (T != bson_types.BsonDecimal128) return error.UnexpectedType;
                return try bson_types.BsonDecimal128.fromBytes(value_bytes);
            },
            else => {
                return error.InvalidElementType;
            },
        }
    }

    pub fn getValue(self: *const BsonElement) !BsonValue {
        if (self.type == .null) return .{ .null = {} };
        if (self.type == .document or self.type == .array) return error.UnexpectedType;

        const value_bytes = self.getValueBytes();

        switch (self.type) {
            .double => {
                return .{ .double = std.mem.bytesToValue(f64, value_bytes) };
            },
            .string => {
                return .{ .string = value_bytes };
            },
            .int32 => {
                return .{ .int32 = std.mem.bytesToValue(i32, value_bytes) };
            },
            .int64 => {
                return .{ .int64 = std.mem.bytesToValue(i64, value_bytes) };
            },
            .boolean => {
                return .{ .boolean = value_bytes[0] == 0x01 };
            },
            .utc_date_time => {
                const date = std.mem.bytesToValue(i64, value_bytes);
                return .{ .date = bson_types.BsonUtcDatetime.fromInt64(date) };
            },
            .timestamp => {
                const timestamp_value = std.mem.bytesToValue(i64, value_bytes);
                const timestamp = bson_types.BsonTimestamp.fromInt64(timestamp_value);
                return .{ .timestamp = timestamp };
            },
            .binary => {
                return .{ .binary = value_bytes };
            },
            .document, .array, .null => {
                unreachable;
            },
            .object_id => {
                return .{ .object_id = try bson_types.BsonObjectId.fromBytes(value_bytes) };
            },
            .decimal128 => {
                return .{ .decimal128 = try bson_types.BsonDecimal128.fromBytes(value_bytes) };
            },
            else => {
                return error.InvalidElementType;
            },
        }
    }

    pub fn isNullOrEmpty(self: *const BsonElement) !bool {
        if (self.type == .null) return true;
        return self.size == 0;
    }

    pub fn hasValue(self: *const BsonElement, T: type) !bool {
        _ = T;
        if (self.type == .null) return false;
        return self.size > 0;
    }

    pub fn endPos(self: *const BsonElement) usize {
        return self.pos + self.size;
    }

    pub fn getValueBytes(self: *const BsonElement) []const u8 {
        const start_pos = self.pos;
        const end_pos = start_pos + self.size;
        return self.parent_bytes[start_pos..end_pos];
    }
    pub fn getValueBytesZ(self: *const BsonElement) [:0]const u8 {
        const start_pos = self.pos;
        const end_pos = start_pos + self.size;
        return self.parent_bytes[start_pos .. end_pos - 1 :0];
    }

    pub fn getValueBytesOwned(self: *const BsonElement, allocator: Allocator) ![]const u8 {
        const bytes = self.getValueBytes();
        const new_bytes = try allocator.dupe(u8, bytes);
        return new_bytes;
    }
};

pub const BsonValue = union(enum) {
    int32: i32,
    int64: i64,
    double: f64,
    string: []const u8,
    document: *BsonDocument,
    array: BsonValArray,
    boolean: bool,
    null: void,
    date: bson_types.BsonUtcDatetime,
    timestamp: bson_types.BsonTimestamp,
    binary: []const u8,
    object_id: bson_types.BsonObjectId,
    decimal128: bson_types.BsonDecimal128,

    pub fn deinit(self: *const BsonValue, allocator: Allocator) void {
        switch (self.*) {
            .document => |doc| doc.deinit(allocator),
            .array => |arr| arr.deinit(allocator),
            .binary => |bin| allocator.free(bin),
            else => {},
        }
    }
};

pub const BsonValArray = struct {
    allocator: Allocator,
    next_array_index: usize = 0,
    fbs: std.io.FixedBufferStream([]const u8),

    pub fn deinit(self: *const BsonValArray, allocator: Allocator) void {
        _ = allocator;
        _ = self;
    }

    pub fn fromBytes(allocator: Allocator, bytes: []const u8) !BsonValArray {
        var fbs = std.io.fixedBufferStream(bytes);
        fbs.pos = @sizeOf(i32); // skip the array document length
        return .{
            .allocator = allocator,
            .fbs = fbs,
        };
    }

    pub fn next(self: *BsonValArray) !?BsonElement {
        if (self.fbs.buffer.len - @sizeOf(u8) <= self.fbs.pos) return null;

        var reader = self.fbs.reader();

        const element_type_raw = try reader.readByte();
        const element_type: bson_types.BsonElementType = @enumFromInt(element_type_raw);

        var buf_array_item_name: [6]u8 = undefined;

        var name_fbs = std.io.fixedBufferStream(buf_array_item_name[0..]);
        const writer = name_fbs.writer();

        reader.streamUntilDelimiter(writer, 0x0, buf_array_item_name.len) catch {
            return error.UnexpectedArrayItemName;
        };

        const element_item_name = name_fbs.getWritten();
        var buf: [16]u8 = undefined;
        const element_item_index = try std.fmt.bufPrint(buf[0..], "{d}", .{self.next_array_index});
        if (!std.mem.eql(u8, element_item_name, element_item_index)) {
            return error.UnexpectedArrayItemName;
        }

        var value_pos = self.fbs.pos;

        const doc_size = blk: switch (element_type) {
            .document => {
                break :blk @as(usize, @intCast(try reader.readInt(i32, .little)));
            },
            .binary, .array => {
                break :blk @as(usize, @intCast(try reader.readInt(i32, .little)));
            },
            .string => {
                value_pos += @sizeOf(i32);

                break :blk @as(usize, @intCast(try reader.readInt(i32, .little)));
            },
            .object_id => bson_types.BsonObjectId.bson_object_id_size,
            .int32 => @sizeOf(i32),
            .int64 => @sizeOf(i64),
            .double => @sizeOf(f64),
            .boolean => @sizeOf(bool),
            .utc_date_time => @sizeOf(i64),
            .timestamp => @sizeOf(u64),
            .null => @sizeOf(void),
            .decimal128 => bson_types.BsonDecimal128.size_in_bytes,
            else => {
                return error.InvalidElementType;
            },
        };

        self.next_array_index += 1;

        if (element_type == .document or element_type == .binary or element_type == .array) {
            try self.fbs.seekBy(@as(i64, @intCast(doc_size - @sizeOf(i32))));
        } else {
            try self.fbs.seekBy(@as(i64, @intCast(doc_size)));
        }

        return BsonElement{
            .type = element_type,
            .name = try self.allocator.dupe(u8, element_item_name),
            .pos = value_pos,
            .size = doc_size,
            .parent_bytes = self.fbs.buffer,
        };
    }
};
