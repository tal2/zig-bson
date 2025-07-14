const std = @import("std");
const Allocator = std.mem.Allocator;

const jsonStringToBson = @import("bson-ext-json-parser.zig").jsonStringToBson;
const ext_json_serializer = @import("bson-ext-json-serializer.zig");

pub const BsonDocument = struct {
    len: usize,
    raw_data: []const u8,

    pub fn readDocument(allocator: Allocator, reader: anytype) !*BsonDocument {
        const document_len = try reader.readInt(i32, .little);
        const size = @as(usize, @intCast(document_len));

        var raw_data = try allocator.alloc(u8, size);
        errdefer allocator.free(raw_data);

        @memcpy(raw_data[0..@sizeOf(i32)], std.mem.asBytes(&document_len));

        var pos: usize = @sizeOf(i32);

        while (pos < size) {
            const amt = try reader.read(raw_data[pos..]);
            if (amt == 0) {
                return error.EndOfStream;
            }
            pos += amt;
        }

        std.debug.assert(pos == size);

        var document = try allocator.create(BsonDocument);
        errdefer document.deinit(allocator);
        document.len = size;
        document.raw_data = raw_data;
        return document;
    }

    pub fn deinit(self: *BsonDocument, allocator: Allocator) void {
        allocator.free(self.raw_data);
        allocator.destroy(self);
    }

    pub fn toJsonString(self: *const BsonDocument, allocator: Allocator, comptime is_strict_ext_json: bool) ![]const u8 {
        return ext_json_serializer.toJsonString(self, allocator, is_strict_ext_json);
    }

    pub fn fromJsonString(allocator: Allocator, json_string: []const u8) !*BsonDocument {
        return try jsonStringToBson(allocator, json_string);
    }
};

test {
    _ = @import("datetime.zig");
    _ = @import("bson-tests.zig");
}
