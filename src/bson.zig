const std = @import("std");
const Allocator = std.mem.Allocator;

const jsonStringToBson = @import("bson-ext-json-parser.zig").jsonStringToBson;
const ext_json_serializer = @import("bson-ext-json-serializer.zig");

pub const BsonDocument = struct {
    len: usize,
    raw_data: []const u8,

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
