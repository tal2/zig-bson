const std = @import("std");
const Allocator = std.mem.Allocator;

pub const BsonDocument = struct {
    len: usize,
    raw_data: []const u8,

    pub fn deinit(self: *BsonDocument, allocator: Allocator) void {
        allocator.free(self.raw_data);
        allocator.destroy(self);
    }
};

test {
    _ = @import("datetime.zig");
    _ = @import("bson-tests.zig");
}
