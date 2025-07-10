const std = @import("std");
const builtin = @import("builtin");
const datetime = @import("datetime.zig");
const utils = @import("utils.zig");
const colors = @import("colors.zig");
const bson_types = @import("bson-types.zig");

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
const BsonDecimal128 = bson_types.BsonDecimal128;

const FixedBufferStreamJsonReader = std.io.FixedBufferStream([]const u8).Reader;
const JsonReader = std.json.Reader(0x1000, FixedBufferStreamJsonReader);

pub const BsonDocument = struct {
    len: usize,
    raw_data: []const u8,

    pub fn deinit(self: *BsonDocument, allocator: Allocator) void {
        allocator.free(self.raw_data);
    }
};

test {
    _ = datetime;
    _ = @import("bson-tests.zig");
    _ = @import("bson-corpus-tests.zig");
}
