const std = @import("std");
const time = std.time;

pub const BsonObjectIdError = error{ ValueSizeNot24Bytes, InvalidCharacter };
pub const JsonParseError = error{UnexpectedToken} || std.json.Scanner.NextError;

pub const BsonObjectId = struct {
    pub const bson_object_id_size = 12;
    pub const bson_object_id_as_string_size = bson_object_id_size * 2;

    value: [12]u8,

    pub fn isEqualTo(self: *const BsonObjectId, b: *const BsonObjectId) bool {
        return self == b or std.mem.eql(u8, &self.value, &b.value);
    }

    pub fn fromString(hex: []const u8) BsonObjectIdError!BsonObjectId {
        if (hex.len != bson_object_id_as_string_size) {
            return BsonObjectIdError.ValueSizeNot24Bytes;
        }

        var id: BsonObjectId = undefined;
        _ = std.fmt.hexToBytes(&id.value, hex) catch |err| {
            return switch (err) {
                error.InvalidCharacter => BsonObjectIdError.InvalidCharacter,
                else => unreachable,
            };
        };

        return id;
    }

    pub fn jsonParse(allocator: std.mem.Allocator, source: *std.json.Scanner, options: std.json.ParseOptions) JsonParseError!BsonObjectId {
        _ = allocator;
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

        return BsonObjectId.fromString(value) catch return error.UnexpectedToken;
    }

    pub fn init(counter: u24, random: std.Random) BsonObjectId {
        var id: BsonObjectId = undefined;

        // Get current timestamp (seconds since Unix epoch)
        const now = time.timestamp();
        const timestamp: u32 = @intCast(@as(u64, @intCast(now)));

        // Set timestamp field (4 bytes, big endian)
        id.value[0] = @intCast((timestamp >> 24) & 0xFF);
        id.value[1] = @intCast((timestamp >> 16) & 0xFF);
        id.value[2] = @intCast((timestamp >> 8) & 0xFF);
        id.value[3] = @intCast(timestamp & 0xFF);

        random.bytes(id.value[4..9]);

        // // Set counter (3 bytes, big endian)
        id.value[9] = @intCast((counter >> 16) & 0xFF);
        id.value[10] = @intCast((counter >> 8) & 0xFF);
        id.value[11] = @intCast(counter & 0xFF);

        return id;
    }

    /// Get the timestamp as seconds since Unix epoch
    pub fn getTimestamp(self: *const BsonObjectId) u32 {
        return (@as(u32, self.value[0]) << 24) |
            (@as(u32, self.value[1]) << 16) |
            (@as(u32, self.value[2]) << 8) |
            @as(u32, self.value[3]);
    }

    pub fn getUnixTimestamp(self: *const BsonObjectId) i64 {
        return @as(i64, self.getTimestamp());
    }

    pub fn toString(self: *const BsonObjectId) [24]u8 {
        return std.fmt.bytesToHex(self.value[0..12], .lower);
    }
};

test "BsonObjectId fromString validation" {
    // Test invalid length
    try std.testing.expectError(BsonObjectIdError.ValueSizeNot24Bytes, BsonObjectId.fromString("123"));
    try std.testing.expectError(BsonObjectIdError.ValueSizeNot24Bytes, BsonObjectId.fromString("123456789012345678901234567890"));

    // Test invalid hex characters
    try std.testing.expectError(error.InvalidCharacter, BsonObjectId.fromString("12345678901234567890123g"));
    try std.testing.expectError(error.InvalidCharacter, BsonObjectId.fromString("12345678901234567890123!"));

    // Test valid hex string
    const valid_hex = "507f1f77bcf86cd799439011";
    std.debug.print("valid_hex: {s}\n", .{valid_hex});
    const id = try BsonObjectId.fromString(valid_hex);
    const back_to_hex = id.toString();
    try std.testing.expectEqualStrings(valid_hex, &back_to_hex);
}
