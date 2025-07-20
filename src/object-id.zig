const std = @import("std");
const time = std.time;

pub const BsonObjectIdError = error{ ValueSizeNot24Bytes, InvalidCharacter };
pub const JsonParseError = error{UnexpectedToken} || std.json.Scanner.NextError;

var global_counter: u24 = undefined;
var global_generator_initialized: bool = false;
var bson_object_id_process_random_bytes: [5]u8 = undefined;

fn getCounterNext() u24 {
    global_counter +%= 1; // 2^24
    return global_counter;
}

pub const BsonObjectId = struct {
    pub const bson_object_id_size = 12;
    pub const bson_object_id_as_string_size = bson_object_id_size * 2;

    value: [12]u8,

    pub fn initializeGenerator() void {
        if (global_generator_initialized == true) {
            return;
        }
        const seed = @as(u64, @intCast(time.milliTimestamp()));
        var prng = std.Random.DefaultPrng.init(seed);

        global_counter = prng.random().int(u24);
        global_generator_initialized = true;

        prng.random().bytes(bson_object_id_process_random_bytes[0..5]);
    }

    pub fn generate() BsonObjectId {
        return BsonObjectId.init(getCounterNext(), bson_object_id_process_random_bytes);
    }

    fn init(counter: u24, random_bytes: [5]u8) BsonObjectId {
        var id: BsonObjectId = undefined;

        const now = time.timestamp();
        const timestamp: u32 = @intCast(now);

        // Set timestamp field (seconds since Unix epoch)
        std.mem.writeInt(u32, id.value[0..4], timestamp, .big);

        // Set unique process random bytes
        @memcpy(id.value[4..9], &random_bytes);

        // Set current counter
        std.mem.writeInt(u24, id.value[9..12], counter, .big);

        return id;
    }

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

    pub fn toString(self: *const BsonObjectId, case: std.fmt.Case) [24]u8 {
        return std.fmt.bytesToHex(self.value[0..12], case);
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
    const id = try BsonObjectId.fromString(valid_hex);
    const back_to_hex = id.toString(.lower);
    try std.testing.expectEqualStrings(valid_hex, &back_to_hex);
}

test "ObjectId basic functionality" {
    BsonObjectId.initializeGenerator();
    const id1 = BsonObjectId.generate();
    const id2 = BsonObjectId.generate();

    // ObjectIds should be different when created at different times
    try std.testing.expect(!BsonObjectId.isEqualTo(&id1, &id2));

    // ObjectIds should be equal to themselves
    try std.testing.expect(BsonObjectId.isEqualTo(&id1, &id1));
    try std.testing.expect(BsonObjectId.isEqualTo(&id2, &id2));
}

test "ObjectId timestamp field" {
    BsonObjectId.initializeGenerator();
    const id = BsonObjectId.generate();
    const timestamp = id.getTimestamp();
    const unix_timestamp = id.getUnixTimestamp();

    // Timestamp should be reasonable (not 0, not too far in the future)
    try std.testing.expect(timestamp > 0);
    try std.testing.expect(timestamp < 0xFFFFFFFF);
    try std.testing.expectEqual(@as(i64, timestamp), unix_timestamp);

    // Test specific timestamp values from spec
    var test_id: BsonObjectId = undefined;

    // Test 0x00000000: Jan 1st, 1970 00:00:00 UTC
    test_id.value[0] = 0x00;
    test_id.value[1] = 0x00;
    test_id.value[2] = 0x00;
    test_id.value[3] = 0x00;
    try std.testing.expectEqual(@as(u32, 0), test_id.getTimestamp());

    // Test 0x7FFFFFFF: Jan 19th, 2038 03:14:07 UTC
    test_id.value[0] = 0x7F;
    test_id.value[1] = 0xFF;
    test_id.value[2] = 0xFF;
    test_id.value[3] = 0xFF;
    try std.testing.expectEqual(@as(u32, 0x7FFFFFFF), test_id.getTimestamp());

    // Test 0x80000000: Jan 19th, 2038 03:14:08 UTC
    test_id.value[0] = 0x80;
    test_id.value[1] = 0x00;
    test_id.value[2] = 0x00;
    test_id.value[3] = 0x00;
    try std.testing.expectEqual(@as(u32, 0x80000000), test_id.getTimestamp());

    // Test 0xFFFFFFFF: Feb 7th, 2106 06:28:15 UTC
    test_id.value[0] = 0xFF;
    test_id.value[1] = 0xFF;
    test_id.value[2] = 0xFF;
    test_id.value[3] = 0xFF;
    try std.testing.expectEqual(@as(u32, 0xFFFFFFFF), test_id.getTimestamp());
}

test "ObjectId hex string conversion" {
    BsonObjectId.initializeGenerator();
    const id = BsonObjectId.generate();
    const hex_string = id.toString(.lower);

    // Hex string should be 24 characters long
    try std.testing.expectEqual(@as(usize, 24), hex_string.len);

    // All characters should be valid hex digits
    for (hex_string) |c| {
        try std.testing.expect((c >= '0' and c <= '9') or (c >= 'a' and c <= 'f'));
    }

    // Converting back should give the same ObjectId
    const parsed_id = try BsonObjectId.fromString(&hex_string);
    try std.testing.expect(BsonObjectId.isEqualTo(&id, &parsed_id));
}

test "ObjectId counter overflow" {
    BsonObjectId.initializeGenerator();
    // Reset global counter for this test
    global_counter = 16777214; // Just before overflow

    const id1 = BsonObjectId.generate();
    const id2 = BsonObjectId.generate();
    const id3 = BsonObjectId.generate();

    // Counter should overflow from 16777215 to 0
    const counter1 = (@as(u24, id1.value[9]) << 16) | (@as(u24, id1.value[10]) << 8) | @as(u24, id1.value[11]);
    const counter2 = (@as(u24, id2.value[9]) << 16) | (@as(u24, id2.value[10]) << 8) | @as(u24, id2.value[11]);
    const counter3 = (@as(u24, id3.value[9]) << 16) | (@as(u24, id3.value[10]) << 8) | @as(u24, id3.value[11]);

    try std.testing.expectEqual(@as(u24, 16777215), counter1);
    try std.testing.expectEqual(@as(u24, 0), counter2);
    try std.testing.expectEqual(@as(u24, 1), counter3);
}

test "ObjectId generation uniqueness" {
    BsonObjectId.initializeGenerator();
    const id1 = BsonObjectId.generate();
    const id2 = BsonObjectId.generate();
    const id3 = BsonObjectId.generate();

    // Generated ObjectIds should be unique
    try std.testing.expect(!BsonObjectId.isEqualTo(&id1, &id2));
    try std.testing.expect(!BsonObjectId.isEqualTo(&id1, &id3));
    try std.testing.expect(!BsonObjectId.isEqualTo(&id2, &id3));

    // Timestamps should be the same or very close (within 1 second)
    const ts1 = id1.getTimestamp();
    const ts2 = id2.getTimestamp();
    const ts3 = id3.getTimestamp();

    try std.testing.expect(ts2 >= ts1);
    try std.testing.expect(ts3 >= ts2);
    try std.testing.expect(ts3 - ts1 <= 1); // Should be within 1 second
}

test "ObjectId structure validation" {
    BsonObjectId.initializeGenerator();
    const id = BsonObjectId.generate();
    // Verify the 12-byte structure
    try std.testing.expectEqual(@as(usize, 12), id.value.len);

    // All bytes should be initialized (not undefined)
    for (id.value) |byte| {
        _ = byte; // Just check that it's accessible
    }
}
