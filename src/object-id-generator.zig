const std = @import("std");
const time = std.time;

pub const BsonObjectId = @import("./bson-types.zig").BsonObjectId;

var global_counter: u24 = undefined;
var global_counter_initialized: bool = false;

/// Shares counter between all instances of ObjectIdGenerator
pub const BsonObjectIdGenerator = struct {
    random: std.Random.DefaultPrng,

    pub fn init() BsonObjectIdGenerator {
        const seed = @as(u64, @intCast(time.milliTimestamp()));
        var prng = std.Random.DefaultPrng.init(seed);
        var random = prng.random();

        if (global_counter_initialized == false) {
            global_counter = random.int(u24);
            global_counter_initialized = true;
        }

        return .{
            .random = prng,
        };
    }

    /// Generate a new ObjectId with an incrementing counter
    pub fn generateObjectId(self: *BsonObjectIdGenerator) BsonObjectId {
        var r = self.random;
        const random = r.random();
        return BsonObjectId.init(getCounterNext(), random);
    }

    fn getCounterNext() u24 {
        global_counter +%= 1; // 2^24
        return global_counter;
    }
};

test "ObjectId basic functionality" {
    var generator = BsonObjectIdGenerator.init();
    const id1 = generator.generateObjectId();
    const id2 = generator.generateObjectId();

    // ObjectIds should be different when created at different times
    try std.testing.expect(!BsonObjectId.isEqualTo(&id1, &id2));

    // ObjectIds should be equal to themselves
    try std.testing.expect(BsonObjectId.isEqualTo(&id1, &id1));
    try std.testing.expect(BsonObjectId.isEqualTo(&id2, &id2));
}

test "ObjectId timestamp field" {
    var generator = BsonObjectIdGenerator.init();
    const id = generator.generateObjectId();
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
    var generator = BsonObjectIdGenerator.init();
    const id = generator.generateObjectId();
    const hex_string = id.toString();

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
    // Reset global counter for this test
    global_counter = 16777214; // Just before overflow

    var generator = BsonObjectIdGenerator.init();
    const id1 = generator.generateObjectId();
    const id2 = generator.generateObjectId();
    const id3 = generator.generateObjectId();

    // Counter should overflow from 16777215 to 0
    const counter1 = (@as(u24, id1.value[9]) << 16) | (@as(u24, id1.value[10]) << 8) | @as(u24, id1.value[11]);
    const counter2 = (@as(u24, id2.value[9]) << 16) | (@as(u24, id2.value[10]) << 8) | @as(u24, id2.value[11]);
    const counter3 = (@as(u24, id3.value[9]) << 16) | (@as(u24, id3.value[10]) << 8) | @as(u24, id3.value[11]);

    try std.testing.expectEqual(@as(u24, 16777215), counter1);
    try std.testing.expectEqual(@as(u24, 0), counter2);
    try std.testing.expectEqual(@as(u24, 1), counter3);
}

test "ObjectId generation uniqueness" {
    var generator = BsonObjectIdGenerator.init();
    const id1 = generator.generateObjectId();
    const id2 = generator.generateObjectId();
    const id3 = generator.generateObjectId();

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
    var generator = BsonObjectIdGenerator.init();
    const id = generator.generateObjectId();
    // Verify the 12-byte structure
    try std.testing.expectEqual(@as(usize, 12), id.value.len);

    // All bytes should be initialized (not undefined)
    for (id.value) |byte| {
        _ = byte; // Just check that it's accessible
    }
}
