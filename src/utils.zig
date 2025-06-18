const std = @import("std");

const math = std.math;
const fmt = std.fmt;

pub inline fn assertIsInt(comptime T: type) void {
    comptime std.debug.assert(T == i32 or T == u32 or T == i64 or T == u64 or T == i128 or T == u128 or T == usize);
}
pub inline fn assertIsFloat(comptime T: type) void {
    comptime std.debug.assert(T == f32 or T == f64);
}
pub inline fn assertIsNumber(comptime T: type) void {
    comptime std.debug.assert(T == i32 or T == u32 or T == i64 or T == u64 or T == i128 or T == u128 or T == usize or T == f32 or T == f64);
}

pub fn encodeFromReaderToWriter(encoder: *const std.base64.Base64Encoder, destWriter: anytype, sourceReader: anytype, limit: usize) !void {
    var remaining = limit;
    while (remaining > 0) {
        if (remaining == 1) {
            const bytesRead = try sourceReader.readBoundedBytes(1);
            if (bytesRead.len == 0) {
                break;
            }
            remaining -= bytesRead.len;

            var temp: [4]u8 = undefined;
            const s = encoder.encode(&temp, bytesRead.slice());
            try destWriter.writeAll(s);
            break;
        } else if (remaining == 2) {
            const bytesRead = try sourceReader.readBoundedBytes(2);
            if (bytesRead.len == 0) {
                break;
            }
            remaining -= bytesRead.len;

            var temp: [5]u8 = undefined;
            const s = encoder.encode(&temp, bytesRead.slice());
            try destWriter.writeAll(s);
            break;
        } else {
            const bytesRead = try sourceReader.readBoundedBytes(3);

            if (bytesRead.len == 0) {
                break;
            }
            remaining -= bytesRead.len;

            var temp: [5]u8 = undefined;
            const s = encoder.encode(&temp, bytesRead.slice());
            try destWriter.writeAll(s);
        }
    }
}

pub inline fn bytesFromBase64(allocator: std.mem.Allocator, str: []const u8) ![]u8 {
    const expected_size = try std.base64.standard.Decoder.calcSizeForSlice(str);
    const expected_data = try allocator.alloc(u8, expected_size);
    try std.base64.standard.Decoder.decode(expected_data, str);
    return expected_data;
}

pub fn normalizeJsonString(allocator: std.mem.Allocator, json: []const u8) ![]u8 {
    const json_obj = try std.json.parseFromSlice(std.json.Value, allocator, json, .{});
    var data_writer = try std.ArrayList(u8).initCapacity(allocator, json.len);
    defer data_writer.deinit();
    const stringify_options = std.json.StringifyOptions{ .whitespace = .minified };
    var writer = std.json.writeStream(data_writer.writer(), stringify_options);
    try json_obj.value.jsonStringify(&writer);
    return data_writer.toOwnedSlice();
}
