const std = @import("std");
const Allocator = std.mem.Allocator;

const Reader = std.Io.Reader;
const Writer = std.Io.Writer;

const bson_writer = @import("bson-writer.zig");
const ext_json_parser = @import("bson-ext-json-parser.zig");
const jsonStringToBson = ext_json_parser.jsonStringToBson;
const ext_json_serializer = @import("bson-ext-json-serializer.zig");
const bson_iterator = @import("bson-iterator.zig");
pub const bson_types = @import("bson-types.zig");
const bson_parser = @import("bson-parser.zig");

pub const BsonElement = bson_iterator.BsonElement;
pub const BsonValue = bson_iterator.BsonValue;
pub const NullIgnoredFieldNames = bson_writer.NullIgnoredFieldNames;
pub const BsonDocumentIterator = bson_iterator.BsonDocumentIterator;
pub const BsonDocumentView = @import("./document-view.zig").BsonDocumentView;

pub const BsonDocumentReadError = (Allocator.Error || Reader.Error);

pub const BsonDocument = struct {
    len: usize,
    raw_data: []const u8,

    pub fn loadFromBytes(allocator: Allocator, raw_data: []const u8) Allocator.Error!*BsonDocument {
        var document = try allocator.create(BsonDocument);
        errdefer document.deinit(allocator);
        document.len = raw_data.len;
        document.raw_data = try allocator.dupe(u8, raw_data);
        return document;
    }

    pub fn readDocument(allocator: Allocator, reader: *Reader) BsonDocumentReadError!*BsonDocument {
        const document_len = try reader.takeInt(i32, .little);
        const size = @as(usize, @intCast(document_len));

        var raw_data = try allocator.alloc(u8, size);
        errdefer allocator.free(raw_data);

        @memcpy(raw_data[0..@sizeOf(i32)], std.mem.asBytes(&document_len));

        var pos: usize = @sizeOf(i32);

        while (pos < size) {
            const amt = try reader.readSliceShort(raw_data[pos..]);
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

    pub fn deinit(self: *const BsonDocument, allocator: Allocator) void {
        allocator.free(self.raw_data);
        allocator.destroy(self);
    }

    pub fn toJsonString(self: *const BsonDocument, allocator: Allocator, comptime is_strict_ext_json: bool) ![]const u8 {
        return ext_json_serializer.toJsonString(self, allocator, is_strict_ext_json);
    }

    pub fn fromJsonString(allocator: Allocator, json_string: []const u8) !*BsonDocument {
        return try jsonStringToBson(allocator, json_string);
    }

    pub fn fromJsonReader(allocator: Allocator, reader: *std.Io.Reader) !*BsonDocument {
        return try ext_json_parser.jsonReaderToBson(allocator, reader, false);
    }

    pub fn fromObject(allocator: Allocator, comptime T: type, obj: T) !*BsonDocument {
        return try bson_writer.writeToBson(
            T,
            obj,
            allocator,
        );
    }

    pub fn toObject(doc: *const BsonDocument, allocator: Allocator, comptime T: type, options: bson_parser.ParseBsonToObjectOptions) !*T {
        const instance = try allocator.create(T);
        errdefer allocator.destroy(instance);

        try bson_parser.parseBsonToObject(allocator, T, instance, doc, options);
        return instance;
    }

    pub fn dupe(self: *const BsonDocument, allocator: Allocator) Allocator.Error!*BsonDocument {
        const new_data = try allocator.dupe(u8, self.raw_data);
        const new_doc = try allocator.create(BsonDocument);
        new_doc.len = self.len;
        new_doc.raw_data = new_data;
        return new_doc;
    }
};

test {
    _ = @import("datetime.zig");
    _ = @import("bson-tests.zig");
    _ = @import("bson-types.zig");
    _ = @import("bson-parser.zig");
}
