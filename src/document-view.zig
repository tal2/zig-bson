const std = @import("std");
const bson = @import("./bson.zig");
const bson_iterator = @import("./bson-iterator.zig");

const Allocator = std.mem.Allocator;
const BsonDocument = bson.BsonDocument;
const BsonValue = bson_iterator.BsonValue;
const BsonElement = bson_iterator.BsonElement;
const BsonDocumentIterator = bson_iterator.BsonDocumentIterator;

pub const BsonDocumentView = struct {
    allocator: Allocator,
    document: *const BsonDocument,

    /// Does not own the document.
    pub fn loadDocument(allocator: Allocator, document: *const BsonDocument) BsonDocumentView {
        return .{
            .allocator = allocator,
            .document = document,
        };
    }

    pub fn get(self: *const BsonDocumentView, key: []const u8) !?BsonValue {
        const element = try self.getElement(key);
        if (element) |e| {
            defer e.deinit(self.allocator);
            return try e.getValueWithAllocator(self.allocator);
        }
        return null;
    }

    pub fn getBytes(self: *const BsonDocumentView, key: []const u8) !?[]const u8 {
        const element = try self.getElement(key);
        if (element) |e| {
            return e.getValueBytes();
        }
        return null;
    }

    pub fn getAsBsonDocumentElement(self: *const BsonDocumentView, key: []const u8) !?*BsonDocument {
        const element = try self.getElement(key);
        if (element) |e| {
            defer e.deinit(self.allocator);
            return try e.getAsDocumentElement(self.allocator);
        }
        return null;
    }

    pub fn getElement(self: *const BsonDocumentView, key: []const u8) !?*BsonElement {
        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();
        const arena_allocator = arena.allocator();

        var it = try BsonDocumentIterator.init(arena_allocator, self.document);
        errdefer it.deinit();

        const element = try it.findElement(key);
        if (element) |e| {
            return try e.dupe(self.allocator);
        }

        return null;
    }

    pub fn checkElementValue(self: *const BsonDocumentView, key: []const u8, value: anytype) !bool {
        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();
        const arena_allocator = arena.allocator();

        var it = try BsonDocumentIterator.init(arena_allocator, self.document);
        errdefer it.deinit();

        const element = try it.findElement(key);
        if (element) |e| {
            const element_value = try e.getValueAs(@TypeOf(value));
            return (element_value == value);
        }
        return false;
    }

    pub fn isNullOrEmpty(self: *const BsonDocumentView, key: []const u8) !bool {
        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();
        const arena_allocator = arena.allocator();

        var it = try BsonDocumentIterator.init(arena_allocator, self.document);
        errdefer it.deinit();

        const element = try it.findElement(key);
        if (element) |e| {
            return try e.isNullOrEmpty();
        }
        return true;
    }
};
