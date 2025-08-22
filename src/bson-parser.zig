const std = @import("std");

const Allocator = std.mem.Allocator;
const ArenaAllocator = std.heap.ArenaAllocator;
const Reader = std.io.Reader;

const bson = @import("./bson.zig");
const BsonDocument = bson.BsonDocument;
const BsonDocumentView = bson.BsonDocumentView;

const bson_iterator = @import("./bson-iterator.zig");
const BsonDocumentIterator = bson_iterator.BsonDocumentIterator;

const bson_types = @import("./bson-types.zig");
const BsonDecimal128 = bson_types.BsonDecimal128;

pub const ParseBsonToObjectOptions = struct {
    ignore_unknown_fields: bool = false,
};

pub fn parseBsonToObject(allocator: Allocator, T: type, instance: *T, doc: *const BsonDocument, options: ParseBsonToObjectOptions) !void {
    var arena = ArenaAllocator.init(allocator);
    defer arena.deinit();
    const arena_allocator = arena.allocator();

    const view = BsonDocumentView.loadDocument(arena_allocator, doc);

    inline for (std.meta.fields(T)) |field| {
        const field_name = field.name;
        const is_optional = comptime @typeInfo(field.type) == .optional;
        const field_type = comptime if (is_optional) @typeInfo(field.type).optional.child else field.type;
        comptime if (field_type == void) {
            unreachable;
        };
        const field_type_info = comptime @typeInfo(field_type);
        const field_bson_element = try view.getElement(field_name);
        if (field_bson_element) |bson_element| {
            defer bson_element.deinit(arena_allocator);
            switch (field_type_info) {
                .optional, .array => {
                    unreachable;
                },
                .bool,
                .int,
                .float,
                => {
                    const field_value = try bson_element.getValueAs(field_type);
                    @field(instance, field_name) = field_value;
                },
                .@"union" => {
                    const union_field_fields = std.meta.fields(field_type);

                    switch (bson_element.type) {
                        .string => {
                            const zig_type = []const u8;
                            const field_value = try bson_element.getValueAs(zig_type);
                            const field_value_copy: []const u8 = try allocator.dupe(u8, field_value);
                            errdefer allocator.free(field_value_copy);

                            inline for (union_field_fields) |field_| {
                                if (field_.type == zig_type) {
                                    @field(instance, field_name) = @unionInit(field_type, field_.name, field_value_copy);
                                }
                            }
                        },
                        .boolean => {
                            const field_value = try bson_element.getValueAs(bool);

                            inline for (union_field_fields) |field_| {
                                if (field_.type == bool) {
                                    @field(instance, field_name) = @unionInit(field_type, field_.name, field_value);
                                }
                            }
                        },
                        .int32 => {
                            var is_set = false;
                            const field_value = try bson_element.getValueAs(i32);
                            inline for (union_field_fields) |field_| {
                                if (field_.type == i32) {
                                    @field(instance, field_name) = @unionInit(field_type, field_.name, field_value);
                                    is_set = true;
                                }
                            }
                            if (!is_set) {
                                const field_value_i64 = try bson_element.getValueAs(i64);
                                inline for (union_field_fields) |field_| {
                                    if (field_.type == i64) {
                                        @field(instance, field_name) = @unionInit(field_type, field_.name, field_value_i64);
                                        is_set = true;
                                    }
                                }
                            }
                            if (!is_set) {
                                const field_value_f64 = try bson_element.getValueAs(f64);
                                inline for (union_field_fields) |field_| {
                                    if (field_.type == f64) {
                                        @field(instance, field_name) = @unionInit(field_type, field_.name, field_value_f64);
                                        is_set = true;
                                    }
                                }
                            }
                        },
                        .int64 => {
                            var is_set = false;
                            const field_value = try bson_element.getValueAs(i64);
                            inline for (union_field_fields) |field_| {
                                if (field_.type == i64) {
                                    @field(instance, field_name) = @unionInit(field_type, field_.name, field_value);
                                    is_set = true;
                                }
                            }
                            if (!is_set) {
                                const field_value_f64 = try bson_element.getValueAs(f64);
                                inline for (union_field_fields) |field_| {
                                    if (field_.type == f64) {
                                        @field(instance, field_name) = @unionInit(field_type, field_.name, field_value_f64);
                                        is_set = true;
                                    }
                                }
                            }
                        },
                        .double => {
                            const field_value = try bson_element.getValueAs(f64);
                            inline for (union_field_fields) |field_| {
                                if (field_.type == f64) {
                                    @field(instance, field_name) = @unionInit(field_type, field_.name, field_value);
                                }
                            }
                        },
                        .decimal128 => {
                            const field_value = try bson_element.getValueAs(BsonDecimal128);

                            inline for (union_field_fields) |field_| {
                                if (field_.type == BsonDecimal128) {
                                    @field(instance, field_name) = @unionInit(field_type, field_.name, field_value);
                                }
                            }
                        },
                        .object_id => {
                            const field_value = try bson_element.getValueAs(bson_types.BsonObjectId);

                            inline for (union_field_fields) |field_| {
                                if (field_.type == bson_types.BsonObjectId) {
                                    @field(instance, field_name) = @unionInit(field_type, field_.name, field_value);
                                }
                            }
                        },
                        .timestamp => {
                            const field_value = try bson_element.getValueAs(bson_types.BsonTimestamp);

                            inline for (union_field_fields) |field_| {
                                if (field_.type == bson_types.BsonTimestamp) {
                                    @field(instance, field_name) = @unionInit(field_type, field_.name, field_value);
                                }
                            }
                        },
                        .utc_date_time => {
                            const field_value = try bson_element.getValueAs(bson_types.BsonUtcDatetime);

                            inline for (union_field_fields) |field_| {
                                if (field_.type == bson_types.BsonUtcDatetime) {
                                    @field(instance, field_name) = @unionInit(field_type, field_.name, field_value);
                                }
                            }
                        },
                        .binary => {
                            const field_value = try bson_element.getValueAs(bson_types.BsonBinary);

                            inline for (union_field_fields) |field_| {
                                if (field_.type == bson_types.BsonBinary) {
                                    @field(instance, field_name) = @unionInit(field_type, field_.name, field_value);
                                }
                            }
                        },
                        .null => {
                            inline for (union_field_fields) |field_| {
                                if (field_.type == void) {
                                    @field(instance, field_name) = @unionInit(field_type, field_.name, void{});
                                }
                            }
                        },

                        else => {
                            return error.UnionFieldTypeNotSupported;
                        },
                    }
                },

                .@"struct" => {
                    switch (bson_element.type) {
                        .object_id, .timestamp, .utc_date_time => {
                            const field_value = try bson_element.getValueAs(field_type);

                            @field(instance, field_name) = field_value;
                        },
                        .array => {
                            return error.ArrayTypeNotSupported;
                        },
                        .null => {
                            if (is_optional) {
                                @field(instance, field_name) = null;
                            } else {
                                return error.NullValueForNonOptionalField;
                            }
                        },
                        else => {
                            const field_value_document = try bson_element.getAsDocumentElement(arena_allocator);

                            defer field_value_document.deinit(arena_allocator);
                            const field_obj_value = try field_value_document.toObject(allocator, field_type, options);
                            defer allocator.destroy(field_obj_value); // destroy only the address

                            @field(instance, field_name) = field_obj_value.*;
                        },
                    }
                },

                .pointer => {
                    switch (field_type_info.pointer.size) {
                        .one => {
                            if (field_type_info.pointer.child == BsonDocument) {
                                const field_value_document = try bson_element.getAsDocumentElement(allocator);
                                errdefer field_value_document.deinit(allocator);
                                @field(instance, field_name) = field_value_document;
                            } else {
                                const field_value_document = try bson_element.getAsDocumentElement(arena_allocator);
                                defer field_value_document.deinit(arena_allocator);
                                const field_obj_value = try field_value_document.toObject(allocator, field_type_info.pointer.child, options);
                                errdefer allocator.destroy(field_obj_value); // destroy only the address
                                @field(instance, field_name) = field_obj_value;
                            }
                        },
                        .slice => {
                            if (field_type_info.pointer.child == u8) {
                                const field_value = try bson_element.getValueAs(field_type);

                                const field_value_copy = if (comptime field_type_info.pointer.sentinel_ptr == null)
                                    try allocator.dupe(u8, field_value)
                                else
                                    try allocator.dupeZ(u8, field_value);
                                errdefer allocator.free(field_value_copy);
                                @field(instance, field_name) = field_value_copy;
                            } else {
                                const field_value = try bson_element.getValueAsArrayOf(allocator, field_type_info.pointer.child);
                                @field(instance, field_name) = field_value;
                            }
                        },
                        .many => {
                            return error.ManyPointerTypeNotSupported;
                        },
                        .c => {
                            return error.PointerTypeNotSupported;
                        },
                    }
                },
                else => {
                    return error.FieldTypeNotSupported;
                },
            }
        } else {
            if (options.ignore_unknown_fields) {
                if (is_optional) {
                    @field(instance, field_name) = null;
                } else {
                    return error.MissingValueForNonOptionalField;
                }
            } else {
                return error.FieldNotFound;
            }
        }
    }
}

test "parseBsonToObject with i32" {
    const allocator = std.testing.allocator;
    const T = struct {
        a: i32,
    };

    const doc = try BsonDocument.fromJsonString(allocator, "{\"a\": 1}");
    defer doc.deinit(allocator);
    const instance = try doc.toObject(allocator, T, .{});
    defer allocator.destroy(instance);

    try std.testing.expectEqual(1, instance.a);
}

test "parseBsonToObject with string and i32" {
    const allocator = std.testing.allocator;
    const T = struct {
        a: i32,
        b: []const u8,
    };

    const doc = try BsonDocument.fromJsonString(allocator, "{\"a\": 1, \"b\": \"hello\"}");
    defer doc.deinit(allocator);
    const instance = try doc.toObject(allocator, T, .{});
    defer allocator.free(instance.b);
    defer allocator.destroy(instance);

    try std.testing.expectEqual(1, instance.a);
    try std.testing.expectEqualStrings("hello", instance.b);
}

test "parseBsonToObject with string and string" {
    const allocator = std.testing.allocator;
    const T = struct {
        a: []const u8,
        b: []const u8,
    };

    const doc = try BsonDocument.fromJsonString(allocator, "{\"a\": \"hello\", \"b\": \"world\"}");
    defer doc.deinit(allocator);
    const instance = try doc.toObject(allocator, T, .{});
    defer allocator.free(instance.a);
    defer allocator.free(instance.b);
    defer allocator.destroy(instance);

    try std.testing.expectEqualStrings("hello", instance.a);
    try std.testing.expectEqualStrings("world", instance.b);
}

test "parseBsonToObject with int array" {
    const allocator = std.testing.allocator;
    const T = struct {
        a: []const i32,
    };

    const doc = try BsonDocument.fromJsonString(allocator, "{\"a\": [1, 2, 3]}");
    defer doc.deinit(allocator);
    const instance = try doc.toObject(allocator, T, .{});
    defer allocator.free(instance.a);
    defer allocator.destroy(instance);

    try std.testing.expectEqual(@as(i32, 1), instance.a[0]);
    try std.testing.expectEqual(@as(i32, 2), instance.a[1]);
    try std.testing.expectEqual(@as(i32, 3), instance.a[2]);
}

test "parseBsonToObject with string" {
    const allocator = std.testing.allocator;
    const T = struct {
        const Self = @This();
        b: []const u8,

        fn deinit(self: *const Self, _allocator: Allocator) void {
            _allocator.free(self.b);
            _allocator.destroy(self);
        }
    };

    const doc = try BsonDocument.fromJsonString(allocator, "{\"b\": \"hello\"}");
    defer doc.deinit(allocator);
    const instance = try doc.toObject(allocator, T, .{ .ignore_unknown_fields = true });
    defer instance.deinit(allocator);

    try std.testing.expectEqualStrings("hello", instance.b);
}

test "parseBsonToObject with stringZ" {
    const allocator = std.testing.allocator;
    const T = struct {
        const Self = @This();
        b: [:0]const u8,
        fn deinit(self: *const Self, _allocator: Allocator) void {
            _allocator.free(self.b);
            _allocator.destroy(self);
        }
    };

    const doc = try BsonDocument.fromJsonString(allocator, "{\"b\": \"hello\"}");
    defer doc.deinit(allocator);
    const instance = try doc.toObject(allocator, T, .{ .ignore_unknown_fields = true });
    defer instance.deinit(allocator);

    try std.testing.expectEqualStrings("hello", instance.b);
}

test "parseBsonToObject with sub document" {
    const allocator = std.testing.allocator;

    const SubT = struct {
        const Self = @This();
        bbbb: []const u8,

        fn deinit(self: *const Self, _allocator: Allocator) void {
            _allocator.free(self.bbbb);
        }
    };

    const T = struct {
        const Self = @This();
        aaaa: SubT,

        fn deinit(self: *const Self, _allocator: Allocator) void {
            self.aaaa.deinit(_allocator);
            _allocator.destroy(self);
        }
    };

    const doc = try BsonDocument.fromJsonString(allocator, "{\"aaaa\": {\"bbbb\": \"cccc\"}}");
    defer doc.deinit(allocator);
    const instance = try doc.toObject(allocator, T, .{});
    defer instance.deinit(allocator);

    try std.testing.expectEqualStrings("cccc", instance.aaaa.bbbb);
}

test "parseBsonToObject with objectId" {
    const allocator = std.testing.allocator;

    const T = struct {
        const Self = @This();
        processId: bson.bson_types.BsonObjectId,
        counter: i64,

        fn deinit(self: *const Self, _allocator: Allocator) void {
            _allocator.destroy(self);
        }
    };

    const doc = try BsonDocument.fromJsonString(allocator, "{\"processId\":{\"$oid\":\"688e29cb4fdb65e172b4d104\"},\"counter\":0}");
    defer doc.deinit(allocator);
    const instance = try doc.toObject(allocator, T, .{});
    defer instance.deinit(allocator);

    const object_id = instance.processId;
    try std.testing.expectEqualStrings("688e29cb4fdb65e172b4d104", &object_id.toString(.lower));
}

test "parseBsonToObject with sub document and objectId" {
    const allocator = std.testing.allocator;

    const TopologyVersion = struct {
        const Self = @This();
        processId: bson.bson_types.BsonObjectId,
        counter: i64,
    };

    const Topology = struct {
        const Self = @This();
        aaaaaa: TopologyVersion,

        fn deinit(self: *const Self, _allocator: Allocator) void {
            _allocator.destroy(self);
        }
    };

    const doc = try BsonDocument.fromObject(allocator, Topology, .{
        .aaaaaa = .{
            .processId = bson.bson_types.BsonObjectId.fromString("688e29cb4fdb65e172b40000") catch unreachable,
            .counter = 0,
        },
    });
    defer doc.deinit(allocator);
    const instance = try doc.toObject(allocator, Topology, .{});
    defer instance.deinit(allocator);

    const object_id = instance.aaaaaa.processId;
    try std.testing.expectEqualStrings("688e29cb4fdb65e172b40000", &object_id.toString(.lower));
}

test "parseBsonToObject with all optional fields present" {
    const allocator = std.testing.allocator;
    const T = struct {
        required: i32,
        optional: ?[]const u8,
        another_optional: ?i64,
    };

    const doc = try BsonDocument.fromJsonString(allocator,
        \\{"required": 42, "optional": "hello", "another_optional": 123}
    );
    defer doc.deinit(allocator);
    const instance = try doc.toObject(allocator, T, .{});
    defer allocator.destroy(instance);
    defer if (instance.optional) |str| allocator.free(str);

    try std.testing.expectEqual(@as(i32, 42), instance.required);
    try std.testing.expectEqualStrings("hello", instance.optional.?);
    try std.testing.expectEqual(@as(i64, 123), instance.another_optional.?);
}

test "parseBsonToObject with optional fields missing" {
    const allocator = std.testing.allocator;
    const T = struct {
        required: i32,
        optional: ?[]const u8,
        another_optional: ?i64,
    };

    const doc = try BsonDocument.fromJsonString(allocator,
        \\{"required": 42}
    );
    defer doc.deinit(allocator);
    const instance = try doc.toObject(allocator, T, .{ .ignore_unknown_fields = true });
    defer allocator.destroy(instance);

    try std.testing.expectEqual(@as(i32, 42), instance.required);
    try std.testing.expectEqual(@as(?[]const u8, null), instance.optional);
    try std.testing.expectEqual(@as(?i64, null), instance.another_optional);
}

test "parseBsonToObject with nested structs" {
    const allocator = std.testing.allocator;
    const Inner = struct {
        const Self = @This();
        value: i32,
        name: []const u8,
        fn deinit(self: *const Self, _allocator: Allocator) void {
            _allocator.free(self.name);
        }
    };
    const Outer = struct {
        const Self = @This();
        inner: Inner,
        flag: bool,
        fn deinit(self: *const Self, _allocator: Allocator) void {
            self.inner.deinit(_allocator);
            _allocator.destroy(self);
        }
    };

    const doc = try BsonDocument.fromJsonString(allocator,
        \\{"inner": {"value": 123, "name": "test"}, "flag": true}
    );
    defer doc.deinit(allocator);
    const instance = try doc.toObject(allocator, Outer, .{});
    defer instance.deinit(allocator);

    try std.testing.expectEqual(@as(i32, 123), instance.inner.value);
    try std.testing.expectEqualStrings("test", instance.inner.name);
    try std.testing.expectEqual(true, instance.flag);
}

test "parseBsonToObject error on missing required field" {
    const allocator = std.testing.allocator;
    const T = struct {
        required: i32,
    };

    const doc = try BsonDocument.fromJsonString(allocator, "{}");
    defer doc.deinit(allocator);
    const instance = try allocator.create(T);
    defer allocator.destroy(instance);

    try std.testing.expectError(error.MissingValueForNonOptionalField, parseBsonToObject(allocator, T, instance, doc, .{ .ignore_unknown_fields = true }));
}

test "parseBsonToObject error on field not found" {
    const allocator = std.testing.allocator;
    const T = struct {
        required: i32,
    };

    const doc = try BsonDocument.fromJsonString(allocator, "{}");
    defer doc.deinit(allocator);
    const instance = try allocator.create(T);
    defer allocator.destroy(instance);

    try std.testing.expectError(error.FieldNotFound, parseBsonToObject(allocator, T, instance, doc, .{ .ignore_unknown_fields = false }));
}

test "parseBsonToObject with array field" {
    const allocator = std.testing.allocator;

    const T = struct {
        const Self = @This();
        values: []const i32,

        fn deinit(self: *const Self, _allocator: Allocator) void {
            _allocator.free(self.values);
            _allocator.destroy(self);
        }
    };

    const doc = try BsonDocument.fromJsonString(allocator, "{\"values\": [1, 2, 3, 4, 5]}");
    defer doc.deinit(allocator);

    const instance = try doc.toObject(allocator, T, .{});
    defer instance.deinit(allocator);

    try std.testing.expectEqual(@as(usize, 5), instance.values.len);
    try std.testing.expectEqual(@as(i32, 1), instance.values[0]);
    try std.testing.expectEqual(@as(i32, 2), instance.values[1]);
    try std.testing.expectEqual(@as(i32, 3), instance.values[2]);
    try std.testing.expectEqual(@as(i32, 4), instance.values[3]);
    try std.testing.expectEqual(@as(i32, 5), instance.values[4]);
}
