const std = @import("std");
const builtin = @import("builtin");
const datetime = @import("datetime.zig");
const utils = @import("utils.zig");
const colors = @import("colors.zig");
const bson_types = @import("bson-types.zig");
const bson = @import("bson.zig");
const BsonDocument = bson.BsonDocument;
const BsonWriter = @import("bson-writer.zig");
const ExtJsonSerializer = @import("bson-ext-json-serializer.zig");
const ExtJsonParser = @import("bson-ext-json-parser.zig");

const testing = std.testing;
const Allocator = std.mem.Allocator;
const native_endian = builtin.cpu.arch.endian();
const debug = std.debug;

const ElementType = bson_types.BsonElementType;
const BsonSubType = bson_types.BsonSubType;
const BsonObjectIdError = bson_types.BsonObjectIdError;
const BsonBinary = bson_types.BsonBinary;
const BsonObjectId = bson_types.BsonObjectId;
const BsonUtcDatetime = bson_types.BsonUtcDatetime;
const BsonTimestamp = bson_types.BsonTimestamp;
const BsonRegexpOptions = bson_types.RegexpOptions;

const JsonParsingRegExpError = ExtJsonParser.JsonParsingRegExpError;
const WriteJsonStringError = ExtJsonSerializer.WriteJsonStringError;

test "hello world example from bson spec" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const arena_allocator = arena.allocator();

    const HelloWorldStruct = struct {
        hello: [:0]const u8,
    };

    const person = HelloWorldStruct{
        .hello = "world",
    };

    const bson_document = try BsonWriter.writeToBson(HelloWorldStruct, person, arena_allocator);
    defer arena_allocator.destroy(bson_document);

    const expected_data =
        // document length (int 32)
        [_]u8{ @as(u8, 22), 0, 0, 0 } ++
        // field type (int 8)
        [_]u8{@as(u8, @intFromEnum(ElementType.string))} ++
        // field name (null-terminated string)
        "hello".* ++ [_]u8{0} ++
        // string length (int 32)
        [_]u8{ @as(u8, @intCast("world".len + 1)), 0, 0, 0 } ++
        // string value
        "world".* ++
        // string null terminator (int 8)
        [_]u8{0} ++
        // document null terminator (int 8)
        [_]u8{0};
    try testing.expectEqualSlices(u8, &expected_data, bson_document.raw_data);

    const json_string = try ExtJsonSerializer.toJsonString(bson_document, arena_allocator, false);
    defer arena_allocator.free(json_string);

    try testing.expectEqualSlices(u8, "{\"hello\":\"world\"}", json_string);
}

test "hello world to bson with int32" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const arena_allocator = arena.allocator();

    const HelloWorldStruct = struct {
        num: i32,
    };

    const person = HelloWorldStruct{
        .num = 42,
    };

    const bson_document = try BsonWriter.writeToBson(HelloWorldStruct, person, arena_allocator);
    defer arena_allocator.destroy(bson_document);

    const expected_data =
        // document length (int 32)
        [_]u8{ @as(u8, 14), 0, 0, 0 } ++
        // field type (int 8)
        [_]u8{@as(u8, @intFromEnum(ElementType.int32))} ++
        // field name (null-terminated string)
        "num".* ++ [_]u8{0} ++
        // field value
        std.mem.toBytes(@as(i32, @intCast(42))) ++
        // document null terminator (int 8)
        [_]u8{0};

    try testing.expectEqualSlices(u8, &expected_data, bson_document.raw_data);

    const json_string = try ExtJsonSerializer.toJsonString(bson_document, arena_allocator, false);
    defer arena_allocator.free(json_string);

    try testing.expectEqualSlices(u8, "{\"num\":42}", json_string);
}

test "write bson with int16 (coerced to int32)" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const arena_allocator = arena.allocator();

    const HelloWorldStruct = struct {
        num: i16,
    };

    const person = HelloWorldStruct{
        .num = 42,
    };

    const bson_document = try BsonWriter.writeToBson(HelloWorldStruct, person, arena_allocator);
    defer arena_allocator.destroy(bson_document);

    const expected_data =
        // document length (int 32)
        [_]u8{ @as(u8, 14), 0, 0, 0 } ++
        // field type (int 8)
        [_]u8{@as(u8, @intFromEnum(ElementType.int32))} ++
        // field name (null-terminated string)
        "num".* ++ [_]u8{0} ++
        // field value
        std.mem.toBytes(@as(i32, @intCast(42))) ++
        // document null terminator (int 8)
        [_]u8{0};

    try testing.expectEqualSlices(u8, &expected_data, bson_document.raw_data);

    const json_string = try ExtJsonSerializer.toJsonString(bson_document, arena_allocator, false);
    defer arena_allocator.free(json_string);

    try testing.expectEqualSlices(u8, "{\"num\":42}", json_string);
}

test "write bson with int64" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const arena_allocator = arena.allocator();

    const HelloWorldStruct = struct {
        num: i64,
    };

    const person = HelloWorldStruct{
        .num = 42,
    };

    const bson_document = try BsonWriter.writeToBson(HelloWorldStruct, person, arena_allocator);
    defer arena_allocator.destroy(bson_document);

    const expected_data =
        // document length (int 32)
        [_]u8{ @as(u8, 18), 0, 0, 0 } ++
        // field type (int 8)
        [_]u8{@as(u8, @intFromEnum(ElementType.int64))} ++
        // field name (null-terminated string)
        "num".* ++ [_]u8{0} ++
        // field value
        std.mem.toBytes(@as(i64, @intCast(42))) ++
        // document null terminator (int 8)
        [_]u8{0};

    try testing.expectEqualSlices(u8, &expected_data, bson_document.raw_data);

    const json_string = try ExtJsonSerializer.toJsonString(bson_document, arena_allocator, false);
    defer arena_allocator.free(json_string);

    try testing.expectEqualSlices(u8, "{\"num\":42}", json_string);
}

test "write bson with sub document" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const arena_allocator = arena.allocator();

    const ChildDocument = struct {
        name: [:0]const u8,
    };

    const ParentDocument = struct {
        sub_document: ChildDocument,
    };

    const parent_document = ParentDocument{
        .sub_document = ChildDocument{
            .name = "child",
        },
    };

    const bson_document = try BsonWriter.writeToBson(ParentDocument, parent_document, arena_allocator);
    defer arena_allocator.destroy(bson_document);

    const sub_document_data =
        // document length (int 32)
        [_]u8{ @as(u8, 21), 0, 0, 0 } ++
        // field type (int 8)
        [_]u8{@as(u8, @intFromEnum(ElementType.string))} ++
        // field name (null-terminated string)
        "name".* ++ [_]u8{0} ++
        // string length (int 32)
        [_]u8{ @as(u8, @intCast("child".len + 1)), 0, 0, 0 } ++
        // string value
        "child".* ++
        // string null terminator (int 8)
        [_]u8{0} ++
        // document null terminator (int 8)
        [_]u8{0};
    const expected_data =
        // document length (int 32)
        [_]u8{ @as(u8, 40), 0, 0, 0 } ++
        // field type (int 8)
        [_]u8{@as(u8, @intFromEnum(ElementType.document))} ++
        // field name (null-terminated string)
        "sub_document".* ++ [_]u8{0} ++
        // field type (int 8)
        sub_document_data ++
        // parent document null terminator (int 8)
        [_]u8{0};

    try testing.expectEqualSlices(u8, &expected_data, bson_document.raw_data);

    const json_string = try ExtJsonSerializer.toJsonString(bson_document, arena_allocator, false);
    defer arena_allocator.free(json_string);

    try testing.expectEqualSlices(u8, "{\"sub_document\":{\"name\":\"child\"}}", json_string);
}

// test "write bson with decimal128" {
//     var arena = std.heap.ArenaAllocator.init(testing.allocator);
//     defer arena.deinit();
//     const arena_allocator = arena.allocator();

//     const BsonWithDecimal128 = struct {
//         num: f128,
//     };

//     const doc = BsonWithDecimal128{
//         .num = 42,
//     };

//     const bson_document = try BsonWriter.writeToBson(BsonWithDecimal128, doc, arena_allocator);
//     defer arena_allocator.destroy(bson_document);

//     const expected_data =
//         // document length (int 32)
//         [_]u8{ @as(u8, 26), 0, 0, 0 } ++
//         // field type (int 8)
//         [_]u8{@as(u8, @intFromEnum(ElementType.decimal128))} ++
//         // field name (null-terminated string)
//         "num".* ++ [_]u8{0} ++
//         // field value
//         std.mem.toBytes(@as(f128, @floatCast(42))) ++
//         // document null terminator (int 8)
//         [_]u8{0};

//     try testing.expectEqualSlices(u8, &expected_data, bson_document.raw_data);

//     const json_string = try ExtJsonSerializer.toJsonString(bson_document, arena_allocator, false);
//     defer arena_allocator.free(json_string);

//     try testing.expectEqualSlices(u8, "{\"num\":{\"$numberDecimal\":\"42\"}}", json_string);
// }

test "write bson with null value" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const arena_allocator = arena.allocator();

    const BsonWithNull = struct {
        num: ?i32,
    };

    const doc = BsonWithNull{
        .num = null,
    };

    const bson_document = try BsonWriter.writeToBson(BsonWithNull, doc, arena_allocator);
    defer arena_allocator.destroy(bson_document);

    const expected_data =
        // document length (int 32)
        [_]u8{ @as(u8, 10), 0, 0, 0 } ++
        // field type (int 8)
        [_]u8{@as(u8, @intFromEnum(ElementType.null))} ++
        // field name (null-terminated string)
        "num".* ++ [_]u8{0} ++
        // document null terminator (int 8)
        [_]u8{0};

    try testing.expectEqualSlices(u8, &expected_data, bson_document.raw_data);

    const json_string = try ExtJsonSerializer.toJsonString(bson_document, arena_allocator, false);
    defer arena_allocator.free(json_string);

    try testing.expectEqualSlices(u8, "{\"num\":null}", json_string);
}

test "write bson with binary" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const arena_allocator = arena.allocator();

    const BinaryDocument = struct {
        binary_data: BsonBinary,
    };

    const value = [_]u8{ 'h', 'e', 'l', 'l', 'o' };
    const doc = BinaryDocument{
        .binary_data = BsonBinary.fromBytes(u8, &value, BsonSubType.generic),
    };

    const bson_document = try BsonWriter.writeToBson(BinaryDocument, doc, arena_allocator);
    defer arena_allocator.destroy(bson_document);

    const expected_data =
        // document length (int 32)
        [_]u8{ @as(u8, 28), 0, 0, 0 } ++
        // field type (int 8)
        [_]u8{@as(u8, @intFromEnum(ElementType.binary))} ++
        // field name (null-terminated string)
        "binary_data".* ++ [_]u8{0} ++
        // binary length (int 32)
        std.mem.toBytes(@as(i32, @intCast("hello".len))) ++
        // binary sub type (int 8)
        [_]u8{@intFromEnum(BsonSubType.generic)} ++
        // binary data
        std.mem.toBytes(value) ++
        // document null terminator (int 8)
        [_]u8{0};

    try testing.expectEqualSlices(u8, &expected_data, bson_document.raw_data);
}

test "write bson with boolean" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const arena_allocator = arena.allocator();

    const BooleanDocument = struct {
        boolean_data: bool,
    };

    const doc = BooleanDocument{
        .boolean_data = true,
    };

    const bson_document = try BsonWriter.writeToBson(BooleanDocument, doc, arena_allocator);
    defer arena_allocator.destroy(bson_document);

    const expected_data =
        // document length (int 32)
        [_]u8{ @as(u8, 20), 0, 0, 0 } ++
        // field type (int 8)
        [_]u8{@as(u8, @intFromEnum(ElementType.boolean))} ++
        // field name (null-terminated string)
        "boolean_data".* ++ [_]u8{0} ++
        // field value
        [_]u8{if (doc.boolean_data) 1 else 0} ++
        // document null terminator (int 8)
        [_]u8{0};

    try testing.expectEqualSlices(u8, &expected_data, bson_document.raw_data);
}

test "write bson to json with int32 value in range - /bson-corpus/tests/int32.json" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const arena_allocator = arena.allocator();

    const DocumentStruct = struct {
        i: i32,
    };

    const doc = DocumentStruct{
        .i = 1,
    };

    const bson_document = try BsonWriter.writeToBson(DocumentStruct, doc, arena_allocator);
    defer arena_allocator.destroy(bson_document);

    const expected_data =
        // document length (int 32)
        [_]u8{ @as(u8, 12), 0, 0, 0 } ++
        // field type (int 8)
        [_]u8{@as(u8, @intFromEnum(ElementType.int32))} ++
        // field name (null-terminated string)
        "i".* ++ [_]u8{0} ++
        // field value
        std.mem.toBytes(@as(i32, @intCast(1))) ++
        // document null terminator (int 8)
        [_]u8{0};

    try testing.expectEqualSlices(u8, &expected_data, bson_document.raw_data);

    const json_string = try ExtJsonSerializer.toJsonString(bson_document, arena_allocator, false);
    defer arena_allocator.free(json_string);

    try testing.expectEqualSlices(u8, "{\"i\":1}", json_string);

    const extjson_string = try ExtJsonSerializer.toJsonString(bson_document, arena_allocator, true);
    defer arena_allocator.free(extjson_string);

    try testing.expectEqualSlices(u8, "{\"i\":{\"$numberInt\":\"1\"}}", extjson_string);
}

test "write bson to json with int32 min value - /bson-corpus/tests/int32.json" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const arena_allocator = arena.allocator();

    const DocumentStruct = struct {
        i: i32,
    };

    const doc = DocumentStruct{
        .i = -2147483648,
    };

    const bson_document = try BsonWriter.writeToBson(DocumentStruct, doc, arena_allocator);
    defer arena_allocator.destroy(bson_document);

    const expected_data =
        // document length (int 32)
        [_]u8{ @as(u8, 12), 0, 0, 0 } ++
        // field type (int 8)
        [_]u8{@as(u8, @intFromEnum(ElementType.int32))} ++
        // field name (null-terminated string)
        "i".* ++ [_]u8{0} ++
        // field value
        std.mem.toBytes(@as(i32, @intCast(-2147483648))) ++
        // document null terminator (int 8)
        [_]u8{0};

    try testing.expectEqualSlices(u8, &expected_data, bson_document.raw_data);

    const json_string = try ExtJsonSerializer.toJsonString(bson_document, arena_allocator, false);
    defer arena_allocator.free(json_string);
    try testing.expectEqualSlices(u8, "{\"i\":-2147483648}", json_string);

    const bson_document_from_json = try ExtJsonParser.jsonStringToBson(json_string, arena_allocator);
    defer arena_allocator.destroy(bson_document_from_json);
    try testing.expectEqualSlices(u8, &expected_data, bson_document_from_json.raw_data);

    const extjson_string = try ExtJsonSerializer.toJsonString(bson_document, arena_allocator, true);
    defer arena_allocator.free(extjson_string);
    try testing.expectEqualSlices(u8, "{\"i\":{\"$numberInt\":\"-2147483648\"}}", extjson_string);

    const bson_document_from_extjson = try ExtJsonParser.jsonStringToBson(extjson_string, arena_allocator);
    defer arena_allocator.destroy(bson_document_from_extjson);
    try testing.expectEqualSlices(u8, &expected_data, bson_document_from_extjson.raw_data);
}

test "write bson to json with int32 max value - /bson-corpus/tests/int32.json" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const arena_allocator = arena.allocator();

    const DocumentStruct = struct {
        i: i32,
    };

    const doc = DocumentStruct{
        .i = 2147483647,
    };

    const bson_document = try BsonWriter.writeToBson(DocumentStruct, doc, arena_allocator);
    defer arena_allocator.destroy(bson_document);

    const expected_data =
        // document length (int 32)
        [_]u8{ @as(u8, 12), 0, 0, 0 } ++
        // field type (int 8)
        [_]u8{@as(u8, @intFromEnum(ElementType.int32))} ++
        // field name (null-terminated string)
        "i".* ++ [_]u8{0} ++
        // field value
        std.mem.toBytes(@as(i32, @intCast(2147483647))) ++
        // document null terminator (int 8)
        [_]u8{0};

    try testing.expectEqualSlices(u8, &expected_data, bson_document.raw_data);

    const json_string = try ExtJsonSerializer.toJsonString(bson_document, arena_allocator, false);
    defer arena_allocator.free(json_string);

    try testing.expectEqualSlices(u8, "{\"i\":2147483647}", json_string);

    const extjson_string = try ExtJsonSerializer.toJsonString(bson_document, arena_allocator, true);
    defer arena_allocator.free(extjson_string);

    try testing.expectEqualSlices(u8, "{\"i\":{\"$numberInt\":\"2147483647\"}}", extjson_string);
}

test "write bson to json with int64 min value - /bson-corpus/tests/int64.json" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const arena_allocator = arena.allocator();

    const DocumentStruct = struct {
        i: i64,
    };

    const doc = DocumentStruct{
        .i = -9223372036854775808,
    };

    const bson_document = try BsonWriter.writeToBson(DocumentStruct, doc, arena_allocator);
    defer arena_allocator.destroy(bson_document);

    const expected_data =
        // document length (int 32)
        [_]u8{ @as(u8, 16), 0, 0, 0 } ++
        // field type (int 8)
        [_]u8{@as(u8, @intFromEnum(ElementType.int64))} ++
        // field name (null-terminated string)
        "i".* ++ [_]u8{0} ++
        // field value
        std.mem.toBytes(@as(i64, @intCast(-9223372036854775808))) ++
        // document null terminator (int 8)
        [_]u8{0};

    try testing.expectEqualSlices(u8, &expected_data, bson_document.raw_data);

    const json_string = try ExtJsonSerializer.toJsonString(bson_document, arena_allocator, false);
    defer arena_allocator.free(json_string);

    try testing.expectEqualSlices(u8, "{\"i\":-9223372036854775808}", json_string);

    const extjson_string = try ExtJsonSerializer.toJsonString(bson_document, arena_allocator, true);
    defer arena_allocator.free(extjson_string);

    try testing.expectEqualSlices(u8, "{\"i\":{\"$numberLong\":\"-9223372036854775808\"}}", extjson_string);
}

test "write bson to json with int64 max value - /bson-corpus/tests/int64.json" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const arena_allocator = arena.allocator();

    const DocumentStruct = struct {
        i: i64,
    };

    const doc = DocumentStruct{
        .i = 9223372036854775807,
    };

    const bson_document = try BsonWriter.writeToBson(DocumentStruct, doc, arena_allocator);
    defer arena_allocator.destroy(bson_document);

    const expected_data =
        // document length (int 32)
        [_]u8{ @as(u8, 16), 0, 0, 0 } ++
        // field type (int 8)
        [_]u8{@as(u8, @intFromEnum(ElementType.int64))} ++
        // field name (null-terminated string)
        "i".* ++ [_]u8{0} ++
        // field value
        std.mem.toBytes(@as(i64, @intCast(9223372036854775807))) ++
        // document null terminator (int 8)
        [_]u8{0};

    try testing.expectEqualSlices(u8, &expected_data, bson_document.raw_data);

    const json_string = try ExtJsonSerializer.toJsonString(bson_document, arena_allocator, false);
    defer arena_allocator.free(json_string);

    try testing.expectEqualSlices(u8, "{\"i\":9223372036854775807}", json_string);

    const extjson_string = try ExtJsonSerializer.toJsonString(bson_document, arena_allocator, true);
    defer arena_allocator.free(extjson_string);

    try testing.expectEqualSlices(u8, "{\"i\":{\"$numberLong\":\"9223372036854775807\"}}", extjson_string);
}

test "write bson to json with int64 value: 0 - /bson-corpus/tests/int64.json" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const arena_allocator = arena.allocator();

    const DocumentStruct = struct {
        i: i64,
    };

    const doc = DocumentStruct{
        .i = 0,
    };

    const bson_document = try BsonWriter.writeToBson(DocumentStruct, doc, arena_allocator);
    defer arena_allocator.destroy(bson_document);

    const expected_data =
        // document length (int 32)
        [_]u8{ @as(u8, 16), 0, 0, 0 } ++
        // field type (int 8)
        [_]u8{@as(u8, @intFromEnum(ElementType.int64))} ++
        // field name (null-terminated string)
        "i".* ++ [_]u8{0} ++
        // field value
        std.mem.toBytes(@as(i64, @intCast(0))) ++
        // document null terminator (int 8)
        [_]u8{0};

    try testing.expectEqualSlices(u8, &expected_data, bson_document.raw_data);

    const json_string = try ExtJsonSerializer.toJsonString(bson_document, arena_allocator, false);
    defer arena_allocator.free(json_string);

    try testing.expectEqualSlices(u8, "{\"i\":0}", json_string);

    const extjson_string = try ExtJsonSerializer.toJsonString(bson_document, arena_allocator, true);
    defer arena_allocator.free(extjson_string);

    try testing.expectEqualSlices(u8, "{\"i\":{\"$numberLong\":\"0\"}}", extjson_string);
}

test "write bson to json with int64 value: 1 - /bson-corpus/tests/int64.json" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const arena_allocator = arena.allocator();

    const DocumentStruct = struct {
        a: i64,
    };

    const doc = DocumentStruct{
        .a = 1,
    };

    const bson_document = try BsonWriter.writeToBson(DocumentStruct, doc, arena_allocator);
    defer arena_allocator.destroy(bson_document);

    const expected_data =
        // document length (int 32)
        [_]u8{ @as(u8, 16), 0, 0, 0 } ++
        // field type (int 8)
        [_]u8{@as(u8, @intFromEnum(ElementType.int64))} ++
        // field name (null-terminated string)
        "a".* ++ [_]u8{0} ++
        // field value
        std.mem.toBytes(@as(i64, @intCast(1))) ++
        // document null terminator (int 8)
        [_]u8{0};

    try testing.expectEqualSlices(u8, &expected_data, bson_document.raw_data);

    const json_string = try ExtJsonSerializer.toJsonString(bson_document, arena_allocator, false);
    defer arena_allocator.free(json_string);

    try testing.expectEqualSlices(u8, "{\"a\":1}", json_string);

    const extjson_string = try ExtJsonSerializer.toJsonString(bson_document, arena_allocator, true);
    defer arena_allocator.free(extjson_string);

    try testing.expectEqualSlices(u8, "{\"a\":{\"$numberLong\":\"1\"}}", extjson_string);
}

test "write bson to json with boolean value: true - /bson-corpus/tests/boolean.json" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const arena_allocator = arena.allocator();

    const DocumentStruct = struct {
        b: bool,
    };

    const doc = DocumentStruct{
        .b = true,
    };

    const bson_document = try BsonWriter.writeToBson(DocumentStruct, doc, arena_allocator);
    defer arena_allocator.destroy(bson_document);

    const expected_data =
        // document length (int 32)
        [_]u8{ @as(u8, 9), 0, 0, 0 } ++
        // field type (int 8)
        [_]u8{@as(u8, @intFromEnum(ElementType.boolean))} ++
        // field name (null-terminated string)
        "b".* ++ [_]u8{0} ++
        // field value
        [_]u8{if (doc.b) 1 else 0} ++
        // document null terminator (int 8)
        [_]u8{0};

    try testing.expectEqualSlices(u8, &expected_data, bson_document.raw_data);

    const json_string = try ExtJsonSerializer.toJsonString(bson_document, arena_allocator, false);
    defer arena_allocator.free(json_string);

    try testing.expectEqualSlices(u8, "{\"b\":true}", json_string);

    const extjson_string = try ExtJsonSerializer.toJsonString(bson_document, arena_allocator, true);
    defer arena_allocator.free(extjson_string);

    try testing.expectEqualSlices(u8, "{\"b\":true}", extjson_string);
}

test "write bson to json with boolean value: false - /bson-corpus/tests/boolean.json" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const arena_allocator = arena.allocator();

    const DocumentStruct = struct {
        b: bool,
    };

    const doc = DocumentStruct{
        .b = false,
    };

    const bson_document = try BsonWriter.writeToBson(DocumentStruct, doc, arena_allocator);
    defer arena_allocator.destroy(bson_document);

    const expected_data =
        // document length (int 32)
        [_]u8{ @as(u8, 9), 0, 0, 0 } ++
        // field type (int 8)
        [_]u8{@as(u8, @intFromEnum(ElementType.boolean))} ++
        // field name (null-terminated string)
        "b".* ++ [_]u8{0} ++
        // field value
        [_]u8{if (doc.b) 1 else 0} ++
        // document null terminator (int 8)
        [_]u8{0};

    try testing.expectEqualSlices(u8, &expected_data, bson_document.raw_data);

    const json_string = try ExtJsonSerializer.toJsonString(bson_document, arena_allocator, false);
    defer arena_allocator.free(json_string);

    try testing.expectEqualSlices(u8, "{\"b\":false}", json_string);

    const extjson_string = try ExtJsonSerializer.toJsonString(bson_document, arena_allocator, true);
    defer arena_allocator.free(extjson_string);

    try testing.expectEqualSlices(u8, "{\"b\":false}", extjson_string);
}

test "write bson to json with binary subtype 0x00 (Zero-length) - /bson-corpus/tests/binary.json" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const arena_allocator = arena.allocator();

    const DocumentStruct = struct {
        x: BsonBinary,
    };

    const doc = DocumentStruct{
        .x = BsonBinary.fromBytes(u8, &[0]u8{}, BsonSubType.generic),
    };

    const bson_document = try BsonWriter.writeToBson(DocumentStruct, doc, arena_allocator);
    defer arena_allocator.destroy(bson_document);

    const expected_data =
        // document length (int 32)
        [_]u8{ @as(u8, 13), 0, 0, 0 } ++
        // field type (int 8)
        [_]u8{@as(u8, @intFromEnum(ElementType.binary))} ++
        // field name (null-terminated string)
        "x".* ++ [_]u8{0} ++
        // binary length (int 32)
        std.mem.toBytes(@as(i32, 0)) ++
        // binary sub type (int 8)
        [_]u8{@intFromEnum(BsonSubType.generic)} ++
        // binary data
        // no data
        // document null terminator (int 8)
        [_]u8{0};

    try testing.expectEqualSlices(u8, &expected_data, bson_document.raw_data);

    try testing.expectError(WriteJsonStringError.BinaryFieldRequiresStrictExtJson, ExtJsonSerializer.toJsonString(bson_document, arena_allocator, false));

    const extjson_string = try ExtJsonSerializer.toJsonString(bson_document, arena_allocator, true);
    defer arena_allocator.free(extjson_string);

    try testing.expectEqualSlices(u8, "{\"x\":{\"$binary\":{\"base64\":\"\",\"subType\":\"00\"}}}", extjson_string);
}

test "write bson to json with binary subtype 0x00 - /bson-corpus/tests/binary.json" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const arena_allocator = arena.allocator();

    const DocumentStruct = struct {
        x: BsonBinary,
    };

    const doc = DocumentStruct{
        .x = BsonBinary.fromBytes(u8, &[_]u8{ 255, 255 }, BsonSubType.generic),
    };

    const bson_document = try BsonWriter.writeToBson(DocumentStruct, doc, arena_allocator);
    defer arena_allocator.destroy(bson_document);

    const expected_data =
        // document length (int 32)
        [_]u8{ @as(u8, 15), 0, 0, 0 } ++
        // field type (int 8)
        [_]u8{@as(u8, @intFromEnum(ElementType.binary))} ++
        // field name (null-terminated string)
        "x".* ++ [_]u8{0} ++
        // binary length (int 32)
        std.mem.toBytes(@as(i32, 2)) ++
        // binary sub type (int 8)
        [_]u8{@intFromEnum(BsonSubType.generic)} ++
        // binary data
        [_]u8{ 255, 255 } ++
        // document null terminator (int 8)
        [_]u8{0};

    try testing.expectEqualSlices(u8, &expected_data, bson_document.raw_data);

    try testing.expectError(ExtJsonSerializer.WriteJsonStringError.BinaryFieldRequiresStrictExtJson, ExtJsonSerializer.toJsonString(bson_document, arena_allocator, false));

    const extjson_string = try ExtJsonSerializer.toJsonString(bson_document, arena_allocator, true);
    defer arena_allocator.free(extjson_string);

    try testing.expectEqualSlices(u8, "{\"x\":{\"$binary\":{\"base64\":\"//8=\",\"subType\":\"00\"}}}", extjson_string);
}

test "write bson to json with binary subtype 0x01 - /bson-corpus/tests/binary.json" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const arena_allocator = arena.allocator();

    const DocumentStruct = struct {
        x: BsonBinary,
    };

    const doc = DocumentStruct{
        .x = BsonBinary.fromBytes(u8, &[_]u8{ 255, 255 }, BsonSubType.function),
    };

    const bson_document = try BsonWriter.writeToBson(DocumentStruct, doc, arena_allocator);
    defer arena_allocator.destroy(bson_document);

    const expected_data =
        // document length (int 32)
        [_]u8{ @as(u8, 15), 0, 0, 0 } ++
        // field type (int 8)
        [_]u8{@as(u8, @intFromEnum(ElementType.binary))} ++
        // field name (null-terminated string)
        "x".* ++ [_]u8{0} ++
        // binary length (int 32)
        std.mem.toBytes(@as(i32, 2)) ++
        // binary sub type (int 8)
        [_]u8{@intFromEnum(BsonSubType.function)} ++
        // binary data
        [_]u8{ 255, 255 } ++
        // document null terminator (int 8)
        [_]u8{0};

    try testing.expectEqualSlices(u8, &expected_data, bson_document.raw_data);

    try testing.expectError(ExtJsonSerializer.WriteJsonStringError.BinaryFieldRequiresStrictExtJson, ExtJsonSerializer.toJsonString(bson_document, arena_allocator, false));

    const extjson_string = try ExtJsonSerializer.toJsonString(bson_document, arena_allocator, true);
    defer arena_allocator.free(extjson_string);

    try testing.expectEqualSlices(u8, "{\"x\":{\"$binary\":{\"base64\":\"//8=\",\"subType\":\"01\"}}}", extjson_string);
}

test "write bson to json with binary subtype 0x02 - /bson-corpus/tests/binary.json" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const arena_allocator = arena.allocator();

    const DocumentStruct = struct {
        x: BsonBinary,
    };

    const doc = DocumentStruct{
        .x = BsonBinary.fromBytes(u8, &[_]u8{ 255, 255 }, BsonSubType.binary_old),
    };

    const bson_document = try BsonWriter.writeToBson(DocumentStruct, doc, arena_allocator);
    defer arena_allocator.destroy(bson_document);

    const expected_data =
        // document length (int 32)
        [_]u8{ @as(u8, 19), 0, 0, 0 } ++
        // field type (int 8)
        [_]u8{@as(u8, @intFromEnum(ElementType.binary))} ++
        // field name (null-terminated string)
        "x".* ++ [_]u8{0} ++
        // binary length (int 32)
        std.mem.toBytes(@as(i32, 2 + @sizeOf(i32))) ++
        // binary sub type (int 8)
        [_]u8{@intFromEnum(BsonSubType.binary_old)} ++
        // binary_old length (int 32)
        std.mem.toBytes(@as(i32, 2)) ++
        // binary_old data
        [_]u8{ 255, 255 } ++
        // document null terminator (int 8)
        [_]u8{0};

    try testing.expectEqualSlices(u8, &expected_data, bson_document.raw_data);

    try testing.expectError(WriteJsonStringError.BinaryFieldRequiresStrictExtJson, ExtJsonSerializer.toJsonString(bson_document, arena_allocator, false));

    const extjson_string = try ExtJsonSerializer.toJsonString(bson_document, arena_allocator, true);
    defer arena_allocator.free(extjson_string);

    try testing.expectEqualSlices(u8, "{\"x\":{\"$binary\":{\"base64\":\"//8=\",\"subType\":\"02\"}}}", extjson_string);
}

test "write bson to json with binary subtype 0x03 - /bson-corpus/tests/binary.json" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const arena_allocator = arena.allocator();

    const DocumentStruct = struct {
        x: BsonBinary,
    };

    const doc = DocumentStruct{
        .x = BsonBinary.fromBytes(u8, &[_]u8{ 115, 255, 210, 100, 68, 179, 76, 105, 144, 232, 231, 209, 223, 192, 53, 212 }, BsonSubType.uuid_old),
    };

    const bson_document = try BsonWriter.writeToBson(DocumentStruct, doc, arena_allocator);
    defer arena_allocator.destroy(bson_document);

    const expected_data =
        // document length (int 32)
        [_]u8{ @as(u8, 29), 0, 0, 0 } ++
        // field type (int 8)
        [_]u8{@as(u8, @intFromEnum(ElementType.binary))} ++
        // field name (null-terminated string)
        "x".* ++ [_]u8{0} ++
        // binary length (int 32)
        std.mem.toBytes(@as(i32, 16)) ++
        // binary sub type (int 8)
        [_]u8{@intFromEnum(BsonSubType.uuid_old)} ++
        // binary data
        [_]u8{ 115, 255, 210, 100, 68, 179, 76, 105, 144, 232, 231, 209, 223, 192, 53, 212 } ++
        // document null terminator (int 8)
        [_]u8{0};

    try testing.expectEqualSlices(u8, &expected_data, bson_document.raw_data);

    try testing.expectError(ExtJsonSerializer.WriteJsonStringError.BinaryFieldRequiresStrictExtJson, ExtJsonSerializer.toJsonString(bson_document, arena_allocator, false));

    const extjson_string = try ExtJsonSerializer.toJsonString(bson_document, arena_allocator, true);
    defer arena_allocator.free(extjson_string);

    try testing.expectEqualSlices(u8, "{\"x\":{\"$binary\":{\"base64\":\"c//SZESzTGmQ6OfR38A11A==\",\"subType\":\"03\"}}}", extjson_string);
}

test "write bson to json with binary subtype 0x04 - /bson-corpus/tests/binary.json" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const arena_allocator = arena.allocator();

    const DocumentStruct = struct {
        x: BsonBinary,
    };

    const doc = DocumentStruct{
        .x = BsonBinary.fromBytes(u8, &[_]u8{ 115, 255, 210, 100, 68, 179, 76, 105, 144, 232, 231, 209, 223, 192, 53, 212 }, BsonSubType.uuid),
    };

    const bson_document = try BsonWriter.writeToBson(DocumentStruct, doc, arena_allocator);
    defer arena_allocator.destroy(bson_document);

    const expected_data =
        // document length (int 32)
        [_]u8{ @as(u8, 29), 0, 0, 0 } ++
        // field type (int 8)
        [_]u8{@as(u8, @intFromEnum(ElementType.binary))} ++
        // field name (null-terminated string)
        "x".* ++ [_]u8{0} ++
        // binary length (int 32)
        std.mem.toBytes(@as(i32, 16)) ++
        // binary sub type (int 8)
        [_]u8{@intFromEnum(BsonSubType.uuid)} ++
        // binary data
        [_]u8{ 115, 255, 210, 100, 68, 179, 76, 105, 144, 232, 231, 209, 223, 192, 53, 212 } ++
        // document null terminator (int 8)
        [_]u8{0};

    try testing.expectEqualSlices(u8, &expected_data, bson_document.raw_data);

    try testing.expectError(ExtJsonSerializer.WriteJsonStringError.BinaryFieldRequiresStrictExtJson, ExtJsonSerializer.toJsonString(bson_document, arena_allocator, false));

    const extjson_string = try ExtJsonSerializer.toJsonString(bson_document, arena_allocator, true);
    defer arena_allocator.free(extjson_string);

    try testing.expectEqualSlices(u8, "{\"x\":{\"$binary\":{\"base64\":\"c//SZESzTGmQ6OfR38A11A==\",\"subType\":\"04\"}}}", extjson_string);
}

test "write bson to json with binary subtype 0x80 - /bson-corpus/tests/binary.json" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const arena_allocator = arena.allocator();

    const DocumentStruct = struct {
        x: BsonBinary,
    };

    const doc = DocumentStruct{
        .x = BsonBinary.fromBytes(u8, &[_]u8{ 115, 255, 210, 100, 68, 179, 76, 105, 144, 232, 231, 209, 223, 192, 53, 212 }, BsonSubType.user_defined),
    };

    const bson_document = try BsonWriter.writeToBson(DocumentStruct, doc, arena_allocator);
    defer arena_allocator.destroy(bson_document);

    const expected_data =
        // document length (int 32)
        [_]u8{ @as(u8, 29), 0, 0, 0 } ++
        // field type (int 8)
        [_]u8{@as(u8, @intFromEnum(ElementType.binary))} ++
        // field name (null-terminated string)
        "x".* ++ [_]u8{0} ++
        // binary length (int 32)
        std.mem.toBytes(@as(i32, 16)) ++
        // binary sub type (int 8)
        [_]u8{@intFromEnum(BsonSubType.user_defined)} ++
        // binary data
        [_]u8{ 115, 255, 210, 100, 68, 179, 76, 105, 144, 232, 231, 209, 223, 192, 53, 212 } ++
        // document null terminator (int 8)
        [_]u8{0};

    try testing.expectEqualSlices(u8, &expected_data, bson_document.raw_data);

    try testing.expectError(ExtJsonSerializer.WriteJsonStringError.BinaryFieldRequiresStrictExtJson, ExtJsonSerializer.toJsonString(bson_document, arena_allocator, false));

    const extjson_string = try ExtJsonSerializer.toJsonString(bson_document, arena_allocator, true);
    defer arena_allocator.free(extjson_string);

    try testing.expectEqualSlices(u8, "{\"x\":{\"$binary\":{\"base64\":\"c//SZESzTGmQ6OfR38A11A==\",\"subType\":\"80\"}}}", extjson_string);
}

test "write bson to json with datetime: epoch - /bson-corpus/tests/datetime.json" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const arena_allocator = arena.allocator();

    const DocumentStruct = struct {
        a: BsonUtcDatetime,
    };

    const doc = DocumentStruct{
        .a = BsonUtcDatetime.fromInt64(0),
    };

    const bson_document = try BsonWriter.writeToBson(DocumentStruct, doc, arena_allocator);
    defer arena_allocator.destroy(bson_document);

    const expected_data =
        // document length (int 32)
        [_]u8{ @as(u8, 16), 0, 0, 0 } ++
        // field type (int 8)
        [_]u8{@as(u8, @intFromEnum(ElementType.utc_date_time))} ++
        // field name (null-terminated string)
        "a".* ++ [_]u8{0} ++
        // datetime value (int 64)
        [_]u8{ 0, 0, 0, 0, 0, 0, 0, 0 } ++
        // document null terminator (int 8)
        [_]u8{0};

    try testing.expectEqualSlices(u8, &expected_data, bson_document.raw_data);

    const relaxed_json_data = try ExtJsonSerializer.toJsonString(bson_document, arena_allocator, false);
    try testing.expectEqualSlices(u8, "{\"a\":{\"$date\":\"1970-01-01T00:00:00Z\"}}", relaxed_json_data);

    const extjson_string = try ExtJsonSerializer.toJsonString(bson_document, arena_allocator, true);
    defer arena_allocator.free(extjson_string);

    try testing.expectEqualSlices(u8, "{\"a\":{\"$date\":{\"$numberLong\":\"0\"}}}", extjson_string);
}

test "write bson to json with datetime: positive ms - /bson-corpus/tests/datetime.json" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const arena_allocator = arena.allocator();

    const DocumentStruct = struct {
        a: BsonUtcDatetime,
    };

    const doc = DocumentStruct{
        .a = BsonUtcDatetime.fromInt64(1356351330501),
    };

    const bson_document = try BsonWriter.writeToBson(DocumentStruct, doc, arena_allocator);
    defer arena_allocator.destroy(bson_document);

    const expected_data =
        // document length (int 32)
        [_]u8{ @as(u8, 16), 0, 0, 0 } ++
        // field type (int 8)
        [_]u8{@as(u8, @intFromEnum(ElementType.utc_date_time))} ++
        // field name (null-terminated string)
        "a".* ++ [_]u8{0} ++
        // datetime value (int 64)
        std.mem.toBytes(@as(i64, 1356351330501)) ++
        // document null terminator (int 8)
        [_]u8{0};

    try testing.expectEqualSlices(u8, &expected_data, bson_document.raw_data);

    const relaxed_json_data = try ExtJsonSerializer.toJsonString(bson_document, arena_allocator, false);
    try testing.expectEqualSlices(u8, "{\"a\":{\"$date\":\"2012-12-24T12:15:30.501Z\"}}", relaxed_json_data);

    const extjson_string = try ExtJsonSerializer.toJsonString(bson_document, arena_allocator, true);
    defer arena_allocator.free(extjson_string);

    try testing.expectEqualSlices(u8, "{\"a\":{\"$date\":{\"$numberLong\":\"1356351330501\"}}}", extjson_string);
}

test "write bson to json with datetime: negative ms - /bson-corpus/tests/datetime.json" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const arena_allocator = arena.allocator();

    const DocumentStruct = struct {
        a: BsonUtcDatetime,
    };

    const doc = DocumentStruct{
        .a = BsonUtcDatetime.fromInt64(-284643869501),
    };

    const bson_document = try BsonWriter.writeToBson(DocumentStruct, doc, arena_allocator);
    defer arena_allocator.destroy(bson_document);

    const expected_data =
        // document length (int 32)
        [_]u8{ @as(u8, 16), 0, 0, 0 } ++
        // field type (int 8)
        [_]u8{@as(u8, @intFromEnum(ElementType.utc_date_time))} ++
        // field name (null-terminated string)
        "a".* ++ [_]u8{0} ++
        // datetime value (int 64)
        std.mem.toBytes(@as(i64, -284643869501)) ++
        // document null terminator (int 8)
        [_]u8{0};

    try testing.expectEqualSlices(u8, &expected_data, bson_document.raw_data);

    const relaxed_json_data = try ExtJsonSerializer.toJsonString(bson_document, arena_allocator, false);
    try testing.expectEqualSlices(u8, "{\"a\":{\"$date\":{\"$numberLong\":\"-284643869501\"}}}", relaxed_json_data);

    const extjson_string = try ExtJsonSerializer.toJsonString(bson_document, arena_allocator, true);
    defer arena_allocator.free(extjson_string);

    try testing.expectEqualSlices(u8, "{\"a\":{\"$date\":{\"$numberLong\":\"-284643869501\"}}}", extjson_string);
}

test "write bson to json with datetime: Y10K - /bson-corpus/tests/datetime.json" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const arena_allocator = arena.allocator();

    const DocumentStruct = struct {
        a: BsonUtcDatetime,
    };

    const doc = DocumentStruct{
        .a = BsonUtcDatetime.fromInt64(253402300800000),
    };

    const bson_document = try BsonWriter.writeToBson(DocumentStruct, doc, arena_allocator);
    defer arena_allocator.destroy(bson_document);

    const expected_data =
        // document length (int 32)
        [_]u8{ @as(u8, 16), 0, 0, 0 } ++
        // field type (int 8)
        [_]u8{@as(u8, @intFromEnum(ElementType.utc_date_time))} ++
        // field name (null-terminated string)
        "a".* ++ [_]u8{0} ++
        // datetime value (int 64)
        std.mem.toBytes(@as(i64, 253402300800000)) ++
        // document null terminator (int 8)
        [_]u8{0};

    try testing.expectEqualSlices(u8, &expected_data, bson_document.raw_data);

    const relaxed_json_data = try ExtJsonSerializer.toJsonString(bson_document, arena_allocator, false);
    try testing.expectEqualSlices(u8, "{\"a\":{\"$date\":{\"$numberLong\":\"253402300800000\"}}}", relaxed_json_data);

    const extjson_string = try ExtJsonSerializer.toJsonString(bson_document, arena_allocator, true);
    defer arena_allocator.free(extjson_string);

    try testing.expectEqualSlices(u8, "{\"a\":{\"$date\":{\"$numberLong\":\"253402300800000\"}}}", extjson_string);
}

test "array1 of int32s" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const arena_allocator = arena.allocator();

    const ArrayDocument = struct {
        array1: []const i32,
    };

    const doc = ArrayDocument{
        .array1 = &[_]i32{ 3, 4 },
    };

    const bson_document = try BsonWriter.writeToBson(ArrayDocument, doc, arena_allocator);
    defer arena_allocator.destroy(bson_document);

    const expected_data =
        // document length (int 32)
        [_]u8{ @as(u8, 32), 0, 0, 0 } ++
        // field type (int 8)
        [_]u8{@as(u8, @intFromEnum(ElementType.array))} ++
        // field name (null-terminated string)
        "array1".* ++ [_]u8{0} ++
        // array bytes length (int 32)
        [_]u8{ @as(u8, 19), 0, 0, 0 } ++

        // array item type (int 8)
        [_]u8{@as(u8, @intFromEnum(ElementType.int32))} ++
        // field name (null-terminated string)
        "0".* ++ [_]u8{0} ++
        // field value
        std.mem.toBytes(@as(i32, 3)) ++
        // array item type (int 8)
        [_]u8{@as(u8, @intFromEnum(ElementType.int32))} ++
        // field name (null-terminated string)
        "1".* ++ [_]u8{0} ++
        // field value
        std.mem.toBytes(@as(i32, 4)) ++

        // array end marker (int 8)
        [_]u8{0} ++
        // document null terminator (int 8)
        [_]u8{0};

    try testing.expectEqualSlices(u8, &expected_data, bson_document.raw_data);
    const json_string = try ExtJsonSerializer.toJsonString(bson_document, arena_allocator, false);
    defer arena_allocator.free(json_string);
    try testing.expectEqualSlices(u8, "{\"array1\":[3,4]}", json_string);

    const parsed_document = try ExtJsonParser.jsonStringToBson(json_string, arena_allocator);
    defer arena_allocator.destroy(parsed_document);

    try testing.expectEqualSlices(u8, &expected_data, parsed_document.raw_data);
}

test "array1 of int64s" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const arena_allocator = arena.allocator();

    const ArrayDocument = struct {
        array1: []const i64,
    };

    const doc = ArrayDocument{
        .array1 = &[_]i64{ 1, 2 },
    };

    const bson_document = try BsonWriter.writeToBson(ArrayDocument, doc, arena_allocator);
    defer arena_allocator.destroy(bson_document);

    const expected_data =
        // document length (int 32)
        [_]u8{ @as(u8, 40), 0, 0, 0 } ++
        // field type (int 8)
        [_]u8{@as(u8, @intFromEnum(ElementType.array))} ++
        // field name (null-terminated string)
        "array1".* ++ [_]u8{0} ++
        // array bytes length (int 32)
        [_]u8{ @as(u8, 27), 0, 0, 0 } ++

        // array item type (int 8)
        [_]u8{@as(u8, @intFromEnum(ElementType.int64))} ++
        // field name (null-terminated string)
        "0".* ++ [_]u8{0} ++
        // field value
        std.mem.toBytes(@as(i64, 1)) ++
        // array item type (int 8)
        [_]u8{@as(u8, @intFromEnum(ElementType.int64))} ++
        // field name (null-terminated string)
        "1".* ++ [_]u8{0} ++
        // field value
        std.mem.toBytes(@as(i64, 2)) ++

        // array end marker (int 8)
        [_]u8{0} ++
        // document null terminator (int 8)
        [_]u8{0};

    try testing.expectEqualSlices(u8, &expected_data, bson_document.raw_data);
    const json_string = try ExtJsonSerializer.toJsonString(bson_document, arena_allocator, false);
    defer arena_allocator.free(json_string);
    try testing.expectEqualSlices(u8, "{\"array1\":[1,2]}", json_string);
}

test "array1 of strings" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const arena_allocator = arena.allocator();

    const ArrayDocument = struct {
        array1: []const [:0]const u8,
    };

    const doc = ArrayDocument{
        .array1 = &[_][:0]const u8{ "hello", "world" },
    };

    const bson_document = try BsonWriter.writeToBson(ArrayDocument, doc, arena_allocator);
    defer arena_allocator.destroy(bson_document);

    const expected_data =
        // document length (int 32)
        std.mem.toBytes(@as(i32, 44)) ++

        // field type (int 8)
        [_]u8{@as(u8, @intFromEnum(ElementType.array))} ++
        // field name (null-terminated string)
        "array1".* ++ [_]u8{0} ++
        // array length
        std.mem.toBytes(@as(i32, 31)) ++

        // array item type (int 8)
        [_]u8{@as(u8, @intFromEnum(ElementType.string))} ++
        // field name (null-terminated string)
        "0".* ++ [_]u8{0} ++
        // string length (int 32)
        std.mem.toBytes(@as(i32, 6)) ++
        // field value
        "hello".* ++ [_]u8{0} ++
        // array item type (int 8)
        [_]u8{@as(u8, @intFromEnum(ElementType.string))} ++
        // field name (null-terminated string)
        "1".* ++ [_]u8{0} ++
        // string length (int 32)
        std.mem.toBytes(@as(i32, 6)) ++
        // field value
        "world".* ++ [_]u8{0} ++
        // array end marker (int 8)
        [_]u8{0} ++
        // document null terminator (int 8)
        [_]u8{0};

    try testing.expectEqualSlices(u8, &expected_data, bson_document.raw_data);
    const json_string = try ExtJsonSerializer.toJsonString(bson_document, arena_allocator, false);
    defer arena_allocator.free(json_string);
    try testing.expectEqualSlices(u8, "{\"array1\":[\"hello\",\"world\"]}", json_string);
}

test "array1 of booleans" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const arena_allocator = arena.allocator();

    const ArrayDocument = struct {
        array1: []const bool,
    };

    const doc = ArrayDocument{
        .array1 = &[_]bool{ true, false },
    };

    const bson_document = try BsonWriter.writeToBson(ArrayDocument, doc, arena_allocator);
    defer arena_allocator.destroy(bson_document);

    const expected_data =
        // document length (int 32)
        std.mem.toBytes(@as(i32, 26)) ++

        // field type (int 8)
        [_]u8{@as(u8, @intFromEnum(ElementType.array))} ++
        // field name (null-terminated string)
        "array1".* ++ [_]u8{0} ++
        // array length
        std.mem.toBytes(@as(i32, 13)) ++

        // array item type (int 8)
        [_]u8{@as(u8, @intFromEnum(ElementType.boolean))} ++
        // field name (null-terminated string)
        "0".* ++ [_]u8{0} ++
        // field value (boolean: true)
        std.mem.toBytes(@as(u8, 1)) ++
        // array item type (int 8)
        [_]u8{@as(u8, @intFromEnum(ElementType.boolean))} ++
        // field name (null-terminated string)
        "1".* ++ [_]u8{0} ++
        // field value (boolean: false)
        std.mem.toBytes(@as(u8, 0)) ++
        // array end marker (int 8)
        [_]u8{0} ++
        // document null terminator (int 8)
        [_]u8{0};

    try testing.expectEqualSlices(u8, &expected_data, bson_document.raw_data);
    const json_string = try ExtJsonSerializer.toJsonString(bson_document, arena_allocator, false);
    defer arena_allocator.free(json_string);
    try testing.expectEqualSlices(u8, "{\"array1\":[true,false]}", json_string);
}

test "array1 of int32 - extended json" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const arena_allocator = arena.allocator();

    const json_string = "{\"a\":[{\"$numberInt\":\"10\"}]}";
    const bson_document = try ExtJsonParser.jsonStringToBson(json_string, arena_allocator);
    defer arena_allocator.destroy(bson_document);

    const json_string_relaxed = try ExtJsonSerializer.toJsonString(bson_document, arena_allocator, false);
    defer arena_allocator.free(json_string_relaxed);
    try testing.expectEqualSlices(u8, "{\"a\":[10]}", json_string_relaxed);
}

test "regex options - handle unsorted options" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const arena_allocator = arena.allocator();

    const json_string = "{\"a\":{\"$regularExpression\":{\"pattern\":\"^[a-z]+$\",\"options\":\"mi\"}}}";
    const bson_document = try ExtJsonParser.jsonStringToBson(json_string, arena_allocator);
    defer arena_allocator.destroy(bson_document);

    const json_string_relaxed = try ExtJsonSerializer.toJsonString(bson_document, arena_allocator, false);
    defer arena_allocator.free(json_string_relaxed);
    try testing.expectEqualSlices(u8, "{\"a\":{\"$regularExpression\":{\"pattern\":\"^[a-z]+$\",\"options\":\"im\"}}}", json_string_relaxed);
}

test "regex options - handle invalid options" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const arena_allocator = arena.allocator();

    const json_string = "{\"a\":{\"$regularExpression\":{\"pattern\":\"^[a-z]+$\",\"options\":\"q\"}}}";
    try testing.expectError(JsonParsingRegExpError.InvalidRegExpOptions, ExtJsonParser.jsonStringToBson(json_string, arena_allocator));
}
