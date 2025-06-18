const std = @import("std");
const assert = std.debug.assert;
const time = std.time;
const parseInt = std.fmt.parseInt;

pub const DATETIME_MAX_I64 = 253402300800000;

pub fn dateTimeToUtcString(date_time_ms: i64, buffer: []u8) std.fmt.BufPrintError![]u8 {
    const ms = @as(u64, @intCast(@mod(date_time_ms, 1000)));
    const secs = @as(u64, @intCast(@divTrunc(date_time_ms, 1000)));
    const epoch_seconds = time.epoch.EpochSeconds{ .secs = secs };
    const epoch_day = epoch_seconds.getEpochDay();
    const day_seconds = epoch_seconds.getDaySeconds();
    const year_day = epoch_day.calculateYearDay();
    const month_day = year_day.calculateMonthDay();
    const hour = day_seconds.getHoursIntoDay();
    const minute = day_seconds.getMinutesIntoHour();
    const second = day_seconds.getSecondsIntoMinute();

    if (ms == 0) {
        return std.fmt.bufPrint(buffer, "{d:04}-{d:02}-{d:02}T{d:02}:{d:02}:{d:02}Z", .{ year_day.year, month_day.month.numeric(), month_day.day_index + 1, hour, minute, second });
    }

    return std.fmt.bufPrint(buffer, "{d:04}-{d:02}-{d:02}T{d:02}:{d:02}:{d:02}.{d:03}Z", .{ year_day.year, month_day.month.numeric(), month_day.day_index + 1, hour, minute, second, ms });
}

/// Internet Date/Time Format : https://datatracker.ietf.org/doc/html/rfc3339#section-5.6
pub fn parseUtcDateTimeISO8601(date_time_string: []const u8) !i64 {
    assert(date_time_string.len == 20 or date_time_string.len == 24);
    assert(date_time_string[date_time_string.len - 1] == 'Z');

    const year: time.epoch.Year = try parseInt(time.epoch.Year, date_time_string[0..4], 10);
    const month = try parseInt(u4, date_time_string[5..7], 10);
    const day = try parseInt(u5, date_time_string[8..10], 10);
    assert(date_time_string[10] == 'T');
    const hour = try parseInt(i32, date_time_string[11..13], 10);
    const minute = try parseInt(u16, date_time_string[14..16], 10);
    const second = try parseInt(u8, date_time_string[17..19], 10);
    const ms = if (date_time_string.len > 20) try parseInt(u9, date_time_string[20..23], 10) else 0;

    var result: i64 =
        second +
        minute * 60 +
        hour * 60 * 60;

    var days_total: i64 = day - 1;

    days_total += (year - time.epoch.epoch_year) * 365;

    if (year >= 1970 and year < 2100) {
        const leap_days = @divTrunc(year - 1 - 1968, 4); // excluding specified year
        days_total += leap_days;
    } else {
        // TODO: handle this case
        @panic("year is not in range 1970-2100");
    }

    const leap_kind: time.epoch.YearLeapKind = if (time.epoch.isLeapYear(year)) .leap else .not_leap;

    days_total += calculateDaysUpToMonth(@as(time.epoch.Month, @enumFromInt(month)));
    if (leap_kind == .leap and month > 2) {
        days_total += 1;
    }

    result += days_total * 24 * 60 * 60;

    return result * 1000 + ms;
}

fn calculateDaysUpToMonth(month: time.epoch.Month) i64 {
    return switch (month) {
        .jan => 0,
        .feb => comptime time.epoch.getDaysInMonth(.not_leap, .jan) + calculateDaysUpToMonth(.jan),
        .mar => comptime time.epoch.getDaysInMonth(.not_leap, .feb) + calculateDaysUpToMonth(.feb),
        .apr => comptime time.epoch.getDaysInMonth(.not_leap, .mar) + calculateDaysUpToMonth(.mar),
        .may => comptime time.epoch.getDaysInMonth(.not_leap, .apr) + calculateDaysUpToMonth(.apr),
        .jun => comptime time.epoch.getDaysInMonth(.not_leap, .may) + calculateDaysUpToMonth(.may),
        .jul => comptime time.epoch.getDaysInMonth(.not_leap, .jun) + calculateDaysUpToMonth(.jun),
        .aug => comptime time.epoch.getDaysInMonth(.not_leap, .jul) + calculateDaysUpToMonth(.jul),
        .sep => comptime time.epoch.getDaysInMonth(.not_leap, .aug) + calculateDaysUpToMonth(.aug),
        .oct => comptime time.epoch.getDaysInMonth(.not_leap, .sep) + calculateDaysUpToMonth(.sep),
        .nov => comptime time.epoch.getDaysInMonth(.not_leap, .oct) + calculateDaysUpToMonth(.oct),
        .dec => comptime time.epoch.getDaysInMonth(.not_leap, .nov) + calculateDaysUpToMonth(.nov),
    };
}

test "parseUtcDateTimeISO8601 - epoch start" {
    const date_time_string = "1970-01-01T00:00:00.000Z";
    const result = try parseUtcDateTimeISO8601(date_time_string);
    try std.testing.expectEqual(0, result);
}

test "parseUtcDateTimeISO8601 - 1ms" {
    const date_time_string = "1970-01-01T00:00:00.001Z";
    const result = try parseUtcDateTimeISO8601(date_time_string);
    try std.testing.expectEqual(1, result);
}

test "parseUtcDateTimeISO8601 - 1s" {
    const date_time_string = "1970-01-01T00:00:01.000Z";
    const result = try parseUtcDateTimeISO8601(date_time_string);
    try std.testing.expectEqual(1000, result);
}

test "parseUtcDateTimeISO8601 - 1m" {
    const date_time_string = "1970-01-01T00:01:00.000Z";
    const result = try parseUtcDateTimeISO8601(date_time_string);
    try std.testing.expectEqual(60000, result);
}

test "parseUtcDateTimeISO8601 - 1h" {
    const date_time_string = "1970-01-01T01:00:00.000Z";
    const result = try parseUtcDateTimeISO8601(date_time_string);
    try std.testing.expectEqual(3600000, result);
}

test "parseUtcDateTimeISO8601 - 1d" {
    const date_time_string = "1970-01-02T00:00:00.000Z";
    const result = try parseUtcDateTimeISO8601(date_time_string);
    try std.testing.expectEqual(86400000, result);
}

test "parseUtcDateTimeISO8601 - 1 month" {
    const date_time_string = "1970-02-01T00:00:00.000Z";
    const result = try parseUtcDateTimeISO8601(date_time_string);
    try std.testing.expectEqual(2678400000, result);
}

test "parseUtcDateTimeISO8601 - 1y" {
    const date_time_string = "1971-01-01T00:00:00.000Z";
    const result = try parseUtcDateTimeISO8601(date_time_string);
    try std.testing.expectEqual(31536000000, result);
}

test "parseUtcDateTimeISO8601" {
    const date_time_string = "2025-06-07T22:06:54.290Z";
    const result = try parseUtcDateTimeISO8601(date_time_string);
    try std.testing.expectEqual(1749334014290, result);
}

test "parseUtcDateTimeISO8601 - 2012-12-24T12:15:30.500Z" {
    const date_time_string = "2012-12-24T12:15:30.501Z";
    const result = try parseUtcDateTimeISO8601(date_time_string);
    try std.testing.expectEqual(1356351330501, result);
}

test "epoch time start" {
    var buffer: [20]u8 = undefined;
    const date_time_ms: i64 = 0;
    const date_time_string = try dateTimeToUtcString(date_time_ms, &buffer);
    try std.testing.expectEqualStrings("1970-01-01T00:00:00Z", date_time_string);
}

test "epoch time" {
    var buffer: [24]u8 = undefined;
    const date_time_ms: i64 = 1749334014290;
    const date_time_string = try dateTimeToUtcString(date_time_ms, &buffer);
    try std.testing.expectEqualStrings("2025-06-07T22:06:54.290Z", date_time_string);
}
