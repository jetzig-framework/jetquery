const std = @import("std");

const jetcommon = @import("jetcommon");

const fields = @import("fields.zig");

pub fn coerce(
    Table: type,
    field_info: fields.FieldInfo,
    value: anytype,
) CoercedValue(fields.ColumnType(Table, field_info), @TypeOf(value)) {
    switch (field_info.context) {
        .limit, .offset => return switch (@typeInfo(@TypeOf(value))) {
            .int, .comptime_int => .{ .value = value },
            else => coerceDelegate(usize, value),
        },
        else => {},
    }

    const T = fields.ColumnType(Table, field_info);

    if (T == jetcommon.types.DateTime) return value.microseconds;

    return switch (@typeInfo(@TypeOf(value))) {
        .null => .{ .value = null },
        .int, .comptime_int => switch (@typeInfo(T)) {
            .int => .{ .value = @intCast(value) },
            else => coerceDelegate(T, value),
        },
        .float, .comptime_float => switch (@typeInfo(T)) {
            .float => .{ .value = @floatCast(value) },
            else => coerceDelegate(T, value),
        },
        .pointer => |info| switch (@typeInfo(T)) {
            .int => switch (@typeInfo(info.child)) {
                .int => switch (info.size) {
                    // FIXME: For now we get away with this because postgres does not have a u8
                    // type but we may need a better way to identify strings if another database
                    // adapter does support u8.
                    .Slice => if (@TypeOf(value) == []const u8)
                        coerceInt(T, value)
                    else
                        .{ .value = value },
                    else => .{ .value = value },
                },
                else => if (comptime canCoerceDelegate(info.child))
                    coerceDelegate(T, value.*)
                else
                    coerceInt(T, value),
            },
            .float => switch (@typeInfo(info.child)) {
                .float => switch (info.size) {
                    .Slice => .{ .value = value },
                    else => .{ .value = value },
                },
                else => if (comptime canCoerceDelegate(info.child))
                    coerceDelegate(T, value.*)
                else
                    coerceFloat(T, value),
            },
            .bool => switch (@typeInfo(info.child)) {
                .bool => switch (info.size) {
                    .Slice => .{ .value = value },
                    else => .{ .value = value },
                },
                else => if (comptime canCoerceDelegate(info.child))
                    coerceDelegate(T, value.*)
                else
                    coerceBool(T, value),
            },
            .pointer => if (comptime canCoerceDelegate(info.child))
                coerceDelegate(T, value.*)
            else
                .{ .value = value }, // Let Zig compiler figure it out
            else => if (comptime canCoerceDelegate(info.child))
                coerceDelegate(T, value.*)
            else
                @compileError("Incompatible types: `" ++
                    @typeName(T) ++ "` and `" ++ @typeName(info.child) ++ "`"),
        },
        else => coerceDelegate(T, value),
    };
}

// Call `toJetQuery` with a given type and an allocator on the given arg field. Although
// this function expects a return value not specific to JetQuery, the intention is that
// arbitrary types can implement `toJetQuery` if the author wants them to be used with
// JetQuery, otherwise a typical Zig compile error will occur. This feature is used by
// Zmpl for converting Zmpl Values, allowing e.g. request params in Jetzig to be used as
// JetQuery whereclause/etc. params.
pub fn coerceDelegate(Target: type, value: anytype) CoercedValue(Target, @TypeOf(value)) {
    const Source = @TypeOf(value);
    if (comptime canCoerceDelegate(Source)) {
        const coerced = value.toJetQuery(Target) catch |err| {
            return .{ .err = err };
        };
        return .{ .value = coerced, .err = null };
    } else {
        @compileError("Incompatible types: `" ++ @typeName(Target) ++ "` and `" ++ @typeName(Source) ++ "`");
    }
}

pub fn canCoerceDelegate(T: type) bool {
    return switch (@typeInfo(T)) {
        .@"struct", .@"union" => std.meta.hasFn(T, "toJetQuery"),
        .pointer => |info| std.meta.hasFn(info.child, "toJetQuery"),
        else => false,
    };
}

pub fn CoercedValue(Target: type, Source: type) type {
    const T = switch (@typeInfo(Source)) {
        .null => @TypeOf(null),
        .pointer => |info| if (info.child == Target and info.size == .Slice)
            []const Target
        else
            Target,
        else => Target,
    };

    return struct {
        value: T = undefined, // Never used if `err` is present
        err: ?anyerror = null,
    };
}

fn coerceInt(T: type, value: []const u8) CoercedValue(T, @TypeOf(value)) {
    const coerced = std.fmt.parseInt(T, value, 10) catch |err| {
        return .{
            .err = switch (err) {
                error.InvalidCharacter, error.Overflow => error.JetQueryInvalidIntegerString,
            },
        };
    };
    return .{ .value = coerced };
}

fn coerceFloat(T: type, value: []const u8) CoercedValue(T, @TypeOf(value)) {
    const coerced = std.fmt.parseFloat(T, value) catch |err| {
        return .{
            .err = switch (err) {
                error.InvalidCharacter => error.JetQueryInvalidFloatString,
            },
        };
    };
    return .{ .value = coerced };
}

fn coerceBool(T: type, value: []const u8) CoercedValue(T, @TypeOf(value)) {
    if (value.len != 1) return .{ .err = error.JetQueryInvalidBooleanString };

    const maybe_boolean = switch (value[0]) {
        '1' => true,
        '0' => false,
        else => null,
    };

    return if (maybe_boolean) |boolean|
        .{ .value = boolean }
    else
        .{ .err = error.JetQueryInvalidBooleanString };
}
