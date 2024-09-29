const std = @import("std");

const jetcommon = @import("jetcommon");

const fields = @import("fields.zig");

pub fn coerce(
    Table: type,
    relations: []const type,
    field_info: fields.FieldInfo,
    value: anytype,
) CoercedValue(fields.ColumnType(Table, relations, field_info)) {
    switch (field_info.context) {
        .limit => return switch (@typeInfo(@TypeOf(value))) {
            .int, .comptime_int => .{ .value = value },
            else => coerceDelegate(usize, value),
        },
        else => {},
    }

    const T = fields.ColumnType(Table, relations, field_info);

    if (T == jetcommon.types.DateTime) return value.microseconds;

    return switch (@typeInfo(@TypeOf(value))) {
        .int, .comptime_int => switch (@typeInfo(T)) {
            .int => .{ .value = value },
            else => coerceDelegate(T, value),
        },
        .float, .comptime_float => switch (@typeInfo(T)) {
            .float => .{ .value = value },
            else => coerceDelegate(T, value),
        },
        .pointer => |info| switch (@typeInfo(T)) {
            .int => switch (@typeInfo(info.child)) {
                .int => switch (info.size) {
                    .Slice => coerceInt(T, value),
                    else => .{ .value = value },
                },
                else => if (comptime canCoerceDelegate(info.child))
                    coerceDelegate(T, value.*)
                else
                    coerceInt(T, value),
            },
            .float => switch (@typeInfo(info.child)) {
                .float => .{ .value = value },
                else => if (comptime canCoerceDelegate(info.child))
                    coerceDelegate(T, value.*)
                else
                    coerceFloat(T, value),
            },
            .bool => switch (@typeInfo(info.child)) {
                .bool => .{ .value = value },
                else => if (comptime canCoerceDelegate(info.child))
                    coerceDelegate(T, value.*)
                else
                    coerceBool(T, value),
            },
            .pointer => if (comptime canCoerceDelegate(info.child))
                coerceDelegate(T, value.*)
            else
                .{ .value = value }, // TODO
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
pub fn coerceDelegate(Target: type, value: anytype) CoercedValue(Target) {
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

fn canCoerceDelegate(T: type) bool {
    return switch (@typeInfo(T)) {
        .@"struct", .@"union" => @hasDecl(T, "toJetQuery") and @typeInfo(@TypeOf(T.toJetQuery)) == .@"fn",
        .pointer => |info| @hasDecl(info.child, "toJetQuery") and @typeInfo(@TypeOf(T.toJetQuery)) == .@"fn",
        else => false,
    };
}

fn CoercedValue(Target: type) type {
    return struct {
        value: Target = undefined, // Never used if `err` is present
        err: ?anyerror = null,
    };
}

fn coerceInt(T: type, value: []const u8) CoercedValue(T) {
    const coerced = std.fmt.parseInt(T, value, 10) catch |err| {
        return .{
            .err = switch (err) {
                error.InvalidCharacter, error.Overflow => error.JetQueryInvalidIntegerString,
            },
        };
    };
    return .{ .value = coerced };
}

fn coerceFloat(T: type, value: []const u8) CoercedValue(T) {
    const coerced = std.fmt.parseFloat(T, value) catch |err| {
        return .{
            .err = switch (err) {
                error.InvalidCharacter => error.JetQueryInvalidFloatString,
            },
        };
    };
    return .{ .value = coerced };
}

fn coerceBool(T: type, value: []const u8) CoercedValue(T) {
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
