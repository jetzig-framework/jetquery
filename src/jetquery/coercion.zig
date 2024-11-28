const std = @import("std");

const jetcommon = @import("jetcommon");

const fields = @import("fields.zig");

pub fn coerce(
    Adapter: type,
    Table: type,
    field_info: fields.FieldInfo,
    value: anytype,
) CoercedValue(Adapter, fields.ColumnType(Adapter, Table, field_info), @TypeOf(value)) {
    switch (field_info.context) {
        .limit, .offset => return switch (@typeInfo(@TypeOf(value))) {
            .int, .comptime_int => .{ .value = value },
            else => coerceDelegate(usize, value),
        },
        else => {},
    }

    const MaybeT = fields.ColumnType(Adapter, Table, field_info);
    const T = if (@typeInfo(MaybeT) == .optional)
        @typeInfo(MaybeT).optional.child
    else
        MaybeT;

    if (@TypeOf(value) == jetcommon.types.DateTime) return .{ .value = value.microseconds() };
    if (@TypeOf(value) == ?jetcommon.types.DateTime) return if (value) |capture|
        .{ .value = @as(MaybeT, capture.microseconds()) }
    else
        .{ .value = @as(MaybeT, null) };

    return switch (@typeInfo(@TypeOf(value))) {
        .null => .{ .value = null },
        .int, .comptime_int => switch (@typeInfo(T)) {
            .int => .{ .value = @intCast(value) },
            .bool => .{ .value = value == 1 },
            else => coerceDelegate(Adapter, T, value),
        },
        .float, .comptime_float => switch (@typeInfo(T)) {
            .float => .{ .value = @floatCast(value) },
            .bool => .{ .value = value == 1.0 },
            else => coerceDelegate(Adapter, T, value),
        },
        .bool => switch (@typeInfo(T)) {
            .bool => .{ .value = value },
            else => if (comptime canCoerceDelegate(@TypeOf(value)))
                coerceDelegate(Adapter, T, value.*)
            else
                coerceBool(Adapter, T, value),
        },
        .pointer => |info| switch (@typeInfo(T)) {
            .int => switch (@typeInfo(info.child)) {
                .int => switch (info.size) {
                    // FIXME: For now we get away with this because postgres does not have a u8
                    // type but we may need a better way to identify strings if another database
                    // adapter does support u8.
                    .Slice => if (@TypeOf(value) == []const u8)
                        coerceInt(Adapter, T, value)
                    else
                        .{ .value = value },
                    else => .{ .value = value },
                },
                else => if (comptime canCoerceDelegate(info.child))
                    coerceDelegate(Adapter, T, value.*)
                else
                    coerceInt(Adapter, T, value),
            },
            .float => switch (@typeInfo(info.child)) {
                .float => switch (info.size) {
                    .Slice => .{ .value = value },
                    else => .{ .value = value },
                },
                else => if (comptime canCoerceDelegate(info.child))
                    coerceDelegate(Adapter, T, value.*)
                else
                    coerceFloat(Adapter, T, value),
            },
            .bool => switch (@typeInfo(info.child)) {
                .bool => switch (info.size) {
                    .Slice => .{ .value = value },
                    else => .{ .value = value },
                },
                .int, .comptime_int => .{ .value = value.* == 1 },
                .float, .comptime_float => .{ .value = value.* == 1.0 },
                else => if (comptime canCoerceDelegate(info.child))
                    coerceDelegate(Adapter, T, value.*)
                else
                    coerceBool(Adapter, T, value),
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
        .array => .{ .value = &value },
        .optional => |info| if (value) |capture| blk: {
            comptime var source_info = field_info.info;
            source_info.type = @typeInfo(field_info.info.type).optional.child;
            const optional_field_info = fields.fieldInfo(
                source_info,
                field_info.Table,
                field_info.name,
                field_info.context,
            );
            const coerced = coerce(Adapter, Table, optional_field_info, capture);
            break :blk .{ .value = @as(?T, coerced.value), .err = coerced.err };
        } else .{ .value = @as(?info.child, null) },
        else => coerceDelegate(Adapter, T, value),
    };
}

// Call `toJetQuery` with a given type and an allocator on the given arg field. Although
// this function expects a return value not specific to JetQuery, the intention is that
// arbitrary types can implement `toJetQuery` if the author wants them to be used with
// JetQuery, otherwise a typical Zig compile error will occur. This feature is used by
// Zmpl for converting Zmpl Values, allowing e.g. request params in Jetzig to be used as
// JetQuery whereclause/etc. params.
pub fn coerceDelegate(
    Adapter: type,
    Target: type,
    value: anytype,
) CoercedValue(Adapter, Target, @TypeOf(value)) {
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

pub fn CoercedValue(Adapter: type, Target: type, Source: type) type {
    const T = CoercedValueType(Adapter, Target, Source);

    return struct {
        value: T = undefined, // Never used if `err` is present
        err: ?anyerror = null,
    };
}

fn CoercedValueType(Adapter: type, Target: type, Source: type) type {
    return switch (@typeInfo(Source)) {
        .null => @TypeOf(null),
        .pointer => |info| if (info.child == Target and info.size == .Slice)
            []const Target
        else
            Target,
        .array => |info| if (info.child == Target) []const Target else Target,
        .optional => |info| CoercedValueType(Adapter, Target, info.child),
        else => if (Target == jetcommon.DateTime)
            Adapter.DateTimePrimitive
        else if (Target == ?jetcommon.DateTime)
            ?Adapter.DateTimePrimitive
        else
            Target,
    };
}

fn coerceInt(Adapter: type, T: type, value: []const u8) CoercedValue(Adapter, T, @TypeOf(value)) {
    const coerced = std.fmt.parseInt(T, value, 10) catch |err| {
        return .{
            .err = switch (err) {
                error.InvalidCharacter, error.Overflow => error.JetQueryInvalidIntegerString,
            },
        };
    };
    return .{ .value = coerced };
}

fn coerceFloat(Adapter: type, T: type, value: []const u8) CoercedValue(Adapter, T, @TypeOf(value)) {
    const coerced = std.fmt.parseFloat(T, value) catch |err| {
        return .{
            .err = switch (err) {
                error.InvalidCharacter => error.JetQueryInvalidFloatString,
            },
        };
    };
    return .{ .value = coerced };
}

fn coerceBool(Adapter: type, T: type, value: []const u8) CoercedValue(Adapter, T, @TypeOf(value)) {
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
