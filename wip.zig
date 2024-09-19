const std = @import("std");

fn Fields(T: type) type {
    var fields: [std.meta.fields(T).len]std.builtin.Type.StructField = undefined;
    for (std.meta.fields(T), 0..) |field, index| {
        fields[index] = .{
            .name = std.fmt.comptimePrint("{}", .{index}),
            .type = FieldType(field.type),
            .default_value = null,
            .is_comptime = false,
            .alignment = @alignOf(FieldType(field.type)),
        };
    }
    return @Type(.{
        .@"struct" = .{
            .layout = .auto,
            .fields = &fields,
            .decls = &.{},
            .is_tuple = true,
        },
    });
}

fn FieldType(T: type) type {
    return switch (@typeInfo(T)) {
        .comptime_int => usize,
        .bool => bool,
        .pointer => []const u8,
        else => @compileError("Unsupported type: " ++ @typeName(T)),
    };
}

fn SomeType(T: type) type {
    return struct {
        fields: Fields(T),
    };
}

pub fn main() !void {
    var buf: [1024]u8 = undefined;
    const foo = try std.fmt.bufPrint(&buf, "{s}", .{"hello"});
    const args = .{ foo, 123, true };
    const T = SomeType(@TypeOf(args));
    var x: T = undefined;
    inline for (args, 0..) |arg, index| {
        @field(x.fields, std.fmt.comptimePrint("{}", .{index})) = arg;
    }

    std.debug.print("{any}\n", .{x});
}
