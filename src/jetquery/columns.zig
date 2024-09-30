const std = @import("std");

pub const Column = struct {
    name: []const u8,
    type: type,
    table: type,
};

pub fn translate(
    Table: type,
    relations: []const type,
    comptime args: anytype,
) [sizeOf(Table, relations, args)]Column {
    comptime {
        if (args.len == 0) return Table.columns();

        var fields: [sizeOf(Table, relations, args)]Column = undefined;
        var index: usize = 0;

        for (args) |arg| {
            switch (@typeInfo(@TypeOf(arg))) {
                .enum_literal, .@"enum" => {
                    fields[index] = primaryColumn(Table, arg);
                    index += 1;
                },
                .@"struct" => {
                    const count = nestedColumns(Table, relations, arg, undefined, true);
                    var buf: [count]Column = undefined;
                    const nested = nestedColumns(Table, relations, arg, &buf, false);
                    @memcpy(fields[index .. index + count], nested);
                    index += count;
                },
                else => |tag| {
                    @compileError(
                        "Expected [enum, enum_literal, struct] column arguments, found: `" ++ @tagName(tag) ++ "`",
                    );
                },
            }
        }

        return fields;
    }
}

fn sizeOf(
    Table: type,
    comptime relations: []const type,
    comptime args: anytype,
) usize {
    comptime {
        if (args.len == 0) return Table.columns().len;

        var size: usize = 0;
        for (args) |arg| {
            switch (@typeInfo(@TypeOf(arg))) {
                .enum_literal, .@"enum" => {
                    _ = primaryColumn(Table, arg);
                    size += 1;
                },

                .@"struct" => {
                    size += nestedColumns(Table, relations, arg, undefined, true);
                },
                else => |tag| {
                    @compileError(
                        "Expected [enum, enum_literal, struct] column arguments, found: `" ++ @tagName(tag) ++ "`",
                    );
                },
            }
        }
        return size;
    }
}

fn primaryColumn(
    Table: type,
    arg: anytype,
) Column {
    comptime {
        for (Table.columns()) |column| {
            if (std.mem.eql(u8, column.name, @tagName(arg))) return .{
                .table = Table,
                .name = @tagName(arg),
                .type = column.type,
            };
        }
        @compileError(std.fmt.comptimePrint(
            "Failed matching column `{s}` in Schema for `{s}`.",
            .{ @tagName(arg), Table.name },
        ));
    }
}

fn nestedColumns(
    Table: type,
    relations: []const type,
    comptime arg: anytype,
    buf: []Column,
    comptime count: bool,
) if (count) usize else []const Column {
    var index: usize = 0;

    for (std.meta.fields(@TypeOf(arg))) |field| {
        for (relations) |Relation| {
            if (std.mem.eql(u8, field.name, Relation.relation_name)) {
                for (@field(arg, field.name)) |nested_arg| {
                    for (Relation.Source.columns()) |column| {
                        if (std.mem.eql(u8, column.name, @tagName(nested_arg))) {
                            if (!count) {
                                buf[index] = .{
                                    .name = @tagName(nested_arg),
                                    .table = Relation.Source,
                                    .type = column.type,
                                };
                            }
                            index += 1;
                            break;
                        }
                    } else @compileError(std.fmt.comptimePrint(
                        "Failed matching column `{s}.{s}` .",
                        .{ field.name, @tagName(nested_arg) },
                    ));
                }
                break;
            }
        } else @compileError(std.fmt.comptimePrint(
            "Failed matching relation `{s}` for table `{s}`.",
            .{ field.name, Table.name },
        ));
    }

    return if (count) index else buf[0..];
}
