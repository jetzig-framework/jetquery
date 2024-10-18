const std = @import("std");

const sql = @import("sql.zig");

pub const Column = struct {
    name: []const u8,
    type: type,
    table: type,
    function: ?sql.FunctionContext = null,
    alias: ?[]const u8 = null,

    pub fn ResultType(comptime self: Column, Adapter: type) type {
        return if (self.function) |function| Adapter.Aggregate(function) else self.type;
    }
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
                    fields[index] = primaryColumn(Table, @tagName(arg));
                    index += 1;
                },
                .@"struct" => {
                    const count = nestedColumns(Table, relations, arg, undefined, true);
                    var buf: [count]Column = undefined;
                    const nested = nestedColumns(Table, relations, arg, &buf, false);
                    @memcpy(fields[index .. index + count], nested);
                    index += count;
                },
                .type => {
                    if (@hasField(arg, "__jetquery_function")) {
                        const function = (arg{}).__jetquery_function;
                        var column = primaryColumn(Table, function.column_name);
                        column.function = function.context;
                        column.alias = @tagName(function.context) ++ "_" ++ column.name;
                        fields[index] = column;
                        index += 1;
                    } else {
                        @compileError("Unexpected type in columns: `" ++ @typeName(arg) ++ "`");
                    }
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
            if (@TypeOf(arg) == sql.Function) {
                _ = primaryColumn(Table, arg.column_name);
                size += 1;
                continue;
            }

            switch (@typeInfo(@TypeOf(arg))) {
                .enum_literal, .@"enum" => {
                    _ = primaryColumn(Table, @tagName(arg));
                    size += 1;
                },
                .pointer => {
                    _ = primaryColumn(Table, arg);
                    size += 1;
                },
                .@"struct" => {
                    size += nestedColumns(Table, relations, arg, undefined, true);
                },
                .type => {
                    if (@hasField(arg, "__jetquery_function")) {
                        size += 1;
                    } else {
                        @compileError(
                            "Unsupported type in column arguments: `" ++ @typeName(arg) ++ "`",
                        );
                    }
                },
                else => |tag| {
                    @compileError(
                        "Expected [enum, enum_literal, []const u8, struct] column arguments, found: `" ++ @tagName(tag) ++ "`",
                    );
                },
            }
        }
        return size;
    }
}

fn primaryColumn(
    Table: type,
    comptime name: []const u8,
) Column {
    comptime {
        for (Table.columns()) |column| {
            if (std.mem.eql(u8, column.name, name)) return .{
                .table = Table,
                .name = name,
                .type = column.type,
            };
        }
        @compileError(std.fmt.comptimePrint(
            "Failed matching column `{s}` in Schema for `{s}`.",
            .{ name, Table.name },
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
