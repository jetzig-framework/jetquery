const std = @import("std");

const sql = @import("sql.zig");

pub const Column = struct {
    name: []const u8,
    type: type,
    table: type,
    from: ?[]const u8 = null,
    function: ?sql.FunctionContext = null,
    alias: ?[]const u8 = null,
    sql: ?[]const u8 = null,

    pub fn ResultType(comptime self: Column, Adapter: type) type {
        return if (self.function) |function| Adapter.Aggregate(function) else self.type;
    }

    pub fn as(comptime self: Column, comptime alias: anytype) Column {
        var column = self;
        column.alias = @tagName(alias);
        return column;
    }
};

pub const TranslateOptions = struct {
    from: ?[]const u8 = null,
};
pub fn translate(
    Table: type,
    relations: []const type,
    comptime maybe_args: anytype,
    comptime options: TranslateOptions,
) [sizeOf(Table, relations, maybe_args)]Column {
    comptime {
        const args = if (@TypeOf(maybe_args) == @TypeOf(null)) return .{} else maybe_args;

        if (args.len == 0) {
            var columns = Table.columns();
            if (options.from) |from| {
                for (&columns) |*column| column.from = from;
            }
            return columns;
        }

        var fields: [sizeOf(Table, relations, args)]Column = undefined;
        var index: usize = 0;

        for (args) |arg| {
            if (@TypeOf(arg) == Column) {
                if (arg.alias == null) @compileError(std.fmt.comptimePrint(
                    \\Custom SQL columns must be aliased. Call `as("...")` to specify an alias. Failed for column `{?s}`,
                ,
                    .{arg.sql},
                ));
                fields[index] = arg;
                index += 1;
                continue;
            }
            switch (@typeInfo(@TypeOf(arg))) {
                .enum_literal, .@"enum" => {
                    fields[index] = primaryColumn(Table, @tagName(arg));
                    fields[index].from = options.from;
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
                        column.alias = function.alias;
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
    comptime maybe_args: anytype,
) usize {
    comptime {
        const args = if (@TypeOf(maybe_args) == @TypeOf(null)) return 0 else maybe_args;

        if (args.len == 0) return Table.columns().len;

        var size: usize = 0;
        for (args) |arg| {
            if (@TypeOf(arg) == sql.Function) {
                _ = primaryColumn(Table, arg.column_name); // validate column
                size += 1;
                continue;
            }

            if (@TypeOf(arg) == Column) {
                size += 1;
                continue;
            }

            switch (@typeInfo(@TypeOf(arg))) {
                .enum_literal, .@"enum" => {
                    _ = primaryColumn(Table, @tagName(arg)); // validate column
                    size += 1;
                },
                .pointer => {
                    _ = primaryColumn(Table, arg); // validate column
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

pub fn primaryColumn(
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
                                    .from = Relation.relation_name,
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
