const std = @import("std");

const jetquery = @import("../jetquery.zig");

pub const render = @import("sql/render.zig").render;
pub const renderUpdateRuntime = @import("sql/render.zig").renderUpdateRuntime;
pub const Where = @import("sql/Where.zig");

pub const FieldState = struct {
    name: []const u8,
    modified: bool,
};

pub const QueryContext = enum {
    select,
    update,
    insert,
    delete,
    delete_all,
    count,
    none,
};

pub const OrderClause = struct {
    // TODO: Allow ordering on relations.
    column: jetquery.columns.Column,
    direction: OrderDirection,
};

pub const OrderDirection = enum { ascending, descending, asc, desc };
pub const CountContext = enum { all, distinct };
pub const CountColumn = struct {
    type: CountContext,
};

// Information about a column's position in a `SELECT` query, used to determine how to assign
// result values to the `ResultType`. We cannot rely on the column name returned by PostgreSQL as
// we only get the name of the column and not the table it came from, so we would otherwise not
// be able to match columns to tables in joins. This also means we can skip a few allocs in
// pg.zig by not requesting column names.
pub const ColumnInfo = struct {
    index: usize,
    name: []const u8,
    type: type,
    relation: ?type,
};

pub const FunctionContext = enum { min, max, count, avg, sum };
pub const Function = struct {
    context: FunctionContext,
    column_name: []const u8,
    alias: []const u8,
};

fn FunctionType(comptime context: FunctionContext, comptime column_tag: anytype) type {
    if (@typeInfo(@TypeOf(column_tag)) != .enum_literal) {
        @compileError(std.fmt.comptimePrint(
            "Expected enum literal as SQL function argument, found: `{s}`",
            .{@tagName(@typeInfo(@TypeOf(column_tag)))},
        ));
    }

    return struct {
        comptime __jetquery_function: Function = .{
            .context = context,
            .column_name = @tagName(column_tag),
            .alias = @tagName(context) ++ "__" ++ @tagName(column_tag),
        },

        pub fn as(comptime alias: []const u8) type {
            return struct {
                comptime __jetquery_function: Function = .{
                    .context = context,
                    .column_name = @tagName(column_tag),
                    .alias = alias,
                },
            };
        }
    };
}

pub inline fn min(comptime column_tag: anytype) type {
    return FunctionType(.min, column_tag);
}

pub inline fn max(comptime column_tag: anytype) type {
    return FunctionType(.max, column_tag);
}

pub inline fn count(comptime column_tag: anytype) type {
    return FunctionType(.count, column_tag);
}

pub inline fn avg(comptime column_tag: anytype) type {
    return FunctionType(.avg, column_tag);
}

pub inline fn sum(comptime column_tag: anytype) type {
    return FunctionType(.sum, column_tag);
}

pub inline fn raw(comptime slice: []const u8) type {
    return struct {
        pub const __jetquery_sql_string = slice;
    };
}

pub inline fn column(T: type, comptime sql: []const u8) jetquery.columns.Column {
    return jetquery.columns.Column{
        .name = undefined,
        .type = T,
        .table = undefined,
        .function = null,
        .alias = null,
        .sql = sql,
    };
}

pub fn translateOrderBy(
    Table: type,
    relations: []const type,
    comptime args: anytype,
) [orderBySize(@TypeOf(args))]jetquery.sql.OrderClause {
    comptime {
        switch (@typeInfo(@TypeOf(args))) {
            .enum_literal => return .{.{
                .column = Table.column(@tagName(args)),
                .direction = .ascending,
            }},
            .@"struct" => {},
            else => |tag| @compileError(
                std.fmt.comptimePrint(
                    "Unsupported `orderBy` argument: `{s}`. Expected [enum_literal, struct]",
                    .{@tagName(tag)},
                ),
            ),
        }
        var clauses: [orderBySize(@TypeOf(args))]jetquery.sql.OrderClause = undefined;
        const is_tuple = @typeInfo(@TypeOf(args)).@"struct".is_tuple;
        const fields = std.meta.fields(@TypeOf(args));

        var index: usize = 0;
        for (fields, if (is_tuple) args else fields) |field, arg| {
            if (is_tuple) {
                // Short-hand (default ascending):
                // orderBy(.{ .foo, .bar, .baz })
                clauses[index] = .{
                    .column = Table.column(@tagName(arg)),
                    .direction = .ascending,
                };
                index += 1;
                continue;
            } else if (@hasField(Table.Definition, field.name)) {
                // Explicit form:
                // orderBy(.{ .foo = .ascending })
                // orderBy(.{ .bar = .descending })
                clauses[index] = .{
                    .column = Table.column(field.name),
                    .direction = std.enums.nameCast(
                        jetquery.sql.OrderDirection,
                        @tagName(@field(args, field.name)),
                    ),
                };
                index += 1;
                continue;
            } else {
                // Nested form, ordering by relations fields:
                // orderBy(.{ .foo = .{ .bar })
                // orderBy(.{ .foo = .{ .bar = .descending } })
                relations: for (relations) |relation| {
                    if (std.mem.eql(u8, relation.relation_name, field.name)) {
                        const nested_clauses = translateOrderBy(
                            relation.Source,
                            &.{},
                            @field(args, field.name),
                        );
                        for (nested_clauses) |clause| {
                            clauses[index] = clause;
                            index += 1;
                        }
                        break :relations;
                    }
                } else {
                    @compileError(
                        std.fmt.comptimePrint(
                            "Unrecognized `orderBy` field `{s}` in current table and active joins/includes.",
                            .{field.name},
                        ),
                    );
                }
            }
        }
        return clauses;
    }
}

fn orderBySize(T: type) usize {
    const error_message = "Unsupported argument type for `orderBy`: `{s}`. Expected [enum_literal, struct]";

    return switch (@typeInfo(T)) {
        .enum_literal => 1,
        .@"struct" => blk: {
            var size: usize = 0;
            for (std.meta.fields(T)) |field| {
                size += switch (@typeInfo(field.type)) {
                    .enum_literal => 1,
                    .@"struct" => orderBySize(field.type),
                    else => |tag| @compileError(
                        std.fmt.comptimePrint(error_message, .{@tagName(tag)}),
                    ),
                };
            }
            break :blk size;
        },
        else => |tag| @compileError(
            std.fmt.comptimePrint(error_message, .{@tagName(tag)}),
        ),
    };
}
