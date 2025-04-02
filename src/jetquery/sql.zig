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
    insert,
    update,
    update_all,
    delete,
    delete_all,
    count,
    returning,
    none,
};

pub const OrderClause = struct {
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

    const column_name = @tagName(column_tag);
    const default_alias = @tagName(context) ++ "__" ++ @tagName(column_tag);

    return struct {
        comptime __jetquery_function: Function = .{
            .context = context,
            .column_name = @tagName(column_tag),
            .alias = default_alias,
        },

        pub fn as(comptime alias: []const u8) type {
            return struct {
                comptime __jetquery_function: Function = .{
                    .context = context,
                    .column_name = column_name,
                    .alias = alias,
                },
            };
        }

        pub fn toColumn(Model: type) jetquery.columns.Column {
            var base_column = jetquery.columns.primaryColumn(Model, column_name);
            base_column.function = context;
            base_column.alias = default_alias;
            return base_column;
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
            // Single-argument short-hand column name:
            // orderBy(.foo)
            .enum_literal => return .{.{
                .column = Table.column(@tagName(args)),
                .direction = .ascending,
            }},
            // Single-argument short-hand with column object:
            // orderBy(jetquery.sql.max(.foo))
            .type => return .{.{
                .column = args.toColumn(Table),
                .direction = .ascending,
            }},
            // A tuple or struct - iterate further below
            .@"struct" => {},
            else => |tag| @compileError(
                std.fmt.comptimePrint(
                    "Unsupported `orderBy` argument: `{s}`. Expected [enum_literal, struct, Column]",
                    .{@tagName(tag)},
                ),
            ),
        }
        var clauses: [orderBySize(@TypeOf(args))]jetquery.sql.OrderClause = undefined;
        const is_tuple = switch (@typeInfo(@TypeOf(args))) {
            .@"struct" => |info| info.is_tuple,
            else => false,
        };
        const fields = std.meta.fields(@TypeOf(args));

        var index: usize = 0;
        for (fields, if (is_tuple) args else fields) |field, arg| {
            if (is_tuple) {
                // Short-hand (default ascending):
                // orderBy(.{ .foo, .bar, .baz })
                const err = "Expected [enum_literal, Column], found: " ++ @typeName(@TypeOf(arg));
                const col = switch (@typeInfo(@TypeOf(arg))) {
                    .enum_literal => Table.column(@tagName(arg)),
                    .type => if (@hasDecl(arg, "toColumn"))
                        arg.toColumn(Table)
                    else
                        @compileError(err),
                    else => @compileError(err),
                };
                clauses[index] = .{
                    .column = col,
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
            } else if (arg.type == type and @hasDecl(@field(args, field.name), "toColumn")) {
                // Explicit form with arbitrary name + Column:
                // orderBy(.{ .any_name_here = jetquery.sql.max(.foo) })
                clauses[index] = .{
                    .column = @field(args, field.name).toColumn(Table),
                    .direction = .ascending,
                };
                index += 1;
                continue;
            } else if (isOrderByCouplet(@TypeOf(@field(args, field.name)))) {
                // Explicit form with arbitrary name + Column in couplet form:
                // orderBy(.{ .any_name_here = .{ jetquery.sql.max(.foo), .desc } })
                clauses[index] = .{
                    .column = @field(args, field.name).@"0".toColumn(Table),
                    .direction = @field(args, field.name).@"1",
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
                            clauses[index].column.from = relation.relation_name;
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

fn isOrderByCouplet(T: type) bool {
    // A tuple of .{ Column, OrderDirection }
    // e.g.:
    // .{ sql.max(.foo), .desc }
    const arg: T = undefined;
    return switch (@typeInfo(T)) {
        .@"struct" => |info| info.is_tuple and
            info.fields.len == 2 and
            info.fields[0].type == type and
            info.fields[0].is_comptime and
            @hasDecl(arg.@"0", "toColumn") and
            @typeInfo(info.fields[1].type) == .enum_literal,
        else => false,
    };
}

fn orderBySize(T: type) usize {
    const error_message = "Unsupported argument type for `orderBy`: `{s}`. Expected [enum_literal, struct]";

    return switch (@typeInfo(T)) {
        .enum_literal, .type => 1,
        .@"struct" => blk: {
            var size: usize = 0;
            for (std.meta.fields(T)) |field| {
                size += switch (@typeInfo(field.type)) {
                    .enum_literal, .type => 1,
                    .@"struct" => if (isOrderByCouplet(field.type))
                        1
                    else
                        orderBySize(field.type),
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
