const std = @import("std");

const jetquery = @import("../jetquery.zig");

pub fn render(
    Adapter: type,
    query_type: jetquery.QueryType,
    Table: type,
    relations: []const type,
    comptime field_infos: []const jetquery.FieldInfo,
    comptime columns: []const std.meta.FieldEnum(Table.Definition),
    comptime order_clauses: []const jetquery.OrderClause(Table),
) []const u8 {
    return switch (query_type) {
        .select => renderSelect(Table, Adapter, relations, columns, field_infos, order_clauses),
        .update => renderUpdate(Table, Adapter, field_infos),
        .insert => renderInsert(Table, Adapter, field_infos),
        .delete, .delete_all => renderDelete(Table, Adapter, field_infos, query_type),
    };
}

fn renderSelect(
    Table: type,
    Adapter: type,
    relations: []const type,
    comptime columns: []const std.meta.FieldEnum(Table.Definition),
    comptime field_infos: []const jetquery.FieldInfo,
    comptime order_clauses: []const jetquery.OrderClause(Table),
) []const u8 {
    comptime {
        const select_columns = renderSelectColumns(Adapter, Table, relations, columns);
        const from = std.fmt.comptimePrint(" FROM {s}", .{Adapter.identifier(Table.table_name)});
        const joins = renderJoins(Adapter, Table, relations);

        return std.fmt.comptimePrint(
            "SELECT{s}{s}{s}{s}{s}{s}",
            .{
                select_columns,
                from,
                joins,
                renderWhere(Adapter, Table, field_infos),
                renderOrder(Table, Adapter, order_clauses),
                renderLimit(Adapter, field_infos),
            },
        );
    }
}

fn renderUpdate(
    Table: type,
    Adapter: type,
    comptime field_infos: []const jetquery.FieldInfo,
) []const u8 {
    var buf: [paramsBufSize(Adapter, Table, field_infos, .update, .assign)]u8 = undefined;
    return std.fmt.comptimePrint(
        "UPDATE {s} SET {s}{s}",
        .{
            Adapter.identifier(Table.table_name),
            renderParams(&buf, Adapter, Table, field_infos, .update, .assign),
            renderWhere(Adapter, Table, field_infos),
        },
    );
}

fn renderInsert(
    Table: type,
    Adapter: type,
    comptime field_infos: []const jetquery.FieldInfo,
) []const u8 {
    var params_buf: [paramsBufSize(Adapter, Table, field_infos, .insert, .column)]u8 = undefined;
    var values_buf: [paramsBufSize(Adapter, Table, field_infos, .insert, .value)]u8 = undefined;
    return std.fmt.comptimePrint(
        "INSERT INTO {s} ({s}) VALUES ({s})",
        .{
            Adapter.identifier(Table.table_name),
            renderParams(&params_buf, Adapter, Table, field_infos, .insert, .column),
            renderParams(&values_buf, Adapter, Table, field_infos, .insert, .value),
        },
    );
}

fn renderDelete(
    Table: type,
    Adapter: type,
    comptime field_infos: []const jetquery.FieldInfo,
    query_type: jetquery.QueryType,
) []const u8 {
    const statement = std.fmt.comptimePrint("DELETE FROM {s}", .{Adapter.identifier(Table.table_name)});
    return switch (query_type) {
        .delete_all => statement,
        .delete => std.fmt.comptimePrint("{s}{s}{s}", .{
            statement,
            renderWhere(Adapter, Table, field_infos),
            renderLimit(Adapter, field_infos),
        }),
        else => |tag| @compileError(
            "Inconsistent query for DELETE: `" ++ @tagName(tag) ++ "` (this is a bug)",
        ),
    };
}

fn renderLimit(
    Adapter: type,
    comptime field_infos: []const jetquery.FieldInfo,
) []const u8 {
    if (!hasParam(field_infos, .limit)) return "";

    return std.fmt.comptimePrint(
        " LIMIT {s}",
        .{Adapter.paramSql(lastParamIndex(field_infos, .limit))},
    );
}

fn renderOrder(
    Table: type,
    Adapter: type,
    comptime order_clauses: []const jetquery.OrderClause(Table),
) []const u8 {
    if (order_clauses.len == 0) return "";

    var size: usize = 0;
    for (order_clauses, 0..) |order_clause, index| {
        const separator = if (index + 1 < order_clauses.len) ", " else "";
        size += (Adapter.orderSql(Table, order_clause) ++ separator).len;
        if (index + 1 < order_clauses.len) size += ", ".len;
    }
    var order_buf: [size]u8 = undefined;
    var cursor: usize = 0;
    for (order_clauses, 0..) |order_clause, index| {
        const separator = if (index + 1 < order_clauses.len) ", " else "";
        const order_sql = Adapter.orderSql(Table, order_clause) ++ separator;
        @memcpy(order_buf[cursor .. cursor + order_sql.len], order_sql);
        cursor += order_sql.len;
    }
    return std.fmt.comptimePrint(
        " ORDER BY {s}",
        .{order_buf},
    );
}

fn renderWhere(
    Adapter: type,
    Table: type,
    comptime field_infos: []const jetquery.FieldInfo,
) []const u8 {
    if (!hasParam(field_infos, .where)) return "";

    var buf: [paramsBufSize(Adapter, Table, field_infos, .where, .assign)]u8 = undefined;
    const params = renderParams(&buf, Adapter, Table, field_infos, .where, .assign);

    return std.fmt.comptimePrint(" WHERE {s}", .{params});
}

pub const JoinOptions = struct {
    // TODO: Use field enums
    left: ?[]const u8 = null,
    right: ?[]const u8 = null,
};

fn renderJoins(Adapter: type, Table: type, relations: []const type) []const u8 {
    comptime {
        if (relations.len == 0) return "";

        var buf_len: usize = 0;
        for (relations) |relation| {
            buf_len += Adapter.innerJoinSql(
                Table,
                relation.Source,
                relation.relation_name,
                .{ .left = null, .right = null }, // TODO: `ON` condition from options
            ).len;
        }
        var buf: [buf_len]u8 = undefined;
        var cursor: usize = 0;
        for (relations) |relation| {
            const sql = Adapter.innerJoinSql(
                Table,
                relation.Source,
                relation.relation_name,
                .{ .left = null, .right = null }, // TODO: `ON` condition from options
            );
            @memcpy(buf[cursor .. cursor + sql.len], sql);
            cursor += sql.len;
        }

        return &buf;
    }
}

fn renderSelectColumns(
    Adapter: type,
    Table: type,
    relations: []const type,
    comptime columns: []const std.meta.FieldEnum(Table.Definition),
) []const u8 {
    comptime {
        var total_columns: usize = columns.len;
        for (relations) |Relation| {
            total_columns += Relation.select_columns.len;
        }

        var columns_buf_len: usize = 0;
        for (columns, 0..) |column, index| {
            columns_buf_len += renderSelectColumn(Adapter, Table, @tagName(column), index, total_columns).len;
        }

        var start = columns.len;
        for (relations) |Relation| {
            for (Relation.select_columns, start..) |column, index| {
                columns_buf_len += renderSelectColumn(
                    Adapter,
                    Relation.Source,
                    @tagName(column),
                    index,
                    total_columns,
                ).len;
                start += Relation.select_columns.len;
            }
        }

        var columns_buf: [columns_buf_len]u8 = undefined;
        var cursor: usize = 0;
        for (columns, 0..) |column, index| {
            const column_identifier = renderSelectColumn(
                Adapter,
                Table,
                @tagName(column),
                index,
                total_columns,
            );
            @memcpy(columns_buf[cursor .. cursor + column_identifier.len], column_identifier);
            cursor += column_identifier.len;
        }

        start = columns.len;
        for (relations) |Relation| {
            for (Relation.select_columns, start..) |column, index| {
                const column_identifier = renderSelectColumn(
                    Adapter,
                    Relation.Source,
                    @tagName(column),
                    index,
                    total_columns,
                );
                @memcpy(columns_buf[cursor .. cursor + column_identifier.len], column_identifier);
                cursor += column_identifier.len;
                start += Relation.select_columns.len;
            }
        }
        return &columns_buf;
    }
}

fn renderSelectColumn(
    Adapter: type,
    Table: type,
    comptime name: []const u8,
    comptime index: usize,
    comptime total: usize,
) []const u8 {
    comptime {
        return std.fmt.comptimePrint(
            " {s}{s}",
            .{ Adapter.columnSql(Table, name), if (index + 1 < total) "," else "" },
        );
    }
}

fn paramsBufSize(
    Adapter: type,
    Table: type,
    comptime field_infos: []const jetquery.FieldInfo,
    comptime context: jetquery.FieldContext,
    comptime format: enum { column, value, assign },
) usize {
    var buf_len: usize = 0;

    const separator = switch (context) {
        .where => " AND ",
        .update, .insert => ", ",
        else => @compileError("Unsupported param type: `" ++ @tagName(context) ++ "`"),
    };

    const last_param_index = lastParamIndex(field_infos, context);

    const template = switch (format) {
        .column, .value => "{s}{s}",
        .assign => "{s} = {s}{s}",
    };

    for (field_infos, 0..) |field, index| {
        if (!fieldContext(field_infos, index, context)) continue;

        const args = switch (format) {
            .column => .{
                switch (context) {
                    .insert => Adapter.identifier(field.name),
                    else => Adapter.columnSql(Table, field.name),
                },
                if (index < last_param_index) separator else "",
            },
            .value => .{
                Adapter.paramSql(index),
                if (index < last_param_index) separator else "",
            },
            .assign => .{
                Adapter.columnSql(Table, field.name),
                Adapter.paramSql(index),
                if (index < last_param_index) separator else "",
            },
        };

        buf_len += std.fmt.count(template, args);
    }
    return buf_len;
}

fn renderParams(
    buf: []u8,
    Adapter: type,
    Table: type,
    comptime field_infos: []const jetquery.FieldInfo,
    comptime context: jetquery.FieldContext,
    comptime format: enum { column, value, assign },
) []const u8 {
    if (!hasParam(field_infos, context)) @compileError("Failed compiling UPDATE query with empty params.");

    const separator = switch (context) {
        .where => " AND ",
        .update, .insert => ", ",
        else => @compileError("Unsupported param type: `" ++ @tagName(context) ++ "`"),
    };

    const last_param_index = lastParamIndex(field_infos, context);
    var cursor: usize = 0;

    const template = switch (format) {
        .column, .value => "{s}{s}",
        .assign => "{s} = {s}{s}",
    };

    for (field_infos, 0..) |field, index| {
        if (!fieldContext(field_infos, index, context)) continue;

        const args = switch (format) {
            .column => .{
                switch (context) {
                    .insert => Adapter.identifier(field.name),
                    else => Adapter.columnSql(Table, field.name),
                },
                if (index < last_param_index) separator else "",
            },
            .value => .{
                Adapter.paramSql(index),
                if (index < last_param_index) separator else "",
            },
            .assign => .{
                Adapter.columnSql(Table, field.name),
                Adapter.paramSql(index),
                if (index < last_param_index) separator else "",
            },
        };

        const param = std.fmt.comptimePrint(template, args);
        @memcpy(buf[cursor .. cursor + param.len], param);
        cursor += param.len;
    }
    return buf;
}

fn fieldContext(
    comptime field_infos: []const jetquery.FieldInfo,
    comptime index: usize,
    comptime context: jetquery.FieldContext,
) bool {
    return if (field_infos.len > index)
        field_infos[index].context == context
    else
        false;
}

fn lastParamIndex(
    comptime field_infos: []const jetquery.FieldInfo,
    comptime context: jetquery.FieldContext,
) usize {
    var maybe_index: ?usize = null;

    for (field_infos, 0..) |field, index| {
        if (field.context == context) maybe_index = index;
    }
    if (maybe_index) |index|
        return index
    else
        @compileError("No param matched for `" ++ @tagName(context) ++ "`");
}

fn hasParam(
    comptime field_infos: []const jetquery.FieldInfo,
    comptime context: jetquery.FieldContext,
) bool {
    for (field_infos) |field| {
        if (field.context == context) return true;
    }
    return false;
}
