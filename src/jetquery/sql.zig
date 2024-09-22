const std = @import("std");

const jetquery = @import("../jetquery.zig");

pub fn render(
    query_type: jetquery.QueryType,
    Table: type,
    comptime Adapter: type,
    comptime field_infos: []const jetquery.FieldInfo,
    comptime columns: []const std.meta.FieldEnum(Table.Definition),
    comptime order_clauses: []const jetquery.OrderClause(Table),
) []const u8 {
    return switch (query_type) {
        .select => renderSelect(Table, Adapter, columns, field_infos, order_clauses),
        .update => renderUpdate(Table, Adapter, field_infos),
        .insert => renderInsert(Table, Adapter, field_infos),
        .delete, .delete_all => renderDelete(Table, Adapter, field_infos, query_type),
    };
}

fn renderSelect(
    Table: type,
    comptime Adapter: type,
    comptime columns: []const std.meta.FieldEnum(Table.Definition),
    comptime field_infos: []const jetquery.FieldInfo,
    comptime order_clauses: []const jetquery.OrderClause(Table),
) []const u8 {
    comptime {
        var columns_buf_len: usize = 0;
        for (columns, 0..) |column, index| {
            columns_buf_len += std.fmt.comptimePrint(
                " {s}{s}",
                .{ Adapter.identifier(@tagName(column)), if (index + 1 < columns.len) "," else "" },
            ).len;
        }
        var columns_buf: [columns_buf_len]u8 = undefined;
        var cursor: usize = 0;
        for (columns, 0..) |column, index| {
            const column_identifier = std.fmt.comptimePrint(
                " {s}{s}",
                .{ Adapter.identifier(@tagName(column)), if (index + 1 < columns.len) "," else "" },
            );
            @memcpy(columns_buf[cursor .. cursor + column_identifier.len], column_identifier);
            cursor += column_identifier.len;
        }
        const from = std.fmt.comptimePrint(" FROM {s}", .{Adapter.identifier(Table.table_name)});
        return std.fmt.comptimePrint(
            "SELECT{s}{s}{s}{s}{s}",
            .{
                columns_buf,
                from,
                renderWhere(Adapter, field_infos),
                renderOrder(Table, Adapter, order_clauses),
                renderLimit(Adapter, field_infos),
            },
        );
    }
}

fn renderUpdate(
    Table: type,
    comptime Adapter: type,
    comptime field_infos: []const jetquery.FieldInfo,
) []const u8 {
    var buf: [paramsBufSize(Adapter, field_infos, .update, .assign)]u8 = undefined;
    return std.fmt.comptimePrint(
        "UPDATE {s} SET {s}{s}",
        .{
            Adapter.identifier(Table.table_name),
            renderParams(&buf, Adapter, field_infos, .update, .assign),
            renderWhere(Adapter, field_infos),
        },
    );
}

fn renderInsert(
    Table: type,
    comptime Adapter: type,
    comptime field_infos: []const jetquery.FieldInfo,
) []const u8 {
    var params_buf: [paramsBufSize(Adapter, field_infos, .insert, .column)]u8 = undefined;
    var values_buf: [paramsBufSize(Adapter, field_infos, .insert, .value)]u8 = undefined;
    return std.fmt.comptimePrint(
        "INSERT INTO {s} ({s}) VALUES ({s})",
        .{
            Adapter.identifier(Table.table_name),
            renderParams(&params_buf, Adapter, field_infos, .insert, .column),
            renderParams(&values_buf, Adapter, field_infos, .insert, .value),
        },
    );
}

fn renderDelete(
    Table: type,
    comptime Adapter: type,
    comptime field_infos: []const jetquery.FieldInfo,
    query_type: jetquery.QueryType,
) []const u8 {
    const statement = std.fmt.comptimePrint("DELETE FROM {s}", .{Adapter.identifier(Table.table_name)});
    return switch (query_type) {
        .delete_all => statement,
        .delete => std.fmt.comptimePrint("{s}{s}{s}", .{
            statement,
            renderWhere(Adapter, field_infos),
            renderLimit(Adapter, field_infos),
        }),
        else => |tag| @compileError(
            "Inconsistent query for DELETE: `" ++ @tagName(tag) ++ "` (this is a bug)",
        ),
    };
}

fn renderLimit(
    comptime Adapter: type,
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
    comptime Adapter: type,
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
    comptime Adapter: type,
    comptime field_infos: []const jetquery.FieldInfo,
) []const u8 {
    if (!hasParam(field_infos, .where)) return "";

    var buf: [paramsBufSize(Adapter, field_infos, .where, .assign)]u8 = undefined;
    const params = renderParams(&buf, Adapter, field_infos, .where, .assign);

    return std.fmt.comptimePrint(" WHERE {s}", .{params});
}

fn paramsBufSize(
    comptime Adapter: type,
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
                Adapter.identifier(field.name),
                if (index < last_param_index) separator else "",
            },
            .value => .{
                Adapter.paramSql(index),
                if (index < last_param_index) separator else "",
            },
            .assign => .{
                Adapter.identifier(field.name),
                Adapter.paramSql(index),
                if (index < last_param_index) separator else "",
            },
        };

        buf_len += std.fmt.comptimePrint(
            template,
            args,
        ).len;
    }
    return buf_len;
}

fn renderParams(
    buf: []u8,
    comptime Adapter: type,
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
                Adapter.identifier(field.name),
                if (index < last_param_index) separator else "",
            },
            .value => .{
                Adapter.paramSql(index),
                if (index < last_param_index) separator else "",
            },
            .assign => .{
                Adapter.identifier(field.name),
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
