const std = @import("std");

const jetquery = @import("../jetquery.zig");
const query = @import("Query.zig");

pub fn render(
    query_type: query.QueryType,
    Table: type,
    comptime adapter: jetquery.adapters.Adapter,
    comptime columns: []const std.meta.FieldEnum(Table.Definition),
    comptime field_infos: []const query.FieldInfo,
) []const u8 {
    return switch (query_type) {
        .select => renderSelect(Table, adapter, columns, field_infos),
        .update => renderUpdate(Table, adapter, field_infos),
        .insert => renderInsert(Table, adapter, field_infos),
        .delete, .delete_all => renderDelete(Table, adapter, field_infos, query_type),
    };
}

fn renderSelect(
    Table: type,
    comptime adapter: jetquery.adapters.Adapter,
    comptime columns: []const std.meta.FieldEnum(Table.Definition),
    comptime field_infos: []const query.FieldInfo,
) []const u8 {
    comptime {
        var columns_buf_len: usize = 0;
        for (columns, 0..) |column, index| {
            columns_buf_len += std.fmt.comptimePrint(
                " {}{s}",
                .{ adapter.identifier(@tagName(column)), if (index + 1 < columns.len) "," else "" },
            ).len;
        }
        var columns_buf: [columns_buf_len]u8 = undefined;
        var cursor: usize = 0;
        for (columns, 0..) |column, index| {
            const column_identifier = std.fmt.comptimePrint(
                " {}{s}",
                .{ adapter.identifier(@tagName(column)), if (index + 1 < columns.len) "," else "" },
            );
            @memcpy(columns_buf[cursor .. cursor + column_identifier.len], column_identifier);
            cursor += column_identifier.len;
        }
        const from = std.fmt.comptimePrint(" FROM {}", .{adapter.identifier(Table.table_name)});
        return std.fmt.comptimePrint(
            "SELECT{s}{s}{s}{s}",
            .{
                columns_buf,
                from,
                renderWhere(adapter, field_infos),
                renderLimit(adapter, field_infos),
            },
        );
    }
}

fn renderUpdate(
    Table: type,
    comptime adapter: jetquery.adapters.Adapter,
    comptime field_infos: []const query.FieldInfo,
) []const u8 {
    var buf: [paramsBufSize(adapter, field_infos, .update, .assign)]u8 = undefined;
    return std.fmt.comptimePrint(
        "UPDATE {} SET {s}{s}",
        .{
            adapter.identifier(Table.table_name),
            renderParams(&buf, adapter, field_infos, .update, .assign),
            renderWhere(adapter, field_infos),
        },
    );
}

fn renderInsert(
    Table: type,
    comptime adapter: jetquery.adapters.Adapter,
    comptime field_infos: []const query.FieldInfo,
) []const u8 {
    var params_buf: [paramsBufSize(adapter, field_infos, .insert, .column)]u8 = undefined;
    var values_buf: [paramsBufSize(adapter, field_infos, .insert, .value)]u8 = undefined;
    return std.fmt.comptimePrint(
        "INSERT INTO {} ({s}) VALUES ({s})",
        .{
            adapter.identifier(Table.table_name),
            renderParams(&params_buf, adapter, field_infos, .insert, .column),
            renderParams(&values_buf, adapter, field_infos, .insert, .value),
        },
    );
}

fn renderDelete(
    Table: type,
    comptime adapter: jetquery.adapters.Adapter,
    comptime field_infos: []const query.FieldInfo,
    query_type: query.QueryType,
) []const u8 {
    const statement = std.fmt.comptimePrint("DELETE FROM {s}", .{adapter.identifier(Table.table_name)});
    return switch (query_type) {
        .delete_all => statement,
        .delete => std.fmt.comptimePrint("{s}{s}{s}", .{
            statement,
            renderWhere(adapter, field_infos),
            renderLimit(adapter, field_infos),
        }),
        else => @compileError("Inconsistent query for DELETE (this is a bug)"),
    };
}

fn renderLimit(
    comptime adapter: jetquery.adapters.Adapter,
    comptime field_infos: []const query.FieldInfo,
) []const u8 {
    if (!hasParam(field_infos, .limit)) return "";

    return std.fmt.comptimePrint(
        " LIMIT {s}",
        .{adapter.paramSqlC(lastParamIndex(field_infos, .limit))},
    );
}

fn renderWhere(
    comptime adapter: jetquery.adapters.Adapter,
    comptime field_infos: []const query.FieldInfo,
) []const u8 {
    if (!hasParam(field_infos, .where)) return "";

    var buf: [paramsBufSize(adapter, field_infos, .where, .assign)]u8 = undefined;
    const params = renderParams(&buf, adapter, field_infos, .where, .assign);

    return std.fmt.comptimePrint(" WHERE {s}", .{params});
}

fn paramsBufSize(
    comptime adapter: jetquery.adapters.Adapter,
    comptime field_infos: []const query.FieldInfo,
    comptime context: query.FieldContext,
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
                adapter.identifier(field.name),
                if (index < last_param_index) separator else "",
            },
            .value => .{
                adapter.paramSqlC(index),
                if (index < last_param_index) separator else "",
            },
            .assign => .{
                adapter.identifier(field.name),
                adapter.paramSqlC(index),
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
    comptime adapter: jetquery.adapters.Adapter,
    comptime field_infos: []const query.FieldInfo,
    comptime context: query.FieldContext,
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
                adapter.identifier(field.name),
                if (index < last_param_index) separator else "",
            },
            .value => .{
                adapter.paramSqlC(index),
                if (index < last_param_index) separator else "",
            },
            .assign => .{
                adapter.identifier(field.name),
                adapter.paramSqlC(index),
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
    comptime field_infos: []const query.FieldInfo,
    comptime index: usize,
    comptime context: query.FieldContext,
) bool {
    return if (field_infos.len > index)
        field_infos[index].context == context
    else
        false;
}

fn lastParamIndex(
    comptime field_infos: []const query.FieldInfo,
    comptime context: query.FieldContext,
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
    comptime field_infos: []const query.FieldInfo,
    comptime context: query.FieldContext,
) bool {
    for (field_infos) |field| {
        if (field.context == context) return true;
    }
    return false;
}
