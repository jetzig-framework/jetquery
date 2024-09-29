const std = @import("std");

const fields = @import("fields.zig");

pub const QueryContext = enum {
    select,
    update,
    insert,
    delete,
    delete_all,
    count,
    none,
};

pub fn OrderClause(Table: type) type {
    // TODO: Allow ordering on relations.
    return struct {
        column: std.meta.FieldEnum(Table.Definition),
        direction: OrderDirection,
    };
}

pub const OrderDirection = enum { ascending, descending };
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

pub fn render(
    Adapter: type,
    query_context: QueryContext,
    Table: type,
    relations: []const type,
    comptime field_infos: []const fields.FieldInfo,
    comptime columns: []const std.meta.FieldEnum(Table.Definition),
    comptime order_clauses: []const OrderClause(Table),
) []const u8 {
    return switch (query_context) {
        .select => renderSelect(Table, Adapter, relations, columns, field_infos, order_clauses),
        .update => renderUpdate(Table, Adapter, field_infos),
        .insert => renderInsert(Table, Adapter, field_infos),
        .delete, .delete_all => renderDelete(Table, Adapter, field_infos, query_context),
        .count => renderCount(Table, Adapter, relations, field_infos),
        .none => "",
    };
}

fn renderSelect(
    Table: type,
    Adapter: type,
    relations: []const type,
    comptime columns: []const std.meta.FieldEnum(Table.Definition),
    comptime field_infos: []const fields.FieldInfo,
    comptime order_clauses: []const OrderClause(Table),
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
    comptime field_infos: []const fields.FieldInfo,
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
    comptime field_infos: []const fields.FieldInfo,
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
    comptime field_infos: []const fields.FieldInfo,
    comptime query_context: QueryContext,
) []const u8 {
    const statement = std.fmt.comptimePrint("DELETE FROM {s}", .{Adapter.identifier(Table.table_name)});
    return switch (query_context) {
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

fn renderCount(
    Table: type,
    Adapter: type,
    comptime relations: []const type,
    comptime field_infos: []const fields.FieldInfo,
) []const u8 {
    comptime {
        const count_column = " " ++ Adapter.countSql();
        const from = std.fmt.comptimePrint(" FROM {s}", .{Adapter.identifier(Table.table_name)});
        const joins = renderJoins(Adapter, Table, relations);

        return std.fmt.comptimePrint(
            "SELECT{s}{s}{s}{s}{s}",
            .{
                count_column,
                from,
                joins,
                renderWhere(Adapter, Table, field_infos),
                renderLimit(Adapter, field_infos),
            },
        );
    }
}

fn renderLimit(
    Adapter: type,
    comptime field_infos: []const fields.FieldInfo,
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
    comptime order_clauses: []const OrderClause(Table),
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
    comptime field_infos: []const fields.FieldInfo,
) []const u8 {
    if (!hasParam(field_infos, .where)) return "";

    var buf: [paramsBufSize(Adapter, Table, field_infos, .where, .assign)]u8 = undefined;
    const params = renderParams(&buf, Adapter, Table, field_infos, .where, .assign);

    return std.fmt.comptimePrint(" WHERE {s}", .{params});
}

fn renderJoins(Adapter: type, Table: type, relations: []const type) []const u8 {
    comptime {
        if (relations.len == 0) return "";

        var buf_len: usize = 0;
        for (relations) |Relation| {
            buf_len += switch (Relation.relation_type) {
                .belongs_to => renderInnerJoin(Adapter, Table, Relation).len,
                .has_many => renderInnerJoin(Adapter, Table, Relation).len,
            };
        }
        var buf: [buf_len]u8 = undefined;
        var cursor: usize = 0;
        for (relations) |Relation| {
            const sql = switch (Relation.relation_type) {
                .belongs_to => renderInnerJoin(Adapter, Table, Relation),
                .has_many => renderInnerJoin(Adapter, Table, Relation),
            };
            @memcpy(buf[cursor .. cursor + sql.len], sql);
            cursor += sql.len;
        }

        return &buf;
    }
}

fn renderInnerJoin(Adapter: type, Table: type, Relation: type) []const u8 {
    const PrimaryKey = std.meta.FieldEnum(Table.Definition);
    const ForeignKey = std.meta.FieldEnum(Table.Definition);

    const primary_key: PrimaryKey = std.enums.nameCast(
        PrimaryKey,
        Relation.options.primary_key orelse "id",
    );

    const foreign_key: ForeignKey = std.enums.nameCast(
        ForeignKey,
        Relation.options.primary_key orelse Relation.relation_name ++ "_id",
    );

    return Adapter.innerJoinSql(
        Table,
        Relation.Source,
        Relation.relation_name,
        .{ .primary_key = @tagName(primary_key), .foreign_key = @tagName(foreign_key) },
    );
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
            }
            start += Relation.select_columns.len;
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
            }
            start += Relation.select_columns.len;
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
    comptime field_infos: []const fields.FieldInfo,
    comptime context: fields.FieldContext,
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
    comptime field_infos: []const fields.FieldInfo,
    comptime context: fields.FieldContext,
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
    comptime field_infos: []const fields.FieldInfo,
    comptime index: usize,
    comptime context: fields.FieldContext,
) bool {
    return if (field_infos.len > index)
        field_infos[index].context == context
    else
        false;
}

fn lastParamIndex(
    comptime field_infos: []const fields.FieldInfo,
    comptime context: fields.FieldContext,
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
    comptime field_infos: []const fields.FieldInfo,
    comptime context: fields.FieldContext,
) bool {
    for (field_infos) |field| {
        if (field.context == context) return true;
    }
    return false;
}
