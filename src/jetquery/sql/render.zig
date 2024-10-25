const std = @import("std");

const jetquery = @import("../../jetquery.zig");

pub fn render(
    Adapter: type,
    query_context: jetquery.sql.QueryContext,
    Table: type,
    relations: []const type,
    comptime field_infos: []const jetquery.fields.FieldInfo,
    comptime columns: []const jetquery.columns.Column,
    comptime order_clauses: []const jetquery.sql.OrderClause,
    comptime distinct: ?[]const jetquery.columns.Column,
    comptime where_clauses: []const jetquery.sql.Where.Tree,
    comptime group_by: ?[]const jetquery.columns.Column,
    comptime having_clauses: []const jetquery.sql.Where.Tree,
) []const u8 {
    return switch (query_context) {
        .select => renderSelect(
            Adapter,
            Table,
            relations,
            columns,
            field_infos,
            order_clauses,
            where_clauses,
            group_by,
            having_clauses,
        ),
        .update => renderUpdate(
            Adapter,
            Table,
            where_clauses,
            field_infos,
        ),
        .insert => renderInsert(
            Adapter,
            Table,
            field_infos,
        ),
        .delete, .delete_all => renderDelete(
            Adapter,
            Table,
            field_infos,
            where_clauses,
            query_context,
        ),
        .count => renderCount(
            Adapter,
            Table,
            relations,
            field_infos,
            where_clauses,
            distinct,
        ),
        .none => "",
    };
}

fn renderSelect(
    Adapter: type,
    Table: type,
    relations: []const type,
    comptime columns: []const jetquery.columns.Column,
    comptime field_infos: []const jetquery.fields.FieldInfo,
    comptime order_clauses: []const jetquery.sql.OrderClause,
    comptime where_clauses: []const jetquery.sql.Where.Tree,
    comptime group_by: ?[]const jetquery.columns.Column,
    comptime having_clauses: []const jetquery.sql.Where.Tree,
) []const u8 {
    comptime {
        const select_columns = renderSelectColumns(Adapter, relations, columns);
        const from = std.fmt.comptimePrint(" FROM {s}", .{Adapter.identifier(Table.name)});
        const joins = renderJoins(Adapter, Table, relations);

        return std.fmt.comptimePrint(
            "SELECT{s}{s}{s}{s}{s}{s}{s}",
            .{
                select_columns,
                from,
                joins,
                renderWhere(Adapter, where_clauses),
                renderGroupBy(Adapter, group_by, having_clauses),
                renderOrder(Adapter, order_clauses),
                renderLimit(Adapter, field_infos),
            },
        );
    }
}

fn renderUpdate(
    Adapter: type,
    Table: type,
    where_clauses: []const jetquery.sql.Where.Tree,
    comptime field_infos: []const jetquery.fields.FieldInfo,
) []const u8 {
    var buf: [paramsBufSize(Adapter, field_infos, .update, .assign)]u8 = undefined;
    return std.fmt.comptimePrint(
        "UPDATE {s} SET {s}{s}",
        .{
            Adapter.identifier(Table.name),
            renderParams(&buf, Adapter, field_infos, .update, .assign),
            renderWhere(Adapter, where_clauses),
        },
    );
}

fn renderInsert(
    Adapter: type,
    Table: type,
    comptime field_infos: []const jetquery.fields.FieldInfo,
) []const u8 {
    var params_buf: [paramsBufSize(Adapter, field_infos, .insert, .column)]u8 = undefined;
    var values_buf: [paramsBufSize(Adapter, field_infos, .insert, .value)]u8 = undefined;
    return std.fmt.comptimePrint(
        "INSERT INTO {s} ({s}) VALUES ({s})",
        .{
            Adapter.identifier(Table.name),
            renderParams(&params_buf, Adapter, field_infos, .insert, .column),
            renderParams(&values_buf, Adapter, field_infos, .insert, .value),
        },
    );
}

fn renderDelete(
    Adapter: type,
    Table: type,
    comptime field_infos: []const jetquery.fields.FieldInfo,
    comptime where_clauses: []const jetquery.sql.Where.Tree,
    comptime query_context: jetquery.sql.QueryContext,
) []const u8 {
    const statement = std.fmt.comptimePrint("DELETE FROM {s}", .{Adapter.identifier(Table.name)});
    return switch (query_context) {
        .delete, .delete_all => std.fmt.comptimePrint("{s}{s}{s}", .{
            statement,
            renderWhere(Adapter, where_clauses),
            renderLimit(Adapter, field_infos),
        }),
        else => |tag| @compileError(
            "Inconsistent query for DELETE: `" ++ @tagName(tag) ++ "` (this is a bug)",
        ),
    };
}

fn renderCount(
    Adapter: type,
    Table: type,
    comptime relations: []const type,
    comptime field_infos: []const jetquery.fields.FieldInfo,
    comptime where_clauses: []const jetquery.sql.Where.Tree,
    comptime distinct: ?[]const jetquery.columns.Column,
) []const u8 {
    comptime {
        const count_column = " " ++ Adapter.countSql(distinct);
        const from = std.fmt.comptimePrint(" FROM {s}", .{Adapter.identifier(Table.name)});
        const joins = renderJoins(Adapter, Table, relations);

        return std.fmt.comptimePrint(
            "SELECT{s}{s}{s}{s}{s}",
            .{
                count_column,
                from,
                joins,
                renderWhere(Adapter, where_clauses),
                renderLimit(Adapter, field_infos),
            },
        );
    }
}

fn renderLimit(
    Adapter: type,
    comptime field_infos: []const jetquery.fields.FieldInfo,
) []const u8 {
    if (!hasParam(field_infos, .limit)) return "";

    const offset = renderOffset(Adapter, field_infos);

    return std.fmt.comptimePrint(
        " LIMIT {s}{s}",
        .{ Adapter.paramSql(lastParamIndex(field_infos, .limit)), offset },
    );
}

fn renderOffset(
    Adapter: type,
    comptime field_infos: []const jetquery.fields.FieldInfo,
) []const u8 {
    if (!hasParam(field_infos, .offset)) return "";

    return std.fmt.comptimePrint(
        " OFFSET {s}",
        .{Adapter.paramSql(lastParamIndex(field_infos, .offset))},
    );
}
fn renderOrder(
    Adapter: type,
    comptime order_clauses: []const jetquery.sql.OrderClause,
) []const u8 {
    if (order_clauses.len == 0) return "";

    var size: usize = 0;
    for (order_clauses, 0..) |order_clause, index| {
        const separator = if (index + 1 < order_clauses.len) ", " else "";
        size += (Adapter.orderSql(order_clause) ++ separator).len;
    }
    var order_buf: [size]u8 = undefined;
    var cursor: usize = 0;
    for (order_clauses, 0..) |order_clause, index| {
        const separator = if (index + 1 < order_clauses.len) ", " else "";
        const order_sql = Adapter.orderSql(order_clause) ++ separator;
        @memcpy(order_buf[cursor .. cursor + order_sql.len], order_sql);
        cursor += order_sql.len;
    }
    return std.fmt.comptimePrint(
        " ORDER BY {s}",
        .{order_buf},
    );
}

fn renderGroupBy(
    Adapter: type,
    comptime maybe_group_by: ?[]const jetquery.columns.Column,
    comptime having_clauses: []const jetquery.sql.Where.Tree,
) []const u8 {
    const group_by = maybe_group_by orelse return "";

    var size: usize = 0;

    for (group_by, 0..) |column, index| {
        const separator = if (index + 1 < group_by.len) ", " else "";
        const column_sql = Adapter.columnSql(column.table, column) ++ separator;
        size += column_sql.len;
    }

    const and_operator = " AND ";
    const having = " HAVING ";
    if (having_clauses.len > 0) size += having.len;
    for (having_clauses, 0..) |clause, index| {
        if (index > 0) size += and_operator.len;
        size += clause.render(Adapter).len;
    }

    var buf: [size]u8 = undefined;
    var cursor: usize = 0;

    for (group_by, 0..) |column, index| {
        const separator = if (index + 1 < group_by.len) ", " else "";
        const column_sql = Adapter.columnSql(column.table, column) ++ separator;
        @memcpy(buf[cursor .. cursor + column_sql.len], column_sql);
        cursor += column_sql.len;
    }

    if (having_clauses.len > 0) {
        @memcpy(buf[cursor .. cursor + having.len], having);
        cursor += having.len;
    }

    for (having_clauses, 0..) |clause, index| {
        if (index > 0) size += and_operator.len;
        const operator = if (index > 0) and_operator else "";
        const sql = operator ++ clause.render(Adapter);
        @memcpy(buf[cursor .. cursor + sql.len], sql);
        cursor += sql.len;
    }

    return std.fmt.comptimePrint(" GROUP BY {s}", .{buf});
}

fn renderWhere(
    Adapter: type,
    comptime where_clauses: []const jetquery.sql.Where.Tree,
) []const u8 {
    if (where_clauses.len == 0) return " WHERE " ++ Adapter.emptyWhereSql();

    const and_operator = " AND ";
    var size: usize = 0;
    for (where_clauses, 0..) |clause, index| {
        if (index > 0) size += and_operator.len;
        size += clause.render(Adapter).len;
    }
    var buf: [size]u8 = undefined;
    var cursor: usize = 0;
    for (where_clauses, 0..) |clause, index| {
        const operator = if (index > 0) and_operator else "";
        const sql = operator ++ clause.render(Adapter);
        @memcpy(buf[cursor .. cursor + sql.len], sql);
        cursor += sql.len;
    }

    return std.fmt.comptimePrint(" WHERE {s}", .{&buf});
}

fn renderJoins(Adapter: type, Table: type, relations: []const type) []const u8 {
    comptime {
        if (relations.len == 0) return "";

        var buf_len: usize = 0;
        for (relations) |Relation| {
            buf_len += switch (Relation.context) {
                .inner => renderInnerJoin(Adapter, Table, Relation).len,
                .outer => renderOuterJoin(Adapter, Table, Relation).len,
                .include => switch (Relation.relation_type) {
                    .belongs_to => renderInnerJoin(Adapter, Table, Relation).len,
                    .has_many => "".len,
                },
            };
        }
        var buf: [buf_len]u8 = undefined;
        var cursor: usize = 0;
        for (relations) |Relation| {
            const sql = switch (Relation.context) {
                .inner => renderInnerJoin(Adapter, Table, Relation),
                .outer => renderOuterJoin(Adapter, Table, Relation),
                .include => switch (Relation.relation_type) {
                    .belongs_to => renderInnerJoin(Adapter, Table, Relation),
                    .has_many => "",
                },
            };
            @memcpy(buf[cursor .. cursor + sql.len], sql);
            cursor += sql.len;
        }

        return &buf;
    }
}

fn renderInnerJoin(Adapter: type, Table: type, Relation: type) []const u8 {
    const PrimaryKey = std.meta.FieldEnum(Relation.Source.Definition);
    const ForeignKey = std.meta.FieldEnum(Table.Definition);
    const primary_key: PrimaryKey = std.enums.nameCast(PrimaryKey, Relation.primary_key);
    const foreign_key: ForeignKey = std.enums.nameCast(ForeignKey, Relation.foreign_key);

    return Adapter.innerJoinSql(
        Table,
        Relation.Source,
        Relation.relation_name,
        .{ .primary_key = @tagName(primary_key), .foreign_key = @tagName(foreign_key) },
    );
}

fn renderOuterJoin(Adapter: type, Table: type, Relation: type) []const u8 {
    const PrimaryKey = std.meta.FieldEnum(Relation.Source.Definition);
    const ForeignKey = std.meta.FieldEnum(Table.Definition);
    const primary_key: PrimaryKey = std.enums.nameCast(PrimaryKey, Relation.primary_key);
    const foreign_key: ForeignKey = std.enums.nameCast(ForeignKey, Relation.foreign_key);

    return Adapter.outerJoinSql(
        Table,
        Relation.Source,
        Relation.relation_name,
        .{ .primary_key = @tagName(primary_key), .foreign_key = @tagName(foreign_key) },
    );
}

fn renderSelectColumns(
    Adapter: type,
    relations: []const type,
    comptime columns: []const jetquery.columns.Column,
) []const u8 {
    comptime {
        var total_columns: usize = columns.len;
        for (relations) |Relation| {
            // has_many relations issue a separate query so we don't select columns here.
            // Only belongs_to uses an inner join.
            if (Relation.relation_type != .belongs_to) continue;
            total_columns += Relation.select_columns.len;
        }

        var columns_buf_len: usize = 0;
        for (columns, 0..) |column, index| {
            columns_buf_len += renderSelectColumn(
                Adapter,
                column.table,
                column,
                index,
                total_columns,
            ).len;
        }

        var start = columns.len;
        for (relations) |Relation| {
            // has_many relations issue a separate query so we don't select columns here.
            // Only belongs_to uses an inner join.
            if (Relation.relation_type != .belongs_to) continue;

            for (Relation.select_columns, start..) |column, index| {
                columns_buf_len += renderSelectColumn(
                    Adapter,
                    Relation.Source,
                    column,
                    index,
                    total_columns,
                ).len;
            }
            start += Relation.select_columns.len;
        }

        var columns_buf: [columns_buf_len]u8 = undefined;
        var cursor: usize = 0;
        for (columns, 0..) |column, index| {
            const column_tag = renderSelectColumn(
                Adapter,
                column.table,
                column,
                index,
                total_columns,
            );
            @memcpy(columns_buf[cursor .. cursor + column_tag.len], column_tag);
            cursor += column_tag.len;
        }

        start = columns.len;
        for (relations) |Relation| {
            // has_many relations issue a separate query so we don't select columns here.
            // Only belongs_to uses an inner join.
            if (Relation.relation_type != .belongs_to) continue;

            for (Relation.select_columns, start..) |column, index| {
                const column_tag = renderSelectColumn(
                    Adapter,
                    Relation.Source,
                    column,
                    index,
                    total_columns,
                );
                @memcpy(columns_buf[cursor .. cursor + column_tag.len], column_tag);
                cursor += column_tag.len;
            }
            start += Relation.select_columns.len;
        }
        return &columns_buf;
    }
}

fn renderSelectColumn(
    Adapter: type,
    Table: type,
    comptime column: jetquery.columns.Column,
    comptime index: usize,
    comptime total: usize,
) []const u8 {
    comptime {
        return std.fmt.comptimePrint(
            " {s}{s}",
            .{ Adapter.columnSql(Table, column), if (index + 1 < total) "," else "" },
        );
    }
}

// TODO: Find a nicer way of counting so we don't have to keep this sync'ed with `renderParams`
fn paramsBufSize(
    Adapter: type,
    comptime field_infos: []const jetquery.fields.FieldInfo,
    comptime context: jetquery.fields.FieldContext,
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
                    else => Adapter.columnSql(field.Table, field),
                },
                if (index < last_param_index) separator else "",
            },
            .value => .{
                Adapter.paramSql(index),
                if (index < last_param_index) separator else "",
            },
            .assign => switch (context) {
                .update => .{
                    Adapter.identifier(field.name),
                    Adapter.paramSql(index),
                    if (index < last_param_index) separator else "",
                },
                else => .{
                    Adapter.columnSql(field.Table, field),
                    Adapter.paramSql(index),
                    if (index < last_param_index) separator else "",
                },
            },
        };

        buf_len += std.fmt.count(template, args);
    }
    return buf_len;
}

fn renderParams(
    buf: []u8,
    Adapter: type,
    comptime field_infos: []const jetquery.fields.FieldInfo,
    comptime context: jetquery.fields.FieldContext,
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
                    else => Adapter.columnSql(field.Table, field),
                },
                if (index < last_param_index) separator else "",
            },
            .value => .{
                Adapter.paramSql(index),
                if (index < last_param_index) separator else "",
            },
            .assign => switch (context) {
                .update => .{
                    Adapter.identifier(field.name),
                    Adapter.paramSql(index),
                    if (index < last_param_index) separator else "",
                },
                else => .{
                    Adapter.columnSql(field.Table, field),
                    Adapter.paramSql(index),
                    if (index < last_param_index) separator else "",
                },
            },
        };

        const param = std.fmt.comptimePrint(template, args);
        @memcpy(buf[cursor .. cursor + param.len], param);
        cursor += param.len;
    }
    return buf;
}

fn fieldContext(
    comptime field_infos: []const jetquery.fields.FieldInfo,
    comptime index: usize,
    comptime context: jetquery.fields.FieldContext,
) bool {
    return if (field_infos.len > index)
        field_infos[index].context == context
    else
        false;
}

fn lastParamIndex(
    comptime field_infos: []const jetquery.fields.FieldInfo,
    comptime context: jetquery.fields.FieldContext,
) usize {
    var maybe_index: ?usize = null;

    for (field_infos, 0..) |field, index| {
        if (field.context == context) maybe_index = index;
    }
    if (maybe_index) |index|
        return index
    else
        @compileError("No param matched for `" ++ @tagName(context) ++ "` query.");
}

fn hasParam(
    comptime field_infos: []const jetquery.fields.FieldInfo,
    comptime context: jetquery.fields.FieldContext,
) bool {
    for (field_infos) |field| {
        if (field.context == context) return true;
    }
    return false;
}
