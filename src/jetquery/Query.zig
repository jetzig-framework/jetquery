const std = @import("std");

const jetcommon = @import("jetcommon");

const jetquery = @import("../jetquery.zig");

const fields = @import("fields.zig");
const coercion = @import("coercion.zig");
const sql = @import("sql.zig");

// Number of rows expected to be returned by a query.
const ResultContext = enum { one, many, none };

/// Create a new query by passing a table definition.
/// ```zig
/// const query = Query(Schema.Cats).init(allocator);
/// ```
pub fn Query(Schema: type, comptime table_name: jetquery.DeclEnum(Schema)) type {
    const Table = @field(Schema, @tagName(table_name));
    return struct {
        table: Table,

        pub const Definition = Table.Definition;

        /// Create a `SELECT` query with the specified `columns`, e.g.:
        /// ```zig
        /// Query(Schema, .MyTable).select(&.{ .foo, .bar }).where(.{ .foo = "qux" });
        /// ```
        /// Pass an empty `columns` array to select all columns:
        /// ```zig
        /// Query(Schema, .MyTable).select(&.{}).where(.{ .foo = "qux" });
        /// ```
        pub fn select(
            comptime columns: []const std.meta.FieldEnum(Table.Definition),
        ) Statement(
            .select,
            Schema,
            Table,
            &.{},
            &fields.fieldInfos(@TypeOf(.{}), .none),
            if (columns.len == 0) Table.columns() else columns,
            &.{},
            .many,
            false,
        ) {
            return InitialStatement(Schema, Table).select(columns);
        }

        /// Create an `UPDATE` query with the specified `args`, e.g.:
        /// ```zig
        /// Query(Schema, .MyTable).update(.{ .foo = "bar", .baz = "qux" }).where(.{ .quux = "corge" });
        /// ```
        pub fn update(args: anytype) Statement(
            .update,
            Schema,
            Table,
            &.{},
            &(fields.fieldInfos(@TypeOf(args), .update) ++ timestampsFields(Table, .update)),
            &.{},
            &.{},
            .none,
            false,
        ) {
            var statement: Statement(
                .update,
                Schema,
                Table,
                &.{},
                &(fields.fieldInfos(@TypeOf(args), .update) ++ timestampsFields(Table, .update)),
                &.{},
                &.{},
                .none,
                false,
            ) = undefined;
            const arg_fields = std.meta.fields(@TypeOf(args));
            inline for (arg_fields, 0..) |field, index| {
                const value = @field(args, field.name);
                const coerced = coercion.coerce(Table, &.{}, fields.fieldInfo(field, .update), value);
                @field(statement.field_values, std.fmt.comptimePrint("{}", .{index})) = coerced.value;
                statement.field_errors[index] = coerced.err;
            }
            if (comptime hasTimestamps(Table)) {
                @field(statement.field_values, std.fmt.comptimePrint("{}", .{arg_fields.len})) = now();
                statement.field_errors[arg_fields.len] = null;
            }
            return statement;
        }

        /// Create an `INSERT` query with the specified `args`, e.g.:
        /// ```zig
        /// Query(Schema, .MyTable).insert(.{ .foo = "bar", .baz = "qux" });
        /// ```
        pub fn insert(args: anytype) Statement(
            .insert,
            Schema,
            Table,
            &.{},
            &(fields.fieldInfos(@TypeOf(args), .insert) ++ timestampsFields(Table, .insert)),
            &.{},
            &.{},
            .none,
            false,
        ) {
            var statement: Statement(
                .insert,
                Schema,
                Table,
                &.{},
                &(fields.fieldInfos(@TypeOf(args), .insert) ++ timestampsFields(Table, .insert)),
                &.{},
                &.{},
                .none,
                false,
            ) = undefined;

            const arg_fields = std.meta.fields(@TypeOf(args));

            inline for (arg_fields, 0..) |field, index| {
                const value = @field(args, field.name);
                const coerced = coercion.coerce(Table, &.{}, fields.fieldInfo(field, .insert), value);
                @field(statement.field_values, std.fmt.comptimePrint("{}", .{index})) = coerced.value;
                statement.field_errors[index] = coerced.err;
            }
            if (comptime hasTimestamps(Table)) {
                const timestamp = now();
                @field(statement.field_values, std.fmt.comptimePrint("{}", .{arg_fields.len})) = timestamp;
                statement.field_errors[arg_fields.len] = null;
                @field(statement.field_values, std.fmt.comptimePrint("{}", .{arg_fields.len + 1})) = timestamp;
                statement.field_errors[arg_fields.len + 1] = null;
            }
            return statement;
        }

        /// Create a `DELETE` query. As a safety measure, a `delete()` query **must** have a
        /// `.where()` clause attached or it will not be executed. Use `deleteAll()` if you wish
        /// to delete all records.
        /// ```zig
        /// Query(Schema, .MyTable).delete().where(.{ .foo = "bar" });
        /// ```
        pub fn delete() Statement(
            .delete,
            Schema,
            Table,
            &.{},
            &fields.fieldInfos(@TypeOf(.{}), .none),
            &.{},
            &.{},
            .none,
            false,
        ) {
            return Statement(
                .delete,
                Schema,
                Table,
                &.{},
                &fields.fieldInfos(@TypeOf(.{}), .none),
                &.{},
                &.{},
                .none,
                false,
            ){ .field_values = .{}, .field_errors = .{} };
        }

        /// Create a `DELETE` query that does not require a `WHERE` clause to delete all records
        /// from a table.
        /// ```zig
        /// Query(Schema, .MyTable).deleteAll();
        /// ```
        pub fn deleteAll() Statement(
            .delete_all,
            Schema,
            Table,
            &.{},
            &fields.fieldInfos(@TypeOf(.{}), .none),
            &.{},
            &.{},
            .none,
            false,
        ) {
            return Statement(
                .delete_all,
                Schema,
                Table,
                &.{},
                &fields.fieldInfos(@TypeOf(.{}), .none),
                &.{},
                &.{},
                .none,
                false,
            ){ .field_values = .{}, .field_errors = .{} };
        }

        /// Create a `SELECT` query to return a single row matching the given ID.
        /// ```zig
        /// Query(Schema, .MyTable).find(1000);
        /// ```
        /// Short-hand for:
        /// ```zig
        /// Query(Schema, .MyTable).select(&.{}).where(.{ .id = id }).limit(1);
        /// ```
        pub fn find(id: anytype) Statement(
            .select,
            Schema,
            Table,
            &.{},
            &(fields.fieldInfos(@TypeOf(.{ .id = id }), .where) ++ fields.fieldInfos(@TypeOf(.{1}), .limit)),
            Table.columns(),
            &.{},
            .one,
            false,
        ) {
            var statement: Statement(
                .select,
                Schema,
                Table,
                &.{},
                &(fields.fieldInfos(@TypeOf(.{ .id = id }), .where) ++ fields.fieldInfos(@TypeOf(.{1}), .limit)),
                Table.columns(),
                &.{},
                .one,
                false,
            ) = undefined;
            if (comptime @hasField(Table.Definition, "id")) {
                const coerced = coercion.coerce(
                    Table,
                    &.{},
                    fields.fieldInfo(std.meta.fieldInfo(Table.Definition, .id), .where),
                    id,
                );
                if (coerced.err) |err| {
                    statement.field_errors = .{ err, null };
                } else {
                    statement.field_values = .{ coerced.value, 1 };
                    statement.field_errors = .{ null, null };
                }
            } else {
                statement.field_errors = .{ error.JetQueryMissingIdField, null };
            }
            return statement;
        }

        /// Create a `SELECT` query to return a single row matching the given args.
        /// ```zig
        /// Query(Schema, .MyTable).findBy(.{ .foo = "bar", .baz = "qux" });
        /// ```
        /// Short-hand for:
        /// ```zig
        /// Query(Schema, .MyTable).select(&.{}).where(args).limit(1);
        /// ```
        pub fn findBy(args: anytype) Statement(
            .select,
            Schema,
            Table,
            &.{},
            &(fields.fieldInfos(@TypeOf(args), .where) ++ fields.fieldInfos(@TypeOf(.{1}), .limit)),
            Table.columns(),
            &.{},
            .one,
            false,
        ) {
            return InitialStatement(Schema, Table).findBy(args);
        }

        /// Indicate that a relation should be fetched with this query. Pass an array of columns
        /// to select from the relation, or pass an empty array to select all columns.
        /// ```zig
        /// Query(Schema, .MyTable).include(.MyRelation, &.{.foo, .bar});
        /// Query(Schema, .MyTable).include(.MyRelation, &.{});
        /// ```
        pub fn include(
            comptime name: jetquery.relation.RelationsEnum(Table),
            comptime select_columns: []const jetquery.relation.ColumnsEnum(Schema, Table, name),
        ) Statement(
            .none,
            Schema,
            Table,
            &.{jetquery.relation.Relation(
                Schema,
                Table,
                name,
                if (select_columns.len == 0)
                    std.enums.values(jetquery.relation.ColumnsEnum(Schema, Table, name))
                else
                    select_columns,
            )},
            &.{},
            &.{},
            &.{},
            .none,
            false,
        ) {
            return Statement(
                .none,
                Schema,
                Table,
                &.{jetquery.relation.Relation(
                    Schema,
                    Table,
                    name,
                    if (select_columns.len == 0)
                        std.enums.values(jetquery.relation.ColumnsEnum(Schema, Table, name))
                    else
                        select_columns,
                )},
                &.{},
                &.{},
                &.{},
                .none,
                false,
            ){ .field_values = .{}, .field_errors = .{} };
        }
    };
}

const MissingField = struct {
    missing: void,
};

fn InitialStatement(Schema: type, Table: type) Statement(
    .none,
    Schema,
    Table,
    &.{},
    &.{},
    &.{},
    &.{},
    .none,
    true,
) {
    return Statement(
        .none,
        Schema,
        Table,
        &.{},
        &.{},
        &.{},
        &.{},
        .none,
        true,
    ){ .field_values = .{}, .field_errors = .{} };
}

fn SchemaTable(Schema: type, comptime name: jetquery.DeclEnum(Schema)) type {
    return @field(Schema, @tagName(name));
}

fn Statement(
    comptime query_type: sql.QueryType,
    Schema: type,
    Table: type,
    comptime relations: []const type,
    comptime field_infos: []const fields.FieldInfo,
    comptime columns: []const std.meta.FieldEnum(Table.Definition),
    comptime order_clauses: []const sql.OrderClause(Table),
    result_context: ResultContext,
    initial: bool,
) type {
    return struct {
        field_values: fields.FieldValues(Table, relations, field_infos),
        limit_bound: ?usize = null,
        field_errors: [field_infos.len]?anyerror,

        comptime query_type: sql.QueryType = query_type,
        comptime field_infos: []const fields.FieldInfo = field_infos,
        comptime columns: []const std.meta.FieldEnum(Table.Definition) = columns,
        comptime order_clauses: []const sql.OrderClause(Table) = order_clauses,

        comptime sql: []const u8 = jetquery.sql.render(
            jetquery.adapters.Type(jetquery.config.database.adapter),
            query_type,
            Table,
            relations,
            field_infos,
            columns,
            order_clauses,
        ),

        pub const Definition = Table.Definition;
        pub const ResultContext = result_context;
        pub const ResultType = QueryResultType();
        pub const ColumnInfos = QueryColumnInfos();

        const Self = @This();

        pub fn extend(
            self: Self,
            S: type,
            statement: *S,
            args: anytype,
            comptime context: fields.FieldContext,
        ) S {
            inline for (0..field_infos.len) |index| {
                const value = self.field_values[index];
                @field(statement.field_values, std.fmt.comptimePrint("{}", .{index})) = value;
                statement.field_errors[index] = self.field_errors[index];
            }
            inline for (std.meta.fields(@TypeOf(args)), field_infos.len..) |field, index| {
                const value = @field(args, field.name);
                const coerced = coercion.coerce(Table, relations, fields.fieldInfo(field, context), value);
                @field(statement.field_values, std.fmt.comptimePrint("{}", .{index})) = coerced.value;
                statement.field_errors[index] = coerced.err;
            }
            return statement.*;
        }

        pub fn select(
            self: Self,
            comptime select_columns: []const std.meta.FieldEnum(Table.Definition),
        ) Statement(
            .select,
            Schema,
            Table,
            relations,
            field_infos,
            if (select_columns.len == 0) Table.columns() else select_columns,
            order_clauses,
            if (initial) .many else result_context,
            false,
        ) {
            validateQueryType(query_type, .select);

            var statement: Statement(
                .select,
                Schema,
                Table,
                relations,
                field_infos,
                if (select_columns.len == 0) Table.columns() else select_columns,
                order_clauses,
                if (initial) .many else result_context,
                false,
            ) = undefined;
            return self.extend(@TypeOf(statement), &statement, .{}, .none);
        }

        /// Apply a `WHERE` clause to the current statement.
        pub fn where(self: Self, args: anytype) Statement(
            query_type,
            Schema,
            Table,
            relations,
            field_infos ++ fields.fieldInfos(@TypeOf(args), .where),
            columns,
            order_clauses,
            result_context,
            false,
        ) {
            var statement: Statement(
                query_type,
                Schema,
                Table,
                relations,
                field_infos ++ fields.fieldInfos(@TypeOf(args), .where),
                columns,
                order_clauses,
                result_context,
                false,
            ) = undefined;
            return self.extend(@TypeOf(statement), &statement, args, .where);
        }

        pub fn findBy(self: Self, args: anytype) Statement(
            .select,
            Schema,
            Table,
            relations,
            field_infos ++ fields.fieldInfos(@TypeOf(args), .where) ++ fields.fieldInfos(@TypeOf(.{1}), .limit),
            Table.columns(),
            &.{},
            .one,
            false,
        ) {
            var statement: Statement(
                .select,
                Schema,
                Table,
                relations,
                field_infos ++ fields.fieldInfos(@TypeOf(args), .where) ++ fields.fieldInfos(@TypeOf(.{1}), .limit),
                Table.columns(),
                &.{},
                .one,
                false,
            ) = undefined;
            _ = self.extend(@TypeOf(statement), &statement, args, .where);
            const arg_fields = std.meta.fields(@TypeOf(args));
            statement.field_values[field_infos.len + arg_fields.len] = 1;
            statement.field_errors[field_infos.len + arg_fields.len] = null;
            return statement;
        }

        /// Apply a `LIMIT` clause to the current statement.
        pub fn limit(self: Self, bound: usize) Statement(
            query_type,
            Schema,
            Table,
            relations,
            field_infos ++ fields.fieldInfos(@TypeOf(.{bound}), .limit),
            columns,
            order_clauses,
            result_context,
            false,
        ) {
            var statement: Statement(
                query_type,
                Schema,
                Table,
                relations,
                field_infos ++ fields.fieldInfos(@TypeOf(.{bound}), .limit),
                columns,
                order_clauses,
                result_context,
                false,
            ) = undefined;
            return self.extend(@TypeOf(statement), &statement, .{bound}, .limit);
        }

        /// Apply an `ORDER BY` clause to the current statement.
        pub fn orderBy(self: Self, comptime args: anytype) Statement(
            query_type,
            Schema,
            Table,
            relations,
            field_infos,
            columns,
            &translateOrderBy(Table, args),
            result_context,
            false,
        ) {
            var statement: Statement(
                query_type,
                Schema,
                Table,
                relations,
                field_infos,
                columns,
                &translateOrderBy(Table, args),
                result_context,
                false,
            ) = undefined;
            return self.extend(@TypeOf(statement), &statement, .{}, .order);
        }

        pub fn relation(
            self: Self,
            comptime name: jetquery.relation.RelationsEnum(Table),
            comptime select_columns: []const jetquery.relation.ColumnsEnum(Schema, Table, name),
        ) Statement(
            query_type,
            Schema,
            Table,
            &.{jetquery.relation.Relation(
                Schema,
                Table,
                name,
                if (select_columns.len == 0)
                    std.enums.values(jetquery.relation.ColumnsEnum(Schema, Table, name))
                else
                    select_columns,
            )},
            field_infos,
            columns,
            order_clauses,
            result_context,
            false,
        ) {
            var statement: Statement(
                query_type,
                Schema,
                Table,
                &.{jetquery.relation.Relation(
                    Schema,
                    Table,
                    name,
                    if (select_columns.len == 0)
                        std.enums.values(jetquery.relation.ColumnsEnum(Schema, Table, name))
                    else
                        select_columns,
                )},
                field_infos,
                columns,
                order_clauses,
                result_context,
                false,
            ) = undefined;
            return self.extend(@TypeOf(statement), &statement, .{}, .none);
        }

        /// Execute the query's SQL with bind params using the provided repo.
        pub fn execute(self: Self, repo: *jetquery.Repo) !switch (result_context) {
            .one => ?ResultType,
            .many => jetquery.Result,
            .none => void,
        } {
            return try repo.execute(self);
        }

        pub fn values(self: Self) fields.FieldValues(Table, relations, field_infos) {
            return self.field_values;
        }

        pub fn validateValues(self: Self) !void {
            for (self.field_errors) |maybe_error| {
                if (maybe_error) |err| return err;
            }
        }

        pub fn validateDelete(self: Self) !void {
            if (query_type == .delete and !self.hasWhereClause()) return error.JetQueryUnsafeDelete;
        }

        fn hasWhereClause(self: Self) bool {
            inline for (self.field_infos) |field| {
                if (field.context == .where) return true;
            }
            return false;
        }

        pub fn QueryColumnInfos() [totalColumnLen()]sql.ColumnInfo {
            comptime {
                var column_infos: [totalColumnLen()]sql.ColumnInfo = undefined;
                for (columns, 0..) |column, index| {
                    column_infos[index] = .{
                        .name = @tagName(column),
                        .type = std.meta.FieldType(Table.Definition, column),
                        .index = index,
                        .relation = null,
                    };
                }
                var start: usize = columns.len;
                for (relations) |Relation| {
                    for (Relation.select_columns, start..) |column, index| {
                        column_infos[index] = .{
                            .name = @tagName(column),
                            .type = std.meta.FieldType(Relation.Source.Definition, column),
                            .index = index,
                            .relation = Relation,
                        };
                    }
                    start += Relation.select_columns.len;
                }

                return column_infos;
            }
        }

        pub fn QueryResultType() type {
            comptime {
                var base_fields: [columns.len]std.builtin.Type.StructField = undefined;

                for (columns, 0..) |column, index| {
                    const T = std.meta.fieldInfo(Table.Definition, column).type;
                    base_fields[index] = .{
                        .name = @tagName(column),
                        .type = T,
                        .default_value = null,
                        .is_comptime = false,
                        .alignment = @alignOf(T),
                    };
                }

                var relations_fields: [relations.len]std.builtin.Type.StructField = undefined;
                for (relations, 0..) |Relation, relation_index| {
                    var relation_fields: [Relation.select_columns.len]std.builtin.Type.StructField = undefined;
                    for (Relation.select_columns, 0..) |column, index| {
                        const T = std.meta.FieldType(Relation.Source.Definition, column);
                        relation_fields[index] = .{
                            .name = @tagName(column),
                            .type = T,
                            .default_value = null,
                            .is_comptime = false,
                            .alignment = @alignOf(T),
                        };
                    }
                    const RT = @Type(.{ .@"struct" = .{
                        .layout = .auto,
                        .fields = &relation_fields,
                        .decls = &.{},
                        .is_tuple = false,
                    } });
                    relations_fields[relation_index] = .{
                        .name = Relation.relation_name,
                        .type = RT,
                        .default_value = null,
                        .is_comptime = false,
                        .alignment = @alignOf(RT),
                    };
                }

                const hidden_id_field = std.builtin.Type.StructField{
                    .name = "__jetquery_id",
                    .type = i128,
                    .default_value = null,
                    .is_comptime = false,
                    .alignment = @alignOf(i128),
                };

                const all_fields = base_fields ++ relations_fields ++ .{hidden_id_field};
                return @Type(.{
                    .@"struct" = .{
                        .layout = .auto,
                        .fields = &all_fields,
                        .decls = &.{},
                        .is_tuple = false,
                    },
                });
            }
        }

        fn totalColumnLen() usize {
            comptime {
                var len: usize = columns.len;
                for (relations) |Relation| {
                    len += Relation.select_columns.len;
                }
                return len;
            }
        }
    };
}

fn translateOrderBy(
    Table: type,
    comptime args: anytype,
) [std.meta.fields(@TypeOf(args)).len]sql.OrderClause(Table) {
    comptime {
        var clauses: [std.meta.fields(@TypeOf(args)).len]sql.OrderClause(Table) = undefined;
        const Columns = std.meta.FieldEnum(Table.Definition);

        for (std.meta.fields(@TypeOf(args)), 0..) |field, index| {
            clauses[index] = .{
                .column = std.enums.nameCast(Columns, field.name),
                .direction = std.enums.nameCast(sql.OrderDirection, @tagName(@field(args, field.name))),
            };
        }
        return clauses;
    }
}

fn timestampsFields(
    Table: type,
    comptime query_type: fields.FieldContext,
) [timestampsSize(Table, query_type)]fields.FieldInfo {
    return if (hasTimestamps(Table)) switch (query_type) {
        .update => .{
            fields.fieldInfo(.{
                .name = "updated_at",
                .type = usize,
                .default_value = null,
                .is_comptime = false,
                .alignment = @alignOf(usize),
            }, query_type), // TODO
        },
        .insert => .{
            fields.fieldInfo(.{
                .name = "created_at",
                .type = usize,
                .default_value = null,
                .is_comptime = false,
                .alignment = @alignOf(usize),
            }, query_type), // TODO
            fields.fieldInfo(.{
                .name = "updated_at",
                .type = usize,
                .default_value = null,
                .is_comptime = false,
                .alignment = @alignOf(usize),
            }, query_type), // TODO
        },
        else => @compileError(
            "Timestamps detection not relevant for `" ++ @tagName(query_type) ++ "` query. (This is a bug).",
        ),
    } else .{};
}

fn hasTimestamps(Table: type) bool {
    return @hasField(Table.Definition, jetquery.column_names.created_at) and
        @hasField(Table.Definition, jetquery.column_names.updated_at);
}

fn timestampsSize(Table: type, comptime query_type: fields.FieldContext) u2 {
    if (!hasTimestamps(Table)) return 0;

    return switch (query_type) {
        .update => 1,
        .insert => 2,
        else => @compileError(
            "Timestamps detection not relevant for `" ++ @tagName(query_type) ++ "` query. (This is a bug).",
        ),
    };
}

fn now() i64 {
    return std.time.microTimestamp();
}

fn validateQueryType(comptime initial: sql.QueryType, comptime attempted: sql.QueryType) void {
    comptime {
        switch (initial) {
            attempted, .none => {},
            else => {
                @compileError(std.fmt.comptimePrint(
                    "Failed attempting to initialize `{s}` query already-initialized on `{s}` query.",
                    .{ @tagName(attempted), @tagName(initial) },
                ));
            },
        }
    }
}
