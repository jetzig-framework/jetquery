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
        ) Statement(.select, Schema, Table, .{
            .columns = if (columns.len == 0) Table.columns() else columns,
        }) {
            return InitialStatement(Schema, Table).select(columns);
        }

        /// Create a `SELECT` query defaulting to all columns selected:
        /// ```zig
        /// Query(Schema, .MyTable).where(.{ .foo = "bar" })
        /// ```
        /// Short-hand for:
        /// ```zig
        /// Query(Schema, .MyTable).select(&.{}).where(.{ .foo = "bar" })
        /// ```
        pub fn where(args: anytype) Statement(.select, Schema, Table, .{
            .field_infos = &fields.fieldInfos(@TypeOf(args), .where),
            .columns = Table.columns(),
        }) {
            return InitialStatement(Schema, Table).select(&.{}).where(args);
        }

        /// Create an `UPDATE` query with the specified `args`, e.g.:
        /// ```zig
        /// Query(Schema, .MyTable).update(.{ .foo = "bar", .baz = "qux" }).where(.{ .quux = "corge" });
        /// ```
        pub fn update(args: anytype) Statement(.update, Schema, Table, .{
            .field_infos = &(fields.fieldInfos(@TypeOf(args), .update) ++ timestampsFields(Table, .update)),
        }) {
            return InitialStatement(Schema, Table).update(args);
        }

        /// Create an `INSERT` query with the specified `args`, e.g.:
        /// ```zig
        /// Query(Schema, .MyTable).insert(.{ .foo = "bar", .baz = "qux" });
        /// ```
        pub fn insert(args: anytype) Statement(.insert, Schema, Table, .{
            .field_infos = &(fields.fieldInfos(@TypeOf(args), .insert) ++ timestampsFields(Table, .insert)),
        }) {
            return InitialStatement(Schema, Table).insert(args);
        }

        /// Create a `DELETE` query. As a safety measure, a `delete()` query **must** have a
        /// `.where()` clause attached or it will not be executed. Use `deleteAll()` if you wish
        /// to delete all records.
        /// ```zig
        /// Query(Schema, .MyTable).delete().where(.{ .foo = "bar" });
        /// ```
        pub fn delete() Statement(.delete, Schema, Table, .{}) {
            return InitialStatement(Schema, Table).delete();
        }

        /// Create a `DELETE` query that does not require a `WHERE` clause to delete all records
        /// from a table.
        /// ```zig
        /// Query(Schema, .MyTable).deleteAll();
        /// ```
        pub fn deleteAll() Statement(.delete_all, Schema, Table, .{}) {
            return InitialStatement(Schema, Table).deleteAll();
        }

        /// Create a `SELECT` query to return a single row matching the given ID.
        /// ```zig
        /// Query(Schema, .MyTable).find(1000);
        /// ```
        /// Short-hand for:
        /// ```zig
        /// Query(Schema, .MyTable).select(&.{}).where(.{ .id = id }).limit(1);
        /// ```
        pub fn find(id: anytype) Statement(.select, Schema, Table, .{
            .field_infos = &(fields.fieldInfos(@TypeOf(.{ .id = id }), .where) ++
                fields.fieldInfos(@TypeOf(.{1}), .limit)),
            .columns = Table.columns(),
            .result_context = .one,
        }) {
            return InitialStatement(Schema, Table).find(id);
        }

        /// Create a `SELECT` query to return a single row matching the given args.
        /// ```zig
        /// Query(Schema, .MyTable).findBy(.{ .foo = "bar", .baz = "qux" });
        /// ```
        /// Short-hand for:
        /// ```zig
        /// Query(Schema, .MyTable).select(&.{}).where(args).limit(1);
        /// ```
        pub fn findBy(args: anytype) Statement(.select, Schema, Table, .{
            .field_infos = &(fields.fieldInfos(@TypeOf(args), .where) ++
                fields.fieldInfos(@TypeOf(.{1}), .limit)),
            .columns = Table.columns(),
            .result_context = .one,
        }) {
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
        ) Statement(.none, Schema, Table, .{
            .relations = &.{jetquery.relation.Relation(
                Schema,
                Table,
                name,
                if (select_columns.len == 0)
                    std.enums.values(jetquery.relation.ColumnsEnum(Schema, Table, name))
                else
                    select_columns,
            )},
        }) {
            return InitialStatement(Schema, Table).include(name, select_columns);
        }
    };
}

const MissingField = struct {
    missing: void,
};

fn InitialStatement(Schema: type, Table: type) Statement(.none, Schema, Table, .{
    .result_context = .none,
    .initial = true,
}) {
    return Statement(.none, Schema, Table, .{
        .result_context = .none,
        .initial = true,
    }){ .field_values = .{}, .field_errors = .{} };
}

fn SchemaTable(Schema: type, comptime name: jetquery.DeclEnum(Schema)) type {
    return @field(Schema, @tagName(name));
}

fn StatementOptions(Table: type, comptime query_context: sql.QueryContext) type {
    return struct {
        relations: []const type = &.{},
        field_infos: []const fields.FieldInfo = &.{},
        columns: []const std.meta.FieldEnum(Table.Definition) = &.{},
        order_clauses: []const sql.OrderClause(Table) = &.{},
        result_context: ResultContext = switch (query_context) {
            .select => .many,
            .update, .insert, .delete, .delete_all, .none => .none,
            .count => .one,
        },
        initial: bool = false,
        distinct: bool = false,
    };
}

fn Statement(
    comptime query_context: sql.QueryContext,
    Schema: type,
    Table: type,
    comptime options: StatementOptions(Table, query_context),
) type {
    return struct {
        field_values: fields.FieldValues(Table, options.relations, options.field_infos),
        limit_bound: ?usize = null,
        field_errors: [options.field_infos.len]?anyerror,

        comptime query_context: sql.QueryContext = query_context,
        comptime field_infos: []const fields.FieldInfo = options.field_infos,
        comptime columns: []const std.meta.FieldEnum(Table.Definition) = options.columns,
        comptime order_clauses: []const sql.OrderClause(Table) = options.order_clauses,

        comptime sql: []const u8 = jetquery.sql.render(
            jetquery.adapters.Type(jetquery.config.database.adapter),
            query_context,
            Table,
            options.relations,
            options.field_infos,
            options.columns,
            options.order_clauses,
        ),

        pub const Definition = Table.Definition;
        pub const ResultContext = options.result_context;
        pub const ResultType = QueryResultType();
        pub const ColumnInfos = QueryColumnInfos();

        const Self = @This();

        pub fn extend(
            self: Self,
            S: type,
            args: anytype,
            comptime context: fields.FieldContext,
        ) S {
            validateQueryContext(self.query_context, query_context);

            var statement: S = undefined;

            inline for (0..options.field_infos.len) |index| {
                const value = self.field_values[index];
                @field(statement.field_values, std.fmt.comptimePrint("{}", .{index})) = value;
                statement.field_errors[index] = self.field_errors[index];
            }

            if (comptime hasTimestamps(Table)) {
                updateTimestamps(S, &statement, options.field_infos, query_context);
            }

            const arg_fields = std.meta.fields(@TypeOf(args));

            inline for (arg_fields, options.field_infos.len..) |field, index| {
                const value = @field(args, field.name);
                const coerced = coercion.coerce(
                    Table,
                    options.relations,
                    fields.fieldInfo(field, context),
                    value,
                );
                @field(statement.field_values, std.fmt.comptimePrint("{}", .{index})) = coerced.value;
                statement.field_errors[index] = coerced.err;
            }

            return statement;
        }

        pub fn select(
            self: Self,
            comptime select_columns: []const std.meta.FieldEnum(Table.Definition),
        ) Statement(.select, Schema, Table, .{
            .relations = options.relations,
            .field_infos = options.field_infos,
            .columns = if (select_columns.len == 0) Table.columns() else select_columns,
            .order_clauses = options.order_clauses,
            .result_context = if (options.initial) .many else options.result_context,
        }) {
            const S = Statement(.select, Schema, Table, .{
                .relations = options.relations,
                .field_infos = options.field_infos,
                .columns = if (select_columns.len == 0) Table.columns() else select_columns,
                .order_clauses = options.order_clauses,
                .result_context = if (options.initial) .many else options.result_context,
            });
            return self.extend(S, .{}, .none);
        }

        /// Apply a `WHERE` clause to the current statement.
        pub fn where(self: Self, args: anytype) Statement(query_context, Schema, Table, .{
            .relations = options.relations,
            .field_infos = options.field_infos ++ fields.fieldInfos(@TypeOf(args), .where),
            .columns = options.columns,
            .order_clauses = options.order_clauses,
            .result_context = options.result_context,
        }) {
            const S = Statement(query_context, Schema, Table, .{
                .relations = options.relations,
                .field_infos = options.field_infos ++ fields.fieldInfos(@TypeOf(args), .where),
                .columns = options.columns,
                .order_clauses = options.order_clauses,
                .result_context = options.result_context,
            });
            return self.extend(S, args, .where);
        }

        pub fn find(self: Self, id: anytype) Statement(.select, Schema, Table, .{
            .field_infos = &(fields.fieldInfos(@TypeOf(.{ .id = id }), .where) ++
                fields.fieldInfos(@TypeOf(.{1}), .limit)),
            .columns = if (options.columns.len == 0) Table.columns() else options.columns,
            .result_context = .one,
        }) {
            // No need to verify `id` presence as `fields.fieldInfos` will reject unknown fields.
            return self.findBy(.{ .id = id });
        }

        pub fn findBy(self: Self, args: anytype) Statement(.select, Schema, Table, .{
            .relations = options.relations,
            .field_infos = options.field_infos ++
                fields.fieldInfos(@TypeOf(args), .where) ++
                fields.fieldInfos(@TypeOf(.{1}), .limit),
            .columns = if (options.columns.len == 0) Table.columns() else options.columns,
            .result_context = .one,
        }) {
            const S = Statement(.select, Schema, Table, .{
                .relations = options.relations,
                .field_infos = options.field_infos ++
                    fields.fieldInfos(@TypeOf(args), .where) ++
                    fields.fieldInfos(@TypeOf(.{1}), .limit),
                .columns = if (options.columns.len == 0) Table.columns() else options.columns,
                .result_context = .one,
            });
            var statement = self.extend(S, args, .where);
            const arg_fields = std.meta.fields(@TypeOf(args));
            statement.field_values[options.field_infos.len + arg_fields.len] = 1;
            statement.field_errors[options.field_infos.len + arg_fields.len] = null;
            return statement;
        }

        pub fn count(self: Self) Statement(.count, Schema, Table, .{
            .relations = options.relations,
            .field_infos = options.field_infos,
        }) {
            const S = Statement(.count, Schema, Table, .{
                .relations = options.relations,
                .field_infos = options.field_infos,
            });
            return self.extend(S, .{}, .none);
        }

        pub fn distinct(self: Self, comptime args: anytype) Statement(query_context, Schema, Table, .{
            .relations = options.relations,
            .field_infos = options.field_infos,
            .columns = options.columns,
            .order_clauses = options.order_clauses,
            .result_context = options.result_context,
            .distinct = true,
        }) {
            const S = Statement(query_context, Schema, Table, .{
                .relations = options.relations,
                .field_infos = options.field_infos,
                .columns = options.columns,
                .order_clauses = options.order_clauses,
                .result_context = options.result_context,
                .distinct = true,
            });
            _ = args;
            return self.extend(S, .{}, .none);
        }
        pub fn update(self: Self, args: anytype) Statement(.update, Schema, Table, .{
            .field_infos = &(fields.fieldInfos(@TypeOf(args), .update) ++ timestampsFields(Table, .update)),
        }) {
            const S = Statement(.update, Schema, Table, .{
                .field_infos = &(fields.fieldInfos(@TypeOf(args), .update) ++ timestampsFields(Table, .update)),
            });
            return self.extend(S, args, .update);
        }

        pub fn insert(self: Self, args: anytype) Statement(.insert, Schema, Table, .{
            .field_infos = &(fields.fieldInfos(@TypeOf(args), .insert) ++
                timestampsFields(Table, .insert)),
        }) {
            const S = Statement(.insert, Schema, Table, .{
                .field_infos = &(fields.fieldInfos(@TypeOf(args), .insert) ++
                    timestampsFields(Table, .insert)),
            });
            return self.extend(S, args, .insert);
        }

        pub fn delete(self: Self) Statement(.delete, Schema, Table, .{
            .field_infos = &fields.fieldInfos(@TypeOf(.{}), .none),
        }) {
            // TODO: Add support for `DELETE ... USING ...`
            if (comptime options.relations.len != 0) @compileError(
                "Failed attempting to generate `DELETE` query with relations. " ++
                    "This error occurred to prevent accidential deletion of potentially unexpected behaviour.",
            );
            const S = Statement(.delete, Schema, Table, .{
                .field_infos = &fields.fieldInfos(@TypeOf(.{}), .none),
            });
            return self.extend(S, .{}, .none);
        }

        pub fn deleteAll(self: Self) Statement(.delete_all, Schema, Table, .{}) {
            // TODO: Add support for `DELETE ... USING ...`
            if (comptime options.relations.len != 0) @compileError(
                "Failed attempting to generate `DELETE` query with relations. " ++
                    "This error occurred to prevent accidential deletion of potentially unexpected behaviour.",
            );
            const S = Statement(.delete_all, Schema, Table, .{});
            return self.extend(S, .{}, .none);
        }

        pub fn limit(self: Self, bound: usize) Statement(query_context, Schema, Table, .{
            .relations = options.relations,
            .field_infos = options.field_infos ++ fields.fieldInfos(@TypeOf(.{bound}), .limit),
            .columns = options.columns,
            .order_clauses = options.order_clauses,
            .result_context = options.result_context,
        }) {
            const S = Statement(query_context, Schema, Table, .{
                .relations = options.relations,
                .field_infos = options.field_infos ++ fields.fieldInfos(@TypeOf(.{bound}), .limit),
                .columns = options.columns,
                .order_clauses = options.order_clauses,
                .result_context = options.result_context,
            });
            return self.extend(S, .{bound}, .limit);
        }

        pub fn orderBy(self: Self, comptime args: anytype) Statement(query_context, Schema, Table, .{
            .relations = options.relations,
            .field_infos = options.field_infos,
            .columns = options.columns,
            .order_clauses = &translateOrderBy(Table, args),
            .result_context = options.result_context,
        }) {
            const S = Statement(query_context, Schema, Table, .{
                .relations = options.relations,
                .field_infos = options.field_infos,
                .columns = options.columns,
                .order_clauses = &translateOrderBy(Table, args),
                .result_context = options.result_context,
            });
            return self.extend(S, .{}, .order);
        }

        pub fn include(
            self: Self,
            comptime name: jetquery.relation.RelationsEnum(Table),
            comptime select_columns: []const jetquery.relation.ColumnsEnum(Schema, Table, name),
        ) Statement(query_context, Schema, Table, .{
            .relations = options.relations ++ .{jetquery.relation.Relation(
                Schema,
                Table,
                name,
                if (select_columns.len == 0)
                    std.enums.values(jetquery.relation.ColumnsEnum(Schema, Table, name))
                else
                    select_columns,
            )},
            .field_infos = options.field_infos,
            .columns = options.columns,
            .order_clauses = options.order_clauses,
            .result_context = options.result_context,
        }) {
            const S = Statement(query_context, Schema, Table, .{
                .relations = options.relations ++ .{jetquery.relation.Relation(
                    Schema,
                    Table,
                    name,
                    if (select_columns.len == 0)
                        std.enums.values(jetquery.relation.ColumnsEnum(Schema, Table, name))
                    else
                        select_columns,
                )},
                .field_infos = options.field_infos,
                .columns = options.columns,
                .order_clauses = options.order_clauses,
                .result_context = options.result_context,
            });
            return self.extend(S, .{}, .none);
        }

        pub fn execute(self: Self, repo: *jetquery.Repo) !switch (options.result_context) {
            .one => ?ResultType,
            .many => jetquery.Result,
            .none => void,
        } {
            return try repo.execute(self);
        }

        pub fn values(self: Self) fields.FieldValues(Table, options.relations, options.field_infos) {
            return self.field_values;
        }

        pub fn validateValues(self: Self) !void {
            for (self.field_errors) |maybe_error| {
                if (maybe_error) |err| return err;
            }
        }

        pub fn validateDelete(self: Self) !void {
            if (query_context == .delete and !self.hasWhereClause()) return error.JetQueryUnsafeDelete;
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
                for (options.columns, 0..) |column, index| {
                    column_infos[index] = .{
                        .name = @tagName(column),
                        .type = std.meta.FieldType(Table.Definition, column),
                        .index = index,
                        .relation = null,
                    };
                }
                var start: usize = options.columns.len;
                for (options.relations) |Relation| {
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
                switch (query_context) {
                    // TODO: Is there a more sensible type to use here ?
                    .count => return i64,
                    else => {},
                }

                var base_fields: [options.columns.len]std.builtin.Type.StructField = undefined;

                for (options.columns, 0..) |column, index| {
                    const T = std.meta.fieldInfo(Table.Definition, column).type;
                    base_fields[index] = .{
                        .name = @tagName(column),
                        .type = T,
                        .default_value = null,
                        .is_comptime = false,
                        .alignment = @alignOf(T),
                    };
                }

                var relations_fields: [options.relations.len]std.builtin.Type.StructField = undefined;
                for (options.relations, 0..) |Relation, relation_index| {
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
                var len: usize = options.columns.len;
                for (options.relations) |Relation| {
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
    comptime query_context: fields.FieldContext,
) [timestampsSize(Table, query_context)]fields.FieldInfo {
    // TODO: Tidy this up a bit to remove all the repetition
    return if (hasTimestamps(Table)) switch (query_context) {
        .update => .{
            fields.fieldInfo(.{
                .name = "updated_at",
                .type = usize,
                .default_value = null,
                .is_comptime = false,
                .alignment = @alignOf(usize),
            }, query_context),
        },
        .insert => .{
            fields.fieldInfo(.{
                .name = "created_at",
                .type = usize,
                .default_value = null,
                .is_comptime = false,
                .alignment = @alignOf(usize),
            }, query_context),
            fields.fieldInfo(.{
                .name = "updated_at",
                .type = usize,
                .default_value = null,
                .is_comptime = false,
                .alignment = @alignOf(usize),
            }, query_context),
        },
        else => @compileError(
            "Timestamps detection not relevant for `" ++ @tagName(query_context) ++ "` query. (This is a bug).",
        ),
    } else .{};
}

fn hasTimestamps(Table: type) bool {
    return @hasField(Table.Definition, jetquery.column_names.created_at) and
        @hasField(Table.Definition, jetquery.column_names.updated_at);
}

fn timestampsSize(Table: type, comptime query_context: fields.FieldContext) u2 {
    if (!hasTimestamps(Table)) return 0;

    return switch (query_context) {
        .update => 1,
        .insert => 2,
        else => @compileError(
            "Timestamps detection not relevant for `" ++ @tagName(query_context) ++ "` query. (This is a bug).",
        ),
    };
}

fn now() i64 {
    return std.time.microTimestamp();
}

fn validateQueryContext(comptime initial: sql.QueryContext, comptime attempted: sql.QueryContext) void {
    comptime {
        switch (attempted) {
            .count => switch (initial) {
                .none, .select => {},
                else => if (attempted != initial) @compileError(std.fmt.comptimePrint(
                    "Failed attempting to initialize `{s}` query already-initialized on `{s}` query.",
                    .{ @tagName(attempted), @tagName(initial) },
                )),
            },
            else => switch (initial) {
                .none => {},
                else => if (attempted != initial) @compileError(std.fmt.comptimePrint(
                    "Failed attempting to initialize `{s}` query already-initialized on `{s}` query.",
                    .{ @tagName(attempted), @tagName(initial) },
                )),
            },
        }
    }
}

fn updateTimestamps(
    T: type,
    statement: *T,
    comptime field_infos: []const fields.FieldInfo,
    comptime context: sql.QueryContext,
) void {
    switch (context) {
        .update => {
            const timestamp = now();
            const index = field_infos.len - 1; // Guaranteed by `Statement.update()`
            @field(statement.field_values, std.fmt.comptimePrint("{}", .{index})) = timestamp;
            statement.field_errors[index] = null;
        },
        .insert => {
            const timestamp = now();
            const index = field_infos.len - 2; // Guaranteed by `Statement.insert()`
            @field(statement.field_values, std.fmt.comptimePrint("{}", .{index})) = timestamp;
            statement.field_errors[index] = null;
            @field(statement.field_values, std.fmt.comptimePrint("{}", .{index + 1})) = timestamp;
            statement.field_errors[index + 1] = null;
        },
        else => {},
    }
}
