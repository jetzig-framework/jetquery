const std = @import("std");

const jetcommon = @import("jetcommon");

const jetquery = @import("../jetquery.zig");

const coercion = @import("coercion.zig");
const sql = @import("sql.zig");

// Number of rows expected to be returned by a query.
const ResultContext = enum { one, many, none };

fn Adapter() type {
    return jetquery.adapters.Type(jetquery.config.database.adapter);
}

/// Create a new query by passing a table definition.
/// ```zig
/// const query = Query(Schema.Cats).init(allocator);
/// ```
pub fn Query(Schema: type, comptime table: anytype) type {
    const Table = switch (@typeInfo(@TypeOf(table))) {
        .enum_literal => @field(Schema, @tagName(std.enums.nameCast(jetquery.DeclEnum(Schema), table))),
        else => switch (@TypeOf(table)) {
            type => table,
            else => @compileError("Expected enum literal or type, found `" ++ @typeName(@TypeOf(table))),
        },
    };

    return struct {
        table: Table,

        pub const Definition = Table.Definition;
        pub const info = .{
            .Table = Table,
            .Schema = Schema,
        };

        /// Create a `SELECT` query with the specified `columns`, e.g.:
        /// ```zig
        /// Query(Schema, .MyTable).select(.{ .foo, .bar }).where(.{ .foo = "qux" });
        /// ```
        /// Pass an empty `columns` array to select all columns:
        /// ```zig
        /// Query(Schema, .MyTable).select(.{}).where(.{ .foo = "qux" });
        /// ```
        pub fn select(comptime columns: anytype) Statement(.select, Schema, Table, .{
            .columns = &jetquery.columns.translate(Table, &.{}, columns),
        }) {
            return InitialStatement(Schema, Table).select(columns);
        }

        /// Create a `SELECT` query defaulting to all columns selected:
        /// ```zig
        /// Query(Schema, .MyTable).where(.{ .foo = "bar" })
        /// ```
        /// Short-hand for:
        /// ```zig
        /// Query(Schema, .MyTable).select(.{}).where(.{ .foo = "bar" })
        /// ```
        pub fn where(args: anytype) Statement(.select, Schema, Table, .{
            .field_infos = &jetquery.fields.fieldInfos(Adapter(), Table, &.{}, @TypeOf(args), .where),
            .columns = &Table.columns(),
            .default_select = true,
            .where_clauses = &.{sql.Where.tree(Adapter(), Table, &.{}, @TypeOf(args), .where, 0)},
            .result_context = defaultResultContext(.select),
        }) {
            return InitialStatement(Schema, Table).where(args);
        }

        /// Create an `UPDATE` query with the specified `args`, e.g.:
        /// ```zig
        /// Query(Schema, .MyTable).update(.{ .foo = "bar", .baz = "qux" }).where(.{ .quux = "corge" });
        /// ```
        pub fn update(args: anytype) Statement(.update, Schema, Table, .{
            .field_infos = &(jetquery.fields.fieldInfos(
                Adapter(),
                Table,
                &.{},
                @TypeOf(args),
                .update,
            ) ++ timestampsFields(Table, .update)),
        }) {
            return InitialStatement(Schema, Table).update(args);
        }

        /// Create an `INSERT` query with the specified `args`, e.g.:
        /// ```zig
        /// Query(Schema, .MyTable).insert(.{ .foo = "bar", .baz = "qux" });
        /// ```
        pub fn insert(args: anytype) Statement(.insert, Schema, Table, .{
            .field_infos = &(jetquery.fields.fieldInfos(
                Adapter(),
                Table,
                &.{},
                @TypeOf(args),
                .insert,
            ) ++ timestampsFields(Table, .insert)),
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
        /// Query(Schema, .MyTable).select(.{}).where(.{ .id = id }).limit(1);
        /// ```
        pub fn find(id: anytype) Statement(.select, Schema, Table, .{
            .field_infos = &(jetquery.fields.fieldInfos(
                Adapter(),
                Table,
                &.{},
                @TypeOf(.{ .id = id }),
                .where,
            ) ++
                jetquery.fields.fieldInfos(Adapter(), Table, &.{}, @TypeOf(.{1}), .limit)),
            .where_clauses = &.{
                sql.Where.tree(Adapter(), Table, &.{}, @TypeOf(.{ .id = id }), .where, 0),
            },
            .columns = &Table.columns(),
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
        /// Query(Schema, .MyTable).select(.{}).where(args).limit(1);
        /// ```
        pub fn findBy(args: anytype) Statement(.select, Schema, Table, .{
            .field_infos = &(jetquery.fields.fieldInfos(
                Adapter(),
                Table,
                &.{},
                @TypeOf(args),
                .where,
            ) ++
                jetquery.fields.fieldInfos(Adapter(), Table, &.{}, @TypeOf(.{1}), .limit)),
            .columns = &Table.columns(),
            .result_context = .one,
            .where_clauses = &.{sql.Where.tree(Adapter(), Table, &.{}, @TypeOf(args), .where, 0)},
        }) {
            return InitialStatement(Schema, Table).findBy(args);
        }

        /// Indicate that a relation should be fetched with this query. Pass an array of columns
        /// to select from the relation, or pass an empty array to select all columns.
        /// ```zig
        /// Query(Schema, .MyTable).include(.my_relation, &.{.foo, .bar});
        /// Query(Schema, .MyTable).include(.my_relation, &.{});
        /// ```
        pub fn include(
            comptime name: jetquery.relation.RelationsEnum(Table),
            comptime select_columns: anytype,
        ) Statement(.select, Schema, Table, .{
            .relations = &.{
                jetquery.relation.Relation(Schema, Table, name, select_columns, .include),
            },
            .default_select = true,
            .columns = &Table.columns(),
        }) {
            return InitialStatement(Schema, Table).include(name, select_columns);
        }

        /// Join to another table by association name. Columns on the joined table are not
        /// included in the result set. Use `include` to fetch associations, use `join` to filter
        /// results.
        /// ```zig
        /// Query(Schema, .MyTable).join(.inner, .my_relation);
        /// ```
        /// If required, call `.select` after a `.join` to specify association columns to select:
        /// ```zig
        /// Query(Schema, .MyTable).join(.inner, .my_relation)
        ///     .select(.{ .foo, .my_relation = .{ .bar, .baz } })
        /// ```
        pub fn join(
            comptime join_context: jetquery.relation.JoinContext,
            comptime name: jetquery.relation.RelationsEnum(Table),
        ) Statement(.select, Schema, Table, .{
            .relations = &.{jetquery.relation.Relation(Schema, Table, name, null, join_context)},
            .default_select = true,
            .columns = &Table.columns(),
        }) {
            return InitialStatement(Schema, Table).join(join_context, name);
        }

        /// Create a `SELECT DISTINCT` query with the specified `columns`, e.g.:
        /// ```zig
        /// Query(Schema, .MyTable).distinct(.{ .foo, .bar }).where(.{ .foo = "qux" });
        /// ```
        ///
        /// ```zig
        /// Query(Schema, .MyTable)
        ///     .include(.my_relation)
        ///     .distinct(.{ .foo, .{ .my_relation = .{.bar} });
        /// ```
        pub fn distinct(comptime columns: anytype) Statement(
            .select,
            Schema,
            Table,
            .{
                .distinct = &jetquery.columns.translate(Table, &.{}, columns),
                .result_context = .many,
            },
        ) {
            return InitialStatement(Schema, Table).distinct(columns);
        }

        pub fn init() Statement(.none, Schema, Table, .{ .default_select = true }) {
            return InitialStatement(Schema, Table);
        }

        pub fn groupBy(comptime columns: anytype) Statement(.select, Schema, Table, .{
            .result_context = .many,
            .group_by = &jetquery.columns.translate(Table, &.{}, columns),
        }) {
            return InitialStatement(Schema, Table).groupBy(columns);
        }
    };
}

const MissingField = struct {
    missing: void,
};

fn InitialStatement(
    Schema: type,
    Table: type,
) Statement(.none, Schema, Table, .{
    .result_context = .none,
    .default_select = true,
}) {
    return Statement(.none, Schema, Table, .{
        .result_context = .none,
        .default_select = true,
    }){ .field_values = .{}, .field_errors = .{} };
}

fn SchemaTable(Schema: type, comptime name: jetquery.DeclEnum(Schema)) type {
    return @field(Schema, @tagName(name));
}

fn StatementOptions(comptime query_context: sql.QueryContext) type {
    return struct {
        relations: []const type = &.{},
        field_infos: []const jetquery.fields.FieldInfo = &.{},
        columns: []const jetquery.columns.Column = &.{},
        order_clauses: []const sql.OrderClause = &.{},
        where_clauses: []const sql.Where.Tree = &.{},
        result_context: ResultContext = defaultResultContext(query_context),
        default_select: bool = false,
        distinct: ?[]const jetquery.columns.Column = null,
        group_by: ?[]const jetquery.columns.Column = null,
        having_clauses: []const sql.Where.Tree = &.{},
    };
}

fn defaultResultContext(query_context: sql.QueryContext) ResultContext {
    return switch (query_context) {
        .select => .many,
        .update, .insert, .delete, .delete_all, .none => .none,
        .count => .one,
    };
}

pub const AuxiliaryQuery = struct {
    query: type,
    relation: type,
};

fn Statement(
    comptime query_context: sql.QueryContext,
    Schema: type,
    Table: type,
    comptime options: StatementOptions(query_context),
) type {
    return struct {
        field_values: jetquery.fields.FieldValues(Table, options.relations, options.field_infos),
        limit_bound: ?usize = null,
        field_errors: [options.field_infos.len]?anyerror,

        comptime query_context: sql.QueryContext = query_context,
        comptime field_infos: []const jetquery.fields.FieldInfo = options.field_infos,
        comptime columns: []const jetquery.columns.Column = options.columns,
        comptime order_clauses: []const sql.OrderClause = options.order_clauses,
        comptime auxiliary_queries: []const AuxiliaryQuery = &auxiliaryQueries(),
        comptime sql: []const u8 = jetquery.sql.render(
            Adapter(),
            query_context,
            Table,
            options.relations,
            options.field_infos,
            options.columns,
            options.order_clauses,
            options.distinct,
            options.where_clauses,
            options.group_by,
            options.having_clauses,
        ),

        pub const info = .{
            .Table = Table,
            .Schema = Schema,
        };
        pub const Definition = Table.Definition;
        pub const ResultContext = options.result_context;
        pub const ResultType = QueryResultType();
        pub const ColumnInfos = QueryColumnInfos();
        pub const relations = options.relations;

        const Self = @This();

        pub fn extend(
            self: Self,
            S: type,
            args: anytype,
            comptime context: jetquery.fields.FieldContext,
        ) S {
            validateQueryContext(self.query_context, query_context);

            var statement: S = undefined;

            inline for (0..self.field_infos.len) |index| {
                const value = self.field_values[index];
                @field(statement.field_values, std.fmt.comptimePrint("{}", .{index})) = value;
                statement.field_errors[index] = self.field_errors[index];
            }

            const tree = sql.Where.tree(
                Adapter(),
                Table,
                options.relations,
                @TypeOf(args),
                context,
                self.field_infos.len,
            );
            const clause_values = tree.values(args);

            const arg_values = clause_values.values;
            const arg_errors = clause_values.errors;

            inline for (options.field_infos.len.., arg_values, arg_errors) |field_index, value, err| {
                @field(statement.field_values, std.fmt.comptimePrint("{d}", .{field_index})) = value;

                statement.field_errors[field_index] = err;
            }

            if (comptime hasTimestamps(Table)) {
                updateTimestamps(S, &statement, statement.field_infos, statement.query_context);
            }

            return statement;
        }

        pub fn select(
            self: Self,
            comptime select_columns: anytype,
        ) Statement(.select, Schema, Table, .{
            .relations = options.relations,
            .field_infos = options.field_infos,
            .columns = &jetquery.columns.translate(Table, options.relations, select_columns),
            .order_clauses = options.order_clauses,
            .group_by = options.group_by,
            .distinct = options.distinct,
            .result_context = if (options.default_select) .many else options.result_context,
        }) {
            const S = Statement(.select, Schema, Table, .{
                .relations = options.relations,
                .field_infos = options.field_infos,
                .columns = &jetquery.columns.translate(Table, options.relations, select_columns),
                .order_clauses = options.order_clauses,
                .group_by = options.group_by,
                .distinct = options.distinct,
                .result_context = if (options.default_select) .many else options.result_context,
            });
            return self.extend(S, .{}, .none);
        }

        /// Apply a `WHERE` clause to the current statement.
        pub fn where(self: Self, args: anytype) Statement(
            switch (query_context) {
                .none => .select,
                else => |tag| tag,
            },
            Schema,
            Table,
            .{
                .relations = options.relations,
                .where_clauses = options.where_clauses ++ .{
                    sql.Where.tree(
                        Adapter(),
                        Table,
                        options.relations,
                        @TypeOf(args),
                        .where,
                        options.field_infos.len,
                    ),
                },
                .field_infos = options.field_infos ++ jetquery.fields.fieldInfos(
                    Adapter(),
                    Table,
                    options.relations,
                    @TypeOf(args),
                    .where,
                ),
                .columns = if (options.default_select) &Table.columns() else options.columns,
                .order_clauses = options.order_clauses,
                .group_by = options.group_by,
                .distinct = options.distinct,
                .result_context = switch (options.result_context) {
                    .none => switch (query_context) {
                        .none => defaultResultContext(.select),
                        else => |tag| defaultResultContext(tag),
                    },
                    else => |tag| tag,
                },
                .default_select = options.default_select,
            },
        ) {
            const S = Statement(switch (query_context) {
                .none => .select,
                else => |tag| tag,
            }, Schema, Table, .{
                .relations = options.relations,
                .field_infos = options.field_infos ++ jetquery.fields.fieldInfos(
                    Adapter(),
                    Table,
                    options.relations,
                    @TypeOf(args),
                    .where,
                ),
                .where_clauses = options.where_clauses ++ .{sql.Where.tree(
                    Adapter(),
                    Table,
                    options.relations,
                    @TypeOf(args),
                    .where,
                    options.field_infos.len,
                )},
                .columns = if (options.default_select) &Table.columns() else options.columns,
                .order_clauses = options.order_clauses,
                .group_by = options.group_by,
                .distinct = options.distinct,
                .result_context = switch (options.result_context) {
                    .none => switch (query_context) {
                        .none => defaultResultContext(.select),
                        else => |tag| defaultResultContext(tag),
                    },
                    else => |tag| tag,
                },
                .default_select = options.default_select,
            });
            return self.extend(S, args, .where);
        }

        pub fn find(self: Self, id: anytype) Statement(.select, Schema, Table, .{
            .field_infos = &(jetquery.fields.fieldInfos(
                Adapter(),
                Table,
                options.relations,
                @TypeOf(.{ .id = id }),
                .where,
            ) ++
                jetquery.fields.fieldInfos(Adapter(), Table, options.relations, @TypeOf(.{1}), .limit)),
            .where_clauses = &.{sql.Where.tree(
                Adapter(),
                Table,
                &.{},
                @TypeOf(.{ .id = id }),
                .where,
                options.field_infos.len,
            )},
            .columns = if (options.columns.len == 0) &Table.columns() else options.columns,
            .result_context = .one,
        }) {
            // No need to verify `id` presence as `jetquery.fields.fieldInfos` will reject unknown fields.
            return self.findBy(.{ .id = id });
        }

        pub fn findBy(self: Self, args: anytype) Statement(.select, Schema, Table, .{
            .relations = options.relations,
            .field_infos = options.field_infos ++
                jetquery.fields.fieldInfos(Adapter(), Table, options.relations, @TypeOf(args), .where) ++
                jetquery.fields.fieldInfos(Adapter(), Table, options.relations, @TypeOf(.{1}), .limit),
            .columns = if (options.columns.len == 0) &Table.columns() else options.columns,
            .where_clauses = &.{sql.Where.tree(
                Adapter(),
                Table,
                &.{},
                @TypeOf(args),
                .where,
                options.field_infos.len,
            )},
            .group_by = options.group_by,
            .result_context = .one,
        }) {
            const S = Statement(.select, Schema, Table, .{
                .relations = options.relations,
                .field_infos = options.field_infos ++
                    jetquery.fields.fieldInfos(Adapter(), Table, options.relations, @TypeOf(args), .where) ++
                    jetquery.fields.fieldInfos(Adapter(), Table, options.relations, @TypeOf(.{1}), .limit),
                .columns = if (options.columns.len == 0) &Table.columns() else options.columns,
                .where_clauses = &.{sql.Where.tree(Adapter(), Table, &.{}, @TypeOf(args), .where, options.field_infos.len)},
                .group_by = options.group_by,
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
            .distinct = options.distinct,
            .where_clauses = options.where_clauses,
            .group_by = options.group_by,
        }) {
            const S = Statement(.count, Schema, Table, .{
                .relations = options.relations,
                .field_infos = options.field_infos,
                .distinct = options.distinct,
                .where_clauses = options.where_clauses,
                .group_by = options.group_by,
            });
            return self.extend(S, .{}, .none);
        }

        pub fn distinct(self: Self, comptime args: anytype) Statement(.select, Schema, Table, .{
            .relations = options.relations,
            .field_infos = options.field_infos,
            .columns = &.{},
            .order_clauses = options.order_clauses,
            .result_context = .many,
            .distinct = &jetquery.columns.translate(Table, options.relations, args),
            .where_clauses = options.where_clauses,
            .group_by = options.group_by,
        }) {
            const S = Statement(.select, Schema, Table, .{
                .relations = options.relations,
                .field_infos = options.field_infos,
                .columns = &.{},
                .order_clauses = options.order_clauses,
                .result_context = .many,
                .distinct = &jetquery.columns.translate(Table, options.relations, args),
                .where_clauses = options.where_clauses,
                .group_by = options.group_by,
            });
            if (!options.default_select) @compileError(
                std.fmt.comptimePrint(
                    "Failed attempting to set distinct columns when `select` already invoked. " ++
                        "Use `Model.distinct(...)` to issue a distinct query.",
                    .{},
                ),
            );

            return self.extend(S, .{}, .none);
        }
        pub fn update(self: Self, args: anytype) Statement(.update, Schema, Table, .{
            .field_infos = &(jetquery.fields.fieldInfos(Adapter(), Table, &.{}, @TypeOf(args), .update) ++
                timestampsFields(Table, .update)),
        }) {
            const S = Statement(.update, Schema, Table, .{
                .field_infos = &(jetquery.fields.fieldInfos(Adapter(), Table, &.{}, @TypeOf(args), .update) ++
                    timestampsFields(Table, .update)),
            });
            return self.extend(S, args, .update);
        }

        pub fn insert(self: Self, args: anytype) Statement(.insert, Schema, Table, .{
            .field_infos = &(jetquery.fields.fieldInfos(Adapter(), Table, &.{}, @TypeOf(args), .insert) ++
                timestampsFields(Table, .insert)),
        }) {
            const S = Statement(.insert, Schema, Table, .{
                .field_infos = &(jetquery.fields.fieldInfos(Adapter(), Table, &.{}, @TypeOf(args), .insert) ++
                    timestampsFields(Table, .insert)),
            });
            return self.extend(S, args, .insert);
        }

        pub fn delete(self: Self) Statement(.delete, Schema, Table, .{
            .field_infos = &jetquery.fields.fieldInfos(Adapter(), Table, &.{}, @TypeOf(.{}), .none),
            .where_clauses = options.where_clauses,
        }) {
            // TODO: Add support for `DELETE ... USING ...`
            if (comptime options.relations.len != 0) @compileError(
                "Failed attempting to generate `DELETE` query with relations. " ++
                    "This error occurred to prevent accidential deletion of potentially unexpected behaviour.",
            );
            const S = Statement(.delete, Schema, Table, .{
                .field_infos = &jetquery.fields.fieldInfos(Adapter(), Table, &.{}, @TypeOf(.{}), .none),
                .where_clauses = options.where_clauses,
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
            .field_infos = options.field_infos ++ jetquery.fields.fieldInfos(Adapter(), Table, &.{}, @TypeOf(.{bound}), .limit),
            .columns = options.columns,
            .order_clauses = options.order_clauses,
            .result_context = options.result_context,
            .where_clauses = options.where_clauses,
            .group_by = options.group_by,
            .having_clauses = options.having_clauses,
        }) {
            const S = Statement(query_context, Schema, Table, .{
                .relations = options.relations,
                .field_infos = options.field_infos ++ jetquery.fields.fieldInfos(Adapter(), Table, &.{}, @TypeOf(.{bound}), .limit),
                .columns = options.columns,
                .order_clauses = options.order_clauses,
                .result_context = options.result_context,
                .where_clauses = options.where_clauses,
                .group_by = options.group_by,
                .having_clauses = options.having_clauses,
            });
            return self.extend(S, .{bound}, .limit);
        }

        pub fn orderBy(self: Self, comptime args: anytype) Statement(query_context, Schema, Table, .{
            .relations = options.relations,
            .field_infos = options.field_infos,
            .columns = options.columns,
            .order_clauses = &translateOrderBy(Table, args),
            .result_context = options.result_context,
            .where_clauses = options.where_clauses,
            .group_by = options.group_by,
            .having_clauses = options.having_clauses,
        }) {
            const S = Statement(query_context, Schema, Table, .{
                .relations = options.relations,
                .field_infos = options.field_infos,
                .columns = options.columns,
                .order_clauses = &translateOrderBy(Table, args),
                .result_context = options.result_context,
                .where_clauses = options.where_clauses,
                .group_by = options.group_by,
                .having_clauses = options.having_clauses,
            });
            return self.extend(S, .{}, .order);
        }

        pub fn groupBy(self: Self, comptime columns: anytype) Statement(.select, Schema, Table, .{
            .relations = options.relations,
            .field_infos = options.field_infos,
            .columns = options.columns,
            .order_clauses = options.order_clauses,
            .result_context = .many,
            .where_clauses = options.where_clauses,
            .group_by = &jetquery.columns.translate(Table, options.relations, columns),
            .having_clauses = options.having_clauses,
        }) {
            const S = Statement(.select, Schema, Table, .{
                .relations = options.relations,
                .field_infos = options.field_infos,
                .columns = options.columns,
                .order_clauses = options.order_clauses,
                .group_by = &jetquery.columns.translate(Table, options.relations, columns),
                .result_context = .many,
                .where_clauses = options.where_clauses,
                .having_clauses = options.having_clauses,
            });
            return self.extend(S, .{}, .none);
        }

        pub fn having(self: Self, args: anytype) Statement(.select, Schema, Table, .{
            .relations = options.relations,
            .field_infos = options.field_infos ++ jetquery.fields.fieldInfos(
                Adapter(),
                Table,
                options.relations,
                @TypeOf(args),
                .where,
            ),
            .columns = options.columns,
            .order_clauses = options.order_clauses,
            .result_context = .many,
            .where_clauses = options.where_clauses,
            .group_by = options.group_by,
            .having_clauses = options.having_clauses ++
                .{sql.Where.tree(
                Adapter(),
                Table,
                options.relations,
                @TypeOf(args),
                .where,
                options.field_infos.len,
            )},
        }) {
            const S = Statement(.select, Schema, Table, .{
                .relations = options.relations,
                .field_infos = options.field_infos ++ jetquery.fields.fieldInfos(
                    Adapter(),
                    Table,
                    options.relations,
                    @TypeOf(args),
                    .where,
                ),
                .columns = options.columns,
                .order_clauses = options.order_clauses,
                .result_context = .many,
                .where_clauses = options.where_clauses,
                .group_by = options.group_by,
                .having_clauses = options.having_clauses ++
                    .{sql.Where.tree(
                    Adapter(),
                    Table,
                    options.relations,
                    @TypeOf(args),
                    .where,
                    options.field_infos.len,
                )},
            });
            return self.extend(S, args, .none);
        }

        pub fn include(
            self: Self,
            comptime name: jetquery.relation.RelationsEnum(Table),
            comptime select_columns: anytype,
        ) Statement(switch (query_context) {
            .none => .select,
            else => |tag| tag,
        }, Schema, Table, .{
            .relations = options.relations ++
                .{jetquery.relation.Relation(Schema, Table, name, select_columns, .include)},
            .field_infos = options.field_infos,
            .columns = switch (query_context) {
                .none => &Table.columns(),
                else => options.columns,
            },
            .order_clauses = options.order_clauses,
            .result_context = switch (options.result_context) {
                .none => .many,
                else => |tag| tag,
            },
            .default_select = options.default_select,
            .where_clauses = options.where_clauses,
            .having_clauses = options.having_clauses,
        }) {
            const S = Statement(switch (query_context) {
                .none => .select,
                else => |tag| tag,
            }, Schema, Table, .{
                .relations = options.relations ++
                    .{jetquery.relation.Relation(Schema, Table, name, select_columns, .include)},
                .field_infos = options.field_infos,
                .columns = switch (query_context) {
                    .none => &Table.columns(),
                    else => options.columns,
                },
                .order_clauses = options.order_clauses,
                .result_context = switch (options.result_context) {
                    .none => .many,
                    else => |tag| tag,
                },
                .default_select = options.default_select,
                .where_clauses = options.where_clauses,
                .having_clauses = options.having_clauses,
            });
            return self.extend(S, .{}, .none);
        }

        pub fn join(
            self: Self,
            comptime join_context: jetquery.relation.JoinContext,
            comptime name: jetquery.relation.RelationsEnum(Table),
        ) Statement(
            switch (query_context) {
                .none => .select,
                else => |tag| tag,
            },
            Schema,
            Table,
            .{
                .relations = options.relations ++
                    .{jetquery.relation.Relation(Schema, Table, name, null, join_context)},
                .field_infos = options.field_infos,
                .columns = switch (query_context) {
                    .none => &Table.columns(),
                    else => options.columns,
                },
                .order_clauses = options.order_clauses,
                .result_context = switch (options.result_context) {
                    .none => .many,
                    else => |tag| tag,
                },
                .default_select = options.default_select,
                .where_clauses = options.where_clauses,
                .having_clauses = options.having_clauses,
            },
        ) {
            const S = Statement(switch (query_context) {
                .none => .select,
                else => |tag| tag,
            }, Schema, Table, .{
                .relations = options.relations ++
                    .{jetquery.relation.Relation(Schema, Table, name, null, join_context)},
                .field_infos = options.field_infos,
                .columns = switch (query_context) {
                    .none => &Table.columns(),
                    else => options.columns,
                },
                .order_clauses = options.order_clauses,
                .result_context = switch (options.result_context) {
                    .none => .many,
                    else => |tag| tag,
                },
                .default_select = options.default_select,
                .where_clauses = options.where_clauses,
                .having_clauses = options.having_clauses,
            });
            return self.extend(S, .{}, .none);
        }

        pub fn execute(self: Self, repo: *jetquery.Repo) !switch (options.result_context) {
            .one => ?ResultType,
            .many => jetquery.Result,
            .none => void,
        } {
            const caller_info = try jetquery.debug.getCallerInfo(@returnAddress());
            return try repo.executeInternal(self, caller_info);
        }

        pub fn all(self: Self, repo: *jetquery.Repo) ![]ResultType {
            var result = try repo.executeInternal(
                self,
                try jetquery.debug.getCallerInfo(@returnAddress()),
            );
            return try result.all(self);
        }

        pub fn values(self: Self) jetquery.fields.FieldValues(Table, options.relations, options.field_infos) {
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

        fn auxiliaryQueries() [auxiliaryQueriesSize()]AuxiliaryQuery {
            comptime {
                var queries: [auxiliaryQueriesSize()]AuxiliaryQuery = undefined;
                var index: usize = 0;

                for (options.relations) |Relation| {
                    if (Relation.relation_type != .has_many) continue;

                    queries[index] = .{
                        .query = Query(Schema, Relation.Source),
                        .relation = Relation,
                    };
                    index += 1;
                }

                return queries;
            }
        }

        pub fn auxiliaryQueriesSize() usize {
            comptime {
                var size: usize = 0;
                for (options.relations) |Relation| {
                    if (Relation.relation_type != .has_many) continue;

                    size += 1;
                }
                return size;
            }
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
                        .name = column.alias orelse column.name,
                        .type = column.ResultType(Adapter()),
                        .index = index,
                        .relation = null,
                    };
                }
                var start: usize = options.columns.len;
                for (options.relations) |Relation| {
                    if (Relation.relation_type != .belongs_to) continue;

                    for (Relation.select_columns, start..) |column, index| {
                        column_infos[index] = .{
                            .name = column.alias orelse column.name,
                            .type = column.ResultType(Adapter()),
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
                    .count => return Adapter().Aggregate(.count),
                    else => {},
                }

                var base_fields: [options.columns.len]std.builtin.Type.StructField = undefined;

                for (options.columns, 0..) |column, index| {
                    base_fields[index] = jetquery.fields.structField(
                        column.alias orelse column.name,
                        column.ResultType(Adapter()),
                    );
                }

                var relations_fields: [options.relations.len]std.builtin.Type.StructField = undefined;
                for (options.relations, 0..) |Relation, relation_index| {
                    var relation_fields: [Relation.select_columns.len]std.builtin.Type.StructField = undefined;
                    for (Relation.select_columns, 0..) |column, index| {
                        relation_fields[index] = jetquery.fields.structField(
                            column.alias orelse column.name,
                            column.ResultType(Adapter()),
                        );
                    }

                    const RelationBaseType = @Type(.{ .@"struct" = .{
                        .layout = .auto,
                        .fields = &(relation_fields ++ internalFields(&relation_fields, false)),
                        .decls = &.{},
                        .is_tuple = false,
                    } });

                    const RelationType = switch (Relation.relation_type) {
                        .belongs_to => RelationBaseType,
                        .has_many => []const RelationBaseType,
                    };

                    const relation_field = jetquery.fields.structField(
                        Relation.relation_name,
                        RelationType,
                    );

                    relations_fields[relation_index] = relation_field;
                }

                const all_fields = base_fields ++
                    relations_fields ++
                    internalFields(&base_fields, true);

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
                    if (Relation.relation_type != .belongs_to) continue;

                    len += Relation.select_columns.len;
                }
                return len;
            }
        }

        fn internalFields(
            comptime base_fields: []const std.builtin.Type.StructField,
            comptime with_relations: bool,
        ) [4]std.builtin.Type.StructField {
            comptime {
                const Originals = @Type(.{ .@"struct" = .{
                    .layout = .auto,
                    .fields = base_fields,
                    .decls = &.{},
                    .is_tuple = false,
                } });

                const jetquery_fields: [2]std.builtin.Type.StructField = .{
                    jetquery.fields.structField("id", i128),
                    jetquery.fields.structField("original_values", Originals),
                    // jetquery.fields.structField("relation_names", [options.relations.len][]const u8),
                };

                const JQ = @Type(.{ .@"struct" = .{
                    .layout = .auto,
                    .fields = &jetquery_fields,
                    .decls = &.{},
                    .is_tuple = false,
                } });

                const len = if (with_relations) options.relations.len else 0;
                var relation_names: [len][]const u8 = undefined;
                if (with_relations) {
                    for (options.relations, 0..) |relation, index| {
                        relation_names[index] = relation.relation_name;
                    }
                }

                const relation_field = std.builtin.Type.StructField{
                    .name = "__jetquery_relation_names",
                    .type = [len][]const u8,
                    .default_value = @ptrCast(&relation_names),
                    .is_comptime = true,
                    .alignment = @alignOf([options.relations.len][]const u8),
                };

                return .{
                    jetquery.fields.structField("__jetquery", JQ),
                    // Sadly we can't store these inside the nested value as Zig doesn't figure
                    // out that the values are available at comptime.
                    jetquery.fields.structFieldComptime("__jetquery_model", Table),
                    jetquery.fields.structFieldComptime("__jetquery_schema", Schema),
                } ++ .{relation_field};
            }
        }
    };
}

fn translateOrderBy(
    Table: type,
    comptime args: anytype,
) [std.meta.fields(@TypeOf(args)).len]sql.OrderClause {
    comptime {
        var clauses: [std.meta.fields(@TypeOf(args)).len]sql.OrderClause = undefined;
        const is_tuple = @typeInfo(@TypeOf(args)).@"struct".is_tuple;
        const fields = std.meta.fields(@TypeOf(args));

        for (fields, if (is_tuple) args else fields, 0..) |field, arg, index| {
            clauses[index] = if (is_tuple)
                .{
                    .column = Table.column(@tagName(arg)),
                    .direction = .ascending,
                }
            else
                .{
                    .column = Table.column(field.name),
                    .direction = std.enums.nameCast(
                        sql.OrderDirection,
                        @tagName(@field(args, field.name)),
                    ),
                };
        }
        return clauses;
    }
}

fn timestampsFields(
    Table: type,
    comptime query_context: jetquery.fields.FieldContext,
) [timestampsSize(Table, query_context)]jetquery.fields.FieldInfo {
    return if (comptime hasTimestamps(Table)) switch (query_context) {
        .update => .{
            jetquery.fields.fieldInfo(
                jetquery.fields.structField("updated_at", i64),
                Table,
                "updated_at",
                query_context,
            ),
        },
        .insert => .{
            jetquery.fields.fieldInfo(
                jetquery.fields.structField("created_at", i64),
                Table,
                "created_at",
                query_context,
            ),
            jetquery.fields.fieldInfo(
                jetquery.fields.structField("updated_at", i64),
                Table,
                "updated_at",
                query_context,
            ),
        },
        else => @compileError(
            "Timestamps detection not relevant for `" ++ @tagName(query_context) ++ "` query. (This is a bug).",
        ),
    } else .{};
}

fn hasTimestamps(Table: type) bool {
    return @hasField(Table.Definition, jetquery.default_column_names.created_at) and
        @hasField(Table.Definition, jetquery.default_column_names.updated_at);
}

fn timestampsSize(Table: type, comptime query_context: jetquery.fields.FieldContext) u2 {
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
    comptime field_infos: []const jetquery.fields.FieldInfo,
    comptime context: sql.QueryContext,
) void {
    _ = field_infos;
    switch (comptime context) {
        .update => {
            const timestamp = now();
            inline for (statement.field_infos, 0..) |field_info, index| {
                if (comptime std.mem.eql(u8, field_info.name, jetquery.default_column_names.updated_at)) {
                    @field(statement.field_values, std.fmt.comptimePrint("{}", .{index})) = timestamp;
                    statement.field_errors[index] = null;
                }
            }
        },
        .insert => {
            const timestamp = now();
            inline for (statement.field_infos, 0..) |field_info, index| {
                if (comptime std.mem.eql(u8, field_info.name, jetquery.default_column_names.updated_at)) {
                    @field(statement.field_values, std.fmt.comptimePrint("{}", .{index})) = timestamp;
                    statement.field_errors[index] = null;
                } else if (comptime std.mem.eql(u8, field_info.name, jetquery.default_column_names.created_at)) {
                    @field(statement.field_values, std.fmt.comptimePrint("{}", .{index})) = timestamp;
                    statement.field_errors[index] = null;
                }
            }
        },
        else => {},
    }
}
