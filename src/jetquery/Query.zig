const std = @import("std");

const jetcommon = @import("jetcommon");

const jetquery = @import("../jetquery.zig");

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
            .field_infos = &jetquery.fields.fieldInfos(@TypeOf(args), .where),
            .columns = &Table.columns(),
            .default_select = true,
        }) {
            return InitialStatement(Schema, Table).where(args);
        }

        /// Create an `UPDATE` query with the specified `args`, e.g.:
        /// ```zig
        /// Query(Schema, .MyTable).update(.{ .foo = "bar", .baz = "qux" }).where(.{ .quux = "corge" });
        /// ```
        pub fn update(args: anytype) Statement(.update, Schema, Table, .{
            .field_infos = &(jetquery.fields.fieldInfos(@TypeOf(args), .update) ++ timestampsFields(Table, .update)),
        }) {
            return InitialStatement(Schema, Table).update(args);
        }

        /// Create an `INSERT` query with the specified `args`, e.g.:
        /// ```zig
        /// Query(Schema, .MyTable).insert(.{ .foo = "bar", .baz = "qux" });
        /// ```
        pub fn insert(args: anytype) Statement(.insert, Schema, Table, .{
            .field_infos = &(jetquery.fields.fieldInfos(@TypeOf(args), .insert) ++ timestampsFields(Table, .insert)),
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
            .field_infos = &(jetquery.fields.fieldInfos(@TypeOf(.{ .id = id }), .where) ++
                jetquery.fields.fieldInfos(@TypeOf(.{1}), .limit)),
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
            .field_infos = &(jetquery.fields.fieldInfos(@TypeOf(args), .where) ++
                jetquery.fields.fieldInfos(@TypeOf(.{1}), .limit)),
            .columns = &Table.columns(),
            .result_context = .one,
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
        ) Statement(.none, Schema, Table, .{
            .relations = &.{
                jetquery.relation.Relation(Schema, Table, name, select_columns),
            },
            .default_select = true,
        }) {
            return InitialStatement(Schema, Table).include(name, select_columns);
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
    };
}

const MissingField = struct {
    missing: void,
};

fn InitialStatement(Schema: type, Table: type) Statement(.none, Schema, Table, .{
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
        result_context: ResultContext = switch (query_context) {
            .select => .many,
            .update, .insert, .delete, .delete_all, .none => .none,
            .count => .one,
        },
        default_select: bool = false,
        distinct: ?[]const jetquery.columns.Column = null,
    };
}

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

        comptime sql: []const u8 = jetquery.sql.render(
            jetquery.adapters.Type(jetquery.config.database.adapter),
            query_context,
            Table,
            options.relations,
            options.field_infos,
            options.columns,
            options.order_clauses,
            options.distinct,
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
            comptime context: jetquery.fields.FieldContext,
        ) S {
            validateQueryContext(self.query_context, query_context);

            var statement: S = undefined;

            inline for (0..self.field_infos.len) |index| {
                const value = self.field_values[index];
                @field(statement.field_values, std.fmt.comptimePrint("{}", .{index})) = value;
                statement.field_errors[index] = self.field_errors[index];
            }

            const arg_values = jetquery.Where.init(@TypeOf(args)).values(args);
            const arg_fields = std.meta.fields(@TypeOf(arg_values));

            inline for (arg_fields, options.field_infos.len.., arg_values) |field, field_index, value| {
                _ = field;
                _ = context;
                // const value = @field(arg_values, std.fmt.comptimePrint("{d}", .{value_index}));
                // TODO: Move to Where
                // const coerced = coercion.coerce(
                //     Table,
                //     options.relations,
                //     jetquery.fields.fieldInfo(field, context),
                //     value,
                // );
                // statement.field_errors[field_index] = coerced.err;
                @field(statement.field_values, std.fmt.comptimePrint("{d}", .{field_index})) = value;

                statement.field_errors[field_index] = null;
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
            .distinct = options.distinct,
            .result_context = if (options.default_select) .many else options.result_context,
        }) {
            const S = Statement(.select, Schema, Table, .{
                .relations = options.relations,
                .field_infos = options.field_infos,
                .columns = &jetquery.columns.translate(Table, options.relations, select_columns),
                .order_clauses = options.order_clauses,
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
                .field_infos = options.field_infos ++ jetquery.fields.fieldInfos(@TypeOf(args), .where),
                .columns = if (options.default_select) &Table.columns() else options.columns,
                .order_clauses = options.order_clauses,
                .distinct = options.distinct,
                .result_context = switch (options.result_context) {
                    .none => .many,
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
                .field_infos = options.field_infos ++ jetquery.fields.fieldInfos(@TypeOf(args), .where),
                .columns = if (options.default_select) &Table.columns() else options.columns,
                .order_clauses = options.order_clauses,
                .distinct = options.distinct,
                .result_context = switch (options.result_context) {
                    .none => .many,
                    else => |tag| tag,
                },
                .default_select = options.default_select,
            });
            return self.extend(S, args, .where);
        }

        pub fn find(self: Self, id: anytype) Statement(.select, Schema, Table, .{
            .field_infos = &(jetquery.fields.fieldInfos(@TypeOf(.{ .id = id }), .where) ++
                jetquery.fields.fieldInfos(@TypeOf(.{1}), .limit)),
            .columns = if (options.columns.len == 0) &Table.columns() else options.columns,
            .result_context = .one,
        }) {
            // No need to verify `id` presence as `jetquery.fields.fieldInfos` will reject unknown fields.
            return self.findBy(.{ .id = id });
        }

        pub fn findBy(self: Self, args: anytype) Statement(.select, Schema, Table, .{
            .relations = options.relations,
            .field_infos = options.field_infos ++
                jetquery.fields.fieldInfos(@TypeOf(args), .where) ++
                jetquery.fields.fieldInfos(@TypeOf(.{1}), .limit),
            .columns = if (options.columns.len == 0) &Table.columns() else options.columns,
            .result_context = .one,
        }) {
            const S = Statement(.select, Schema, Table, .{
                .relations = options.relations,
                .field_infos = options.field_infos ++
                    jetquery.fields.fieldInfos(@TypeOf(args), .where) ++
                    jetquery.fields.fieldInfos(@TypeOf(.{1}), .limit),
                .columns = if (options.columns.len == 0) &Table.columns() else options.columns,
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
        }) {
            const S = Statement(.count, Schema, Table, .{
                .relations = options.relations,
                .field_infos = options.field_infos,
                .distinct = options.distinct,
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
        }) {
            const S = Statement(.select, Schema, Table, .{
                .relations = options.relations,
                .field_infos = options.field_infos,
                .columns = &.{},
                .order_clauses = options.order_clauses,
                .result_context = .many,
                .distinct = &jetquery.columns.translate(Table, options.relations, args),
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
            .field_infos = &(jetquery.fields.fieldInfos(@TypeOf(args), .update) ++
                timestampsFields(Table, .update)),
        }) {
            const S = Statement(.update, Schema, Table, .{
                .field_infos = &(jetquery.fields.fieldInfos(@TypeOf(args), .update) ++
                    timestampsFields(Table, .update)),
            });
            return self.extend(S, args, .update);
        }

        pub fn insert(self: Self, args: anytype) Statement(.insert, Schema, Table, .{
            .field_infos = &(jetquery.fields.fieldInfos(@TypeOf(args), .insert) ++
                timestampsFields(Table, .insert)),
        }) {
            const S = Statement(.insert, Schema, Table, .{
                .field_infos = &(jetquery.fields.fieldInfos(@TypeOf(args), .insert) ++
                    timestampsFields(Table, .insert)),
            });
            return self.extend(S, args, .insert);
        }

        pub fn delete(self: Self) Statement(.delete, Schema, Table, .{
            .field_infos = &jetquery.fields.fieldInfos(@TypeOf(.{}), .none),
        }) {
            // TODO: Add support for `DELETE ... USING ...`
            if (comptime options.relations.len != 0) @compileError(
                "Failed attempting to generate `DELETE` query with relations. " ++
                    "This error occurred to prevent accidential deletion of potentially unexpected behaviour.",
            );
            const S = Statement(.delete, Schema, Table, .{
                .field_infos = &jetquery.fields.fieldInfos(@TypeOf(.{}), .none),
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
            .field_infos = options.field_infos ++ jetquery.fields.fieldInfos(@TypeOf(.{bound}), .limit),
            .columns = options.columns,
            .order_clauses = options.order_clauses,
            .result_context = options.result_context,
        }) {
            const S = Statement(query_context, Schema, Table, .{
                .relations = options.relations,
                .field_infos = options.field_infos ++ jetquery.fields.fieldInfos(@TypeOf(.{bound}), .limit),
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
            comptime select_columns: anytype,
        ) Statement(query_context, Schema, Table, .{
            .relations = options.relations ++
                .{jetquery.relation.Relation(Schema, Table, name, select_columns)},
            .field_infos = options.field_infos,
            .columns = options.columns,
            .order_clauses = options.order_clauses,
            .result_context = options.result_context,
            .default_select = options.default_select,
        }) {
            const S = Statement(query_context, Schema, Table, .{
                .relations = options.relations ++
                    .{jetquery.relation.Relation(Schema, Table, name, select_columns)},
                .field_infos = options.field_infos,
                .columns = options.columns,
                .order_clauses = options.order_clauses,
                .result_context = options.result_context,
                .default_select = options.default_select,
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
                        .name = column.name,
                        .type = column.type,
                        .index = index,
                        .relation = null,
                    };
                }
                var start: usize = options.columns.len;
                for (options.relations) |Relation| {
                    for (Relation.select_columns, start..) |column, index| {
                        column_infos[index] = .{
                            .name = column.name,
                            .type = column.type,
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
                    base_fields[index] = .{
                        .name = column.name ++ "",
                        .type = column.type,
                        .default_value = null,
                        .is_comptime = false,
                        .alignment = @alignOf(column.type),
                    };
                }

                var relations_fields: [options.relations.len]std.builtin.Type.StructField = undefined;
                for (options.relations, 0..) |Relation, relation_index| {
                    var relation_fields: [Relation.select_columns.len]std.builtin.Type.StructField = undefined;
                    for (Relation.select_columns, 0..) |column, index| {
                        relation_fields[index] = .{
                            .name = column.name ++ "",
                            .type = column.type,
                            .default_value = null,
                            .is_comptime = false,
                            .alignment = @alignOf(column.type),
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
) [std.meta.fields(@TypeOf(args)).len]sql.OrderClause {
    comptime {
        var clauses: [std.meta.fields(@TypeOf(args)).len]sql.OrderClause = undefined;

        for (std.meta.fields(@TypeOf(args)), 0..) |field, index| {
            clauses[index] = .{
                .column = Table.column(field.name),
                .direction = std.enums.nameCast(sql.OrderDirection, @tagName(@field(args, field.name))),
            };
        }
        return clauses;
    }
}

fn timestampsFields(
    Table: type,
    comptime query_context: jetquery.fields.FieldContext,
) [timestampsSize(Table, query_context)]jetquery.fields.FieldInfo {
    // TODO: Tidy this up a bit to remove all the repetition
    return if (comptime hasTimestamps(Table)) switch (query_context) {
        .update => .{
            jetquery.fields.fieldInfo(.{
                .name = "updated_at",
                .type = i64,
                .default_value = null,
                .is_comptime = false,
                .alignment = @alignOf(i64),
            }, "updated_at", query_context),
        },
        .insert => .{
            jetquery.fields.fieldInfo(.{
                .name = "created_at",
                .type = i64,
                .default_value = null,
                .is_comptime = false,
                .alignment = @alignOf(i64),
            }, "created_at", query_context),
            jetquery.fields.fieldInfo(.{
                .name = "updated_at",
                .type = i64,
                .default_value = null,
                .is_comptime = false,
                .alignment = @alignOf(i64),
            }, "updated_at", query_context),
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
