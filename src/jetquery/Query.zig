const std = @import("std");

const jetcommon = @import("jetcommon");

const jetquery = @import("../jetquery.zig");

// Available SQL statement types.
pub const QueryType = enum { select, update, insert, delete, delete_all };

// Determines how a field in the generated fields tuple should be used.
pub const FieldContext = enum { where, update, insert, limit, order, none };

// Number of rows expected to be returned by a query.
const ResultContext = enum { one, many, none };

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
        /// Query(MyTable).select(&.{ .foo, .bar }).where(.{ .foo = "qux" });
        /// ```
        /// Pass an empty `columns` array to select all columns:
        /// ```zig
        /// Query(MyTable).select(&.{}).where(.{ .foo = "qux" });
        /// ```
        pub fn select(
            comptime columns: []const std.meta.FieldEnum(Table.Definition),
        ) Statement(
            .select,
            Schema,
            Table,
            &.{},
            &fieldInfos(@TypeOf(.{}), .none),
            if (columns.len == 0) Table.columns() else columns,
            &.{},
            .many,
        ) {
            return Statement(
                .select,
                Schema,
                Table,
                &.{},
                &fieldInfos(@TypeOf(.{}), .none),
                if (columns.len == 0) Table.columns() else columns,
                &.{},
                .many,
            ){ .field_values = .{}, .field_errors = .{} };
        }

        /// Create an `UPDATE` query with the specified `args`, e.g.:
        /// ```zig
        /// Query(MyTable).update(.{ .foo = "bar", .baz = "qux" }).where(.{ .quux = "corge" });
        /// ```
        pub fn update(args: anytype) Statement(
            .update,
            Schema,
            Table,
            &.{},
            &(fieldInfos(@TypeOf(args), .update) ++ timestampsFields(Table, .update)),
            &.{},
            &.{},
            .none,
        ) {
            var statement: Statement(
                .update,
                Schema,
                Table,
                &.{},
                &(fieldInfos(@TypeOf(args), .update) ++ timestampsFields(Table, .update)),
                &.{},
                &.{},
                .none,
            ) = undefined;
            const fields = std.meta.fields(@TypeOf(args));
            inline for (fields, 0..) |field, index| {
                const value = @field(args, field.name);
                const coerced = coerce(Table, &.{}, fieldInfo(field, .update), value);
                @field(statement.field_values, std.fmt.comptimePrint("{}", .{index})) = coerced.value;
                statement.field_errors[index] = coerced.err;
            }
            if (comptime hasTimestamps(Table)) {
                @field(statement.field_values, std.fmt.comptimePrint("{}", .{fields.len})) = now();
                statement.field_errors[fields.len] = null;
            }
            return statement;
        }

        /// Create an `INSERT` query with the specified `args`, e.g.:
        /// ```zig
        /// Query(MyTable).insert(.{ .foo = "bar", .baz = "qux" });
        /// ```
        pub fn insert(args: anytype) Statement(
            .insert,
            Schema,
            Table,
            &.{},
            &(fieldInfos(@TypeOf(args), .insert) ++ timestampsFields(Table, .insert)),
            &.{},
            &.{},
            .none,
        ) {
            var statement: Statement(
                .insert,
                Schema,
                Table,
                &.{},
                &(fieldInfos(@TypeOf(args), .insert) ++ timestampsFields(Table, .insert)),
                &.{},
                &.{},
                .none,
            ) = undefined;

            const fields = std.meta.fields(@TypeOf(args));

            inline for (fields, 0..) |field, index| {
                const value = @field(args, field.name);
                const coerced = coerce(Table, &.{}, fieldInfo(field, .insert), value);
                @field(statement.field_values, std.fmt.comptimePrint("{}", .{index})) = coerced.value;
                statement.field_errors[index] = coerced.err;
            }
            if (comptime hasTimestamps(Table)) {
                const timestamp = now();
                @field(statement.field_values, std.fmt.comptimePrint("{}", .{fields.len})) = timestamp;
                statement.field_errors[fields.len] = null;
                @field(statement.field_values, std.fmt.comptimePrint("{}", .{fields.len + 1})) = timestamp;
                statement.field_errors[fields.len + 1] = null;
            }
            return statement;
        }

        /// Create a `DELETE` query. As a safety measure, a `delete()` query **must** have a
        /// `.where()` clause attached or it will not be executed. Use `deleteAll()` if you wish
        /// to delete all records.
        /// ```zig
        /// Query(MyTable).delete().where(.{ .foo = "bar" });
        /// ```
        pub fn delete() Statement(
            .delete,
            Schema,
            Table,
            &.{},
            &fieldInfos(@TypeOf(.{}), .none),
            &.{},
            &.{},
            .none,
        ) {
            return Statement(
                .delete,
                Schema,
                Table,
                &.{},
                &fieldInfos(@TypeOf(.{}), .none),
                &.{},
                &.{},
                .none,
            ){ .field_values = .{}, .field_errors = .{} };
        }

        /// Create a `DELETE` query that does not require a `WHERE` clause to delete all records
        /// from a table.
        /// ```zig
        /// Query(MyTable).deleteAll();
        /// ```
        pub fn deleteAll() Statement(
            .delete_all,
            Schema,
            Table,
            &.{},
            &fieldInfos(@TypeOf(.{}), .none),
            &.{},
            &.{},
            .none,
        ) {
            return Statement(
                .delete_all,
                Schema,
                Table,
                &.{},
                &fieldInfos(@TypeOf(.{}), .none),
                &.{},
                &.{},
                .none,
            ){ .field_values = .{}, .field_errors = .{} };
        }

        /// Create a `SELECT` query to return a single row matching the given ID.
        /// ```zig
        /// Query(MyTable).find(1000);
        /// ```
        /// Short-hand for:
        /// ```zig
        /// Query(MyTable).select(&.{}).where(.{ .id = id }).limit(1);
        /// ```
        pub fn find(id: anytype) Statement(
            .select,
            Schema,
            Table,
            &.{},
            &(fieldInfos(@TypeOf(.{ .id = id }), .where) ++ fieldInfos(@TypeOf(.{1}), .limit)),
            Table.columns(),
            &.{},
            .one,
        ) {
            var statement: Statement(
                .select,
                Schema,
                Table,
                &.{},
                &(fieldInfos(@TypeOf(.{ .id = id }), .where) ++ fieldInfos(@TypeOf(.{1}), .limit)),
                Table.columns(),
                &.{},
                .one,
            ) = undefined;
            if (comptime @hasField(Table.Definition, "id")) {
                const coerced = coerce(
                    Table,
                    &.{},
                    fieldInfo(std.meta.fieldInfo(Table.Definition, .id), .where),
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
        /// Query(MyTable).findBy(.{ .foo = "bar", .baz = "qux" });
        /// ```
        /// Short-hand for:
        /// ```zig
        /// Query(MyTable).select(&.{}).where(args).limit(1);
        /// ```
        pub fn findBy(args: anytype) Statement(
            .select,
            Schema,
            Table,
            &.{},
            &(fieldInfos(@TypeOf(args), .where) ++ fieldInfos(@TypeOf(.{1}), .limit)),
            Table.columns(),
            &.{},
            .one,
        ) {
            var statement: Statement(
                .select,
                Schema,
                Table,
                &.{},
                &(fieldInfos(@TypeOf(args), .where) ++ fieldInfos(@TypeOf(.{1}), .limit)),
                Table.columns(),
                &.{},
                .one,
            ) = undefined;
            const fields = std.meta.fields(@TypeOf(args));
            inline for (fields, 0..) |field, index| {
                const value = @field(args, field.name);
                const coerced = coerce(Table, &.{}, fieldInfo(field, .where), value);
                @field(statement.field_values, std.fmt.comptimePrint("{}", .{index})) = coerced.value;
                statement.field_errors[index] = coerced.err;
            }
            statement.field_values[fields.len] = 1;
            statement.field_errors[fields.len] = null;
            return statement;
        }
    };
}

const MissingField = struct {
    missing: void,
};

fn ColumnType(Table: type, relations: []const type, comptime field_info: FieldInfo) type {
    switch (field_info.context) {
        .limit => return usize,
        else => {},
    }

    if (comptime @hasField(Table.Definition, field_info.name)) {
        const FT = std.meta.FieldType(
            Table.Definition,
            std.enums.nameCast(std.meta.FieldEnum(Table.Definition), field_info.name),
        );
        if (FT == jetcommon.types.DateTime) return i64 else return FT;
    } else {
        for (relations) |Relation| {
            if (comptime @hasField(Relation.Source.Definition, field_info.name)) {
                const FT = std.meta.FieldType(
                    Relation.Source.Definition,
                    std.enums.nameCast(std.meta.FieldEnum(Relation.Source.Definition), field_info.name),
                );
                if (FT == jetcommon.types.DateTime) return i64 else return FT;
            }
        }

        @compileError(std.fmt.comptimePrint(
            "No column `{s}` defined in Schema for `{s}`.",
            .{ field_info.name, Table.table_name },
        ));
    }
}

fn FieldValues(Table: type, relations: []const type, comptime fields: []const FieldInfo) type {
    var new_fields: [fields.len]std.builtin.Type.StructField = undefined;
    for (fields, 0..) |field, index| {
        new_fields[index] = .{
            .name = std.fmt.comptimePrint("{}", .{index}),
            .type = ColumnType(Table, relations, field),
            .default_value = null,
            .is_comptime = false,
            .alignment = @alignOf(ColumnType(Table, relations, field)),
        };
    }
    return @Type(.{
        .@"struct" = .{
            .layout = .auto,
            .fields = &new_fields,
            .decls = &.{},
            .is_tuple = true,
        },
    });
}

fn canCoerceDelegate(T: type) bool {
    return switch (@typeInfo(T)) {
        .@"struct", .@"union" => @hasDecl(T, "toJetQuery") and @typeInfo(@TypeOf(T.toJetQuery)) == .@"fn",
        .pointer => |info| @hasDecl(info.child, "toJetQuery") and @typeInfo(@TypeOf(T.toJetQuery)) == .@"fn",
        else => false,
    };
}

// Call `toJetQuery` with a given type and an allocator on the given arg field. Although
// this function expects a return value not specific to JetQuery, the intention is that
// arbitrary types can implement `toJetQuery` if the author wants them to be used with
// JetQuery, otherwise a typical Zig compile error will occur. This feature is used by
// Zmpl for converting Zmpl Values, allowing e.g. request params in Jetzig to be used as
// JetQuery whereclause/etc. params.
fn coerceDelegate(Target: type, value: anytype) CoercedValue(Target) {
    const Source = @TypeOf(value);
    if (comptime canCoerceDelegate(Source)) {
        const coerced = value.toJetQuery(Target) catch |err| {
            return .{ .err = err };
        };
        return .{ .value = coerced, .err = null };
    } else {
        @compileError("Incompatible types: `" ++ @typeName(Target) ++ "` and `" ++ @typeName(Source) ++ "`");
    }
}

fn initStatement(
    Table: type,
    relations: []const type,
    C: type,
    statement: *C,
    S: type,
    self: S,
    field_infos: []const FieldInfo,
    args: anytype,
    comptime context: FieldContext,
) void {
    inline for (0..field_infos.len) |index| {
        const value = self.field_values[index];
        @field(statement.field_values, std.fmt.comptimePrint("{}", .{index})) = value;
        statement.field_errors[index] = self.field_errors[index];
    }
    inline for (std.meta.fields(@TypeOf(args)), field_infos.len..) |field, index| {
        const value = @field(args, field.name);
        const coerced = coerce(Table, relations, fieldInfo(field, context), value);
        @field(statement.field_values, std.fmt.comptimePrint("{}", .{index})) = coerced.value;
        statement.field_errors[index] = coerced.err;
    }
}

fn CoercedValue(Target: type) type {
    return struct {
        value: Target = undefined, // Never used if `err` is present
        err: ?anyerror = null,
    };
}

fn coerce(
    Table: type,
    relations: []const type,
    field_info: FieldInfo,
    value: anytype,
) CoercedValue(ColumnType(Table, relations, field_info)) {
    switch (field_info.context) {
        .limit => return switch (@typeInfo(@TypeOf(value))) {
            .int, .comptime_int => .{ .value = value },
            else => coerceDelegate(usize, value),
        },
        else => {},
    }

    const T = ColumnType(Table, relations, field_info);

    if (T == jetcommon.types.DateTime) return value.microseconds;

    return switch (@typeInfo(@TypeOf(value))) {
        .int, .comptime_int => switch (@typeInfo(T)) {
            .int => .{ .value = value },
            else => coerceDelegate(T, value),
        },
        .float, .comptime_float => switch (@typeInfo(T)) {
            .float => .{ .value = value },
            else => coerceDelegate(T, value),
        },
        .pointer => |info| switch (@typeInfo(T)) {
            .int => switch (@typeInfo(info.child)) {
                .int => switch (info.size) {
                    .Slice => coerceInt(T, value),
                    else => .{ .value = value },
                },
                else => if (comptime canCoerceDelegate(info.child))
                    coerceDelegate(T, value.*)
                else
                    coerceInt(T, value),
            },
            .float => switch (@typeInfo(info.child)) {
                .float => .{ .value = value },
                else => if (comptime canCoerceDelegate(info.child))
                    coerceDelegate(T, value.*)
                else
                    coerceFloat(T, value),
            },
            .bool => switch (@typeInfo(info.child)) {
                .bool => .{ .value = value },
                else => if (comptime canCoerceDelegate(info.child))
                    coerceDelegate(T, value.*)
                else
                    coerceBool(T, value),
            },
            .pointer => if (comptime canCoerceDelegate(info.child))
                coerceDelegate(T, value.*)
            else
                .{ .value = value }, // TODO
            else => if (comptime canCoerceDelegate(info.child))
                coerceDelegate(T, value.*)
            else
                @compileError("Incompatible types: `" ++
                    @typeName(T) ++ "` and `" ++ @typeName(info.child) ++ "`"),
        },
        else => coerceDelegate(T, value),
    };
}

fn coerceInt(T: type, value: []const u8) CoercedValue(T) {
    const coerced = std.fmt.parseInt(T, value, 10) catch |err| {
        return .{
            .err = switch (err) {
                error.InvalidCharacter, error.Overflow => error.JetQueryInvalidIntegerString,
            },
        };
    };
    return .{ .value = coerced };
}

fn coerceFloat(T: type, value: []const u8) CoercedValue(T) {
    const coerced = std.fmt.parseFloat(T, value) catch |err| {
        return .{
            .err = switch (err) {
                error.InvalidCharacter => error.JetQueryInvalidFloatString,
            },
        };
    };
    return .{ .value = coerced };
}

fn coerceBool(T: type, value: []const u8) CoercedValue(T) {
    if (value.len != 1) return .{ .err = error.JetQueryInvalidBooleanString };

    const maybe_boolean = switch (value[0]) {
        '1' => true,
        '0' => false,
        else => null,
    };

    return if (maybe_boolean) |boolean|
        .{ .value = boolean }
    else
        .{ .err = error.JetQueryInvalidBooleanString };
}

pub const FieldInfo = struct {
    info: std.builtin.Type.StructField,
    name: []const u8,
    context: FieldContext,
};

pub fn OrderClause(Table: type) type {
    return struct {
        column: std.meta.FieldEnum(Table.Definition),
        direction: OrderDirection,
    };
}

const OrderDirection = enum { ascending, descending };

fn fieldInfos(T: type, comptime context: FieldContext) [std.meta.fields(T).len]FieldInfo {
    var value_fields: [std.meta.fields(T).len]FieldInfo = undefined;
    for (std.meta.fields(T), 0..) |field, index| {
        value_fields[index] = fieldInfo(field, context);
    }
    return value_fields;
}

fn fieldInfo(comptime field: std.builtin.Type.StructField, comptime context: FieldContext) FieldInfo {
    return .{ .info = field, .context = context, .name = field.name };
}

fn SchemaTable(Schema: type, comptime name: jetquery.DeclEnum(Schema)) type {
    return @field(Schema, @tagName(name));
}

fn Statement(
    comptime query_type: QueryType,
    Schema: type,
    Table: type,
    comptime relations: []const type,
    comptime field_infos: []const FieldInfo,
    comptime columns: []const std.meta.FieldEnum(Table.Definition),
    comptime order_clauses: []const OrderClause(Table),
    result_context: ResultContext,
) type {
    return struct {
        field_values: FieldValues(Table, relations, field_infos),
        limit_bound: ?usize = null,
        field_errors: [field_infos.len]?anyerror,

        comptime query_type: QueryType = query_type,
        comptime field_infos: []const FieldInfo = field_infos,
        comptime columns: []const std.meta.FieldEnum(Table.Definition) = columns,
        comptime order_clauses: []const OrderClause(Table) = order_clauses,

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

        /// Apply a `WHERE` clause to the current statement.
        pub fn where(self: Self, args: anytype) Statement(
            query_type,
            Schema,
            Table,
            relations,
            field_infos ++ fieldInfos(@TypeOf(args), .where),
            columns,
            order_clauses,
            result_context,
        ) {
            var statement: Statement(
                query_type,
                Schema,
                Table,
                relations,
                field_infos ++ fieldInfos(@TypeOf(args), .where),
                columns,
                order_clauses,
                result_context,
            ) = undefined;
            initStatement(Table, relations, @TypeOf(statement), &statement, Self, self, field_infos, args, .where);
            return statement;
        }

        /// Apply a `LIMIT` clause to the current statement.
        pub fn limit(self: Self, bound: usize) Statement(
            query_type,
            Schema,
            Table,
            relations,
            field_infos ++ fieldInfos(@TypeOf(.{bound}), .limit),
            columns,
            order_clauses,
            result_context,
        ) {
            var statement: Statement(
                query_type,
                Schema,
                Table,
                relations,
                field_infos ++ fieldInfos(@TypeOf(.{bound}), .limit),
                columns,
                order_clauses,
                result_context,
            ) = undefined;
            initStatement(Table, relations, @TypeOf(statement), &statement, Self, self, field_infos, .{bound}, .limit);
            return statement;
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
            ) = undefined;
            initStatement(
                Table,
                relations,
                @TypeOf(statement),
                &statement,
                Self,
                self,
                field_infos,
                .{},
                .order,
            );
            return statement;
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
            ) = undefined;
            initStatement(
                Table,
                relations,
                @TypeOf(statement),
                &statement,
                Self,
                self,
                field_infos,
                .{},
                .none,
            );
            return statement;
        }

        /// Execute the query's SQL with bind params using the provided repo.
        pub fn execute(self: Self, repo: *jetquery.Repo) !switch (result_context) {
            .one => ?ResultType,
            .many => jetquery.Result,
            .none => void,
        } {
            return try repo.execute(self);
        }

        pub fn values(self: Self) FieldValues(Table, relations, field_infos) {
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

        pub fn QueryColumnInfos() [totalColumnLen()]ColumnInfo {
            comptime {
                var column_infos: [totalColumnLen()]ColumnInfo = undefined;
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
                    var fields: [Relation.select_columns.len]std.builtin.Type.StructField = undefined;
                    for (Relation.select_columns, 0..) |column, index| {
                        const T = std.meta.FieldType(Relation.Source.Definition, column);
                        fields[index] = .{
                            .name = @tagName(column),
                            .type = T,
                            .default_value = null,
                            .is_comptime = false,
                            .alignment = @alignOf(T),
                        };
                    }
                    const RT = @Type(.{ .@"struct" = .{
                        .layout = .auto,
                        .fields = &fields,
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
) [std.meta.fields(@TypeOf(args)).len]OrderClause(Table) {
    comptime {
        var clauses: [std.meta.fields(@TypeOf(args)).len]OrderClause(Table) = undefined;
        const Columns = std.meta.FieldEnum(Table.Definition);

        for (std.meta.fields(@TypeOf(args)), 0..) |field, index| {
            clauses[index] = .{
                .column = std.enums.nameCast(Columns, field.name),
                .direction = std.enums.nameCast(OrderDirection, @tagName(@field(args, field.name))),
            };
        }
        return clauses;
    }
}

fn timestampsFields(
    Table: type,
    comptime query_type: FieldContext,
) [timestampsSize(Table, query_type)]FieldInfo {
    return if (hasTimestamps(Table)) switch (query_type) {
        .update => .{
            fieldInfo(.{
                .name = "updated_at",
                .type = usize,
                .default_value = null,
                .is_comptime = false,
                .alignment = @alignOf(usize),
            }, query_type), // TODO
        },
        .insert => .{
            fieldInfo(.{
                .name = "created_at",
                .type = usize,
                .default_value = null,
                .is_comptime = false,
                .alignment = @alignOf(usize),
            }, query_type), // TODO
            fieldInfo(.{
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

fn timestampsSize(Table: type, comptime query_type: FieldContext) u2 {
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
