const std = @import("std");

const jetquery = @import("../jetquery.zig");

pub const PostgresqlAdapter = @import("adapters/PostgresqlAdapter.zig");
pub const NullAdapter = @import("adapters/NullAdapter.zig");

pub const Name = enum { postgresql, null };

pub fn Type(adapter: Name) type {
    return switch (adapter) {
        .postgresql => PostgresqlAdapter,
        .null => NullAdapter,
    };
}

pub fn Adapter(comptime adapter_name: Name, AdaptedRepo: type) type {
    const Union = union(enum) {
        postgresql: PostgresqlAdapter,
        null: NullAdapter,

        const Self = @This();

        pub const name = adapter_name;

        pub fn connect(self: *Self) !jetquery.Connection {
            return switch (comptime adapter_name) {
                inline else => |tag| try @field(self, @tagName(tag)).connect(),
            };
        }

        pub fn release(self: *Self, connection: jetquery.Connection) void {
            return switch (comptime adapter_name) {
                inline else => |tag| @field(self, @tagName(tag)).release(connection),
            };
        }

        /// Convert a column type to a database type suitable for the active adapter.
        pub fn columnTypeSql(self: Self, comptime column: jetquery.schema.Column) []const u8 {
            return switch (self) {
                inline else => |adapter| @TypeOf(adapter).columnTypeSql(column),
            };
        }

        /// Quote an identifier (e.g. a table name) suitable for the active adapter.
        pub fn identifier(self: Self, comptime value: []const u8) []const u8 {
            return switch (self) {
                inline else => |adapter| @TypeOf(adapter).identifier(value),
            };
        }

        /// Quote a column bound to a table suitable for the active adapter.
        pub fn columnSql(
            self: Self,
            comptime column: jetquery.columns.Column,
        ) []const u8 {
            return switch (self) {
                inline else => |adapter| @TypeOf(adapter).columnSql(column),
            };
        }

        /// SQL fragment used to indicate a primary key.
        pub fn primaryKeySql(self: Self, comptime column: jetquery.schema.Column) []const u8 {
            return switch (self) {
                inline else => |adapter| @TypeOf(adapter).primaryKeySql(column),
            };
        }

        /// SQL fragment used to indicate a column whose value cannot be `NULL`.
        pub fn notNullSql(self: Self) []const u8 {
            return switch (self) {
                inline else => |adapter| @TypeOf(adapter).notNullSql(),
            };
        }

        /// SQL representing a bind parameter, e.g. `$1`.
        pub fn paramSql(self: Self, comptime index: usize) []const u8 {
            return switch (self) {
                inline else => |adapter| @TypeOf(adapter).paramSql(index),
            };
        }

        /// Same as `paramSql` but writes to a buffer at runtime.
        pub fn paramSqlBuf(self: Self, buf: []u8, index: usize) ![]const u8 {
            return switch (self) {
                inline else => |adapter| try @TypeOf(adapter).paramSqlBuf(buf, index),
            };
        }

        /// SQL representing an array bind parameter with an `ANY` call, e.g. `ANY ($1)`.
        pub fn anyParamSql(self: Self, comptime index: usize) []const u8 {
            return switch (self) {
                inline else => |adapter| @TypeOf(adapter).anyParamSql(index),
            };
        }

        /// SQL representing an `ORDER BY` directive, e.g. `"foo" DESC`
        pub fn orderSql(self: Self, comptime order_clause: jetquery.OrderClause) []const u8 {
            return switch (self) {
                inline else => |adapter| @TypeOf(adapter).orderSql(order_clause),
            };
        }

        /// SQL fragment used when generating a `COUNT` column, e.g. `COUNT(*)`
        pub fn countSql(
            self: Self,
            comptime distinct: ?[]const jetquery.columns.Column,
        ) []const u8 {
            return switch (self) {
                inline else => |adapter| @TypeOf(adapter).countSql(distinct),
            };
        }

        /// SQL representing an inner join, e.g. `INNER JOIN "foo" ON "bar"."baz" = "foo"."baz"`
        pub fn innerJoinSql(
            self: Self,
            Model: type,
            JoinTable: type,
            comptime relation_name: []const u8,
            comptime options: JoinOptions,
        ) []const u8 {
            return switch (self) {
                inline else => |adapter| @TypeOf(adapter).innerJoinSql(
                    Model,
                    JoinTable,
                    relation_name,
                    options,
                ),
            };
        }

        /// SQL representing an outer join, e.g. `LEFT OUTER JOIN "foo" ON "bar"."baz" = "foo"."baz"`
        pub fn outerJoinSql(
            self: Self,
            Model: type,
            JoinTable: type,
            comptime relation_name: []const u8,
            comptime options: JoinOptions,
        ) []const u8 {
            return switch (self) {
                inline else => |adapter| @TypeOf(adapter).outerJoinSql(
                    Model,
                    JoinTable,
                    relation_name,
                    options,
                ),
            };
        }

        /// SQL fragment used as a `WHERE` clause when no clause has been applied by the user.
        pub fn emptyWhereSQL(self: Self) []const u8 {
            return switch (self) {
                inline else => |adapter| @TypeOf(adapter).emptyWhereSQL(),
            };
        }

        /// Automatically generate an index name from the given table name and columns. Fails if
        /// generated name is too long for adapter's identifier length limit.
        pub fn indexName(
            self: Self,
            comptime table_name: []const u8,
            comptime column_names: []const []const u8,
        ) []const u8 {
            return switch (self) {
                inline else => |adapter| @TypeOf(adapter).indexName(table_name, column_names),
            };
        }

        /// Generate SQL for creating an index with the active adapter.
        pub fn createIndexSql(
            self: Self,
            comptime index_name: []const u8,
            comptime table_name: []const u8,
            comptime column_names: []const []const u8,
            comptime options: AdaptedRepo.CreateIndexOptions,
        ) []const u8 {
            return switch (self) {
                inline else => |adapter| &@TypeOf(adapter).createIndexSql(
                    index_name,
                    table_name,
                    column_names,
                    options,
                ),
            };
        }

        /// SQL fragment used when specifying a unique constraint.
        pub fn uniqueColumnSql(self: Self) []const u8 {
            return switch (self) {
                inline else => |adapter| @TypeOf(adapter).uniqueColumnSql(),
            };
        }

        /// SQL fragment used to denote a foreign key.
        pub fn referenceSql(
            self: Self,
            comptime reference: jetquery.schema.Column.Reference,
        ) []const u8 {
            return switch (self) {
                inline else => |adapter| @TypeOf(adapter).referenceSql(reference),
            };
        }

        /// Resolve an appropriate type for a given aggregate function (e.g. COUNT, MIN, MAX, etc.).
        pub fn Aggregate(self: Self, context: jetquery.sql.FunctionContext) type {
            return switch (self) {
                inline else => |adapter| @TypeOf(adapter).Aggregate(context),
            };
        }

        /// Return all metadata from the database needed to generate a schema file.
        pub fn reflect(
            self: *Self,
            allocator: std.mem.Allocator,
            repo: *AdaptedRepo,
        ) !jetquery.Reflection {
            return switch (comptime adapter_name) {
                inline else => |tag| try @field(self, @tagName(tag)).reflect(allocator, repo),
            };
        }

        pub fn writeAddColumnSql(
            self: Self,
            comptime column: jetquery.schema.Column,
            writer: anytype,
        ) !void {
            if (column.timestamps) |timestamps| {
                try timestamps.toSql(writer, self);
            } else {
                try writer.print(
                    \\{s}{s}{s}{s}{s}{s}
                , .{
                    self.identifier(column.name),
                    if (column.primary_key)
                        ""
                    else
                        self.columnTypeSql(column),
                    if (!column.primary_key and column.options.not_null)
                        self.notNullSql()
                    else
                        "",
                    if (column.primary_key) self.primaryKeySql(column) else "",
                    if (column.options.unique) self.uniqueColumnSql() else "",
                    if (column.options.reference) |reference|
                        self.referenceSql(reference)
                    else
                        "",
                });
            }
        }
    };
    return Union;
}

pub const JoinOptions = struct {
    foreign_key: ?[]const u8 = null,
    primary_key: ?[]const u8 = null,
};

pub const test_adapter = Adapter(.postgresql){ .postgresql = .{
    .options = undefined,
    .pool = undefined,
    .allocator = undefined,
    .connected = undefined,
} };
