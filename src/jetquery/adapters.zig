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

pub fn Adapter(comptime adapter_name: Name) type {
    const Union = union(enum) {
        postgresql: PostgresqlAdapter,
        null: NullAdapter,

        const Self = @This();
        const AdaptedRepo = jetquery.Repo(adapter_name);

        pub fn connect(self: *Self, repo: *AdaptedRepo) !AdaptedRepo.Connection {
            return switch (comptime adapter_name) {
                inline else => |name| try @field(self, @tagName(name)).connect(repo),
            };
        }

        pub fn release(self: *Self, connection: AdaptedRepo.Connection) void {
            return switch (comptime adapter_name) {
                inline else => |name| @field(self, @tagName(name)).release(connection),
            };
        }

        /// Convert a column type to a database type suitable for the active adapter.
        pub fn columnTypeSql(self: Self, comptime column: jetquery.schema.Column) []const u8 {
            return switch (self) {
                inline else => |adapter| @TypeOf(adapter).columnTypeSql(column),
            };
        }

        /// Quote an identifier (e.g. a table name) suitable for the active adapter.
        pub fn identifier(self: Self, comptime name: []const u8) []const u8 {
            return switch (self) {
                inline else => |adapter| @TypeOf(adapter).identifier(name),
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
            Table: type,
            JoinTable: type,
            comptime name: []const u8,
            comptime options: JoinOptions,
        ) []const u8 {
            return switch (self) {
                inline else => |adapter| @TypeOf(adapter).innerJoinSql(Table, JoinTable, name, options),
            };
        }

        /// SQL representing an outer join, e.g. `LEFT OUTER JOIN "foo" ON "bar"."baz" = "foo"."baz"`
        pub fn outerJoinSql(
            self: Self,
            Table: type,
            JoinTable: type,
            comptime name: []const u8,
            comptime options: JoinOptions,
        ) []const u8 {
            return switch (self) {
                inline else => |adapter| @TypeOf(adapter).outerJoinSql(Table, JoinTable, name, options),
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

        pub fn reflect(
            self: *Self,
            allocator: std.mem.Allocator,
            repo: *AdaptedRepo,
        ) !jetquery.Reflection {
            return switch (comptime adapter_name) {
                inline else => |name| try @field(self, @tagName(name)).reflect(allocator, repo),
            };
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
