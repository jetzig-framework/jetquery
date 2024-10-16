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

pub const Adapter = union(enum) {
    postgresql: PostgresqlAdapter,
    null: NullAdapter,

    /// Execute SQL with the active adapter.
    pub fn execute(
        self: *Adapter,
        repo: *jetquery.Repo,
        sql: []const u8,
        values: anytype,
        caller_info: ?jetquery.debug.CallerInfo,
    ) !jetquery.Result {
        return switch (self.*) {
            inline else => |*adapter| try adapter.execute(repo, sql, values, caller_info),
        };
    }

    /// Convert a column type to a database type suitable for the active adapter.
    pub fn columnTypeSql(self: Adapter, column_type: jetquery.Column.Type) []const u8 {
        return switch (self) {
            inline else => |*adapter| adapter.columnTypeSql(column_type),
        };
    }

    /// Quote an identifier (e.g. a table name) suitable for the active adapter.
    pub fn identifier(self: Adapter, comptime name: []const u8) []const u8 {
        return switch (self) {
            inline else => |adapter| @TypeOf(adapter).identifier(name),
        };
    }

    /// Quote a column bound to a table suitable for the active adapter.
    pub fn columnSql(
        self: Adapter,
        Table: type,
        comptime column: jetquery.columns.Column,
    ) []const u8 {
        return switch (self) {
            inline else => |adapter| @TypeOf(adapter).columnSql(Table, column),
        };
    }

    /// SQL fragment used to indicate a primary key.
    pub fn primaryKeySql(self: Adapter) []const u8 {
        return switch (self) {
            inline else => |adapter| @TypeOf(adapter).primaryKeySql(),
        };
    }

    /// SQL fragment used to indicate a column whose value cannot be `NULL`.
    pub fn notNullSql(self: Adapter) []const u8 {
        return switch (self) {
            inline else => |adapter| @TypeOf(adapter).notNullSql(),
        };
    }

    /// SQL representing a bind parameter, e.g. `$1`.
    pub fn paramSql(self: Adapter, comptime index: usize) []const u8 {
        return switch (self) {
            inline else => |adapter| @TypeOf(adapter).paramSql(index),
        };
    }

    /// SQL representing an array bind parameter with an `ANY` call, e.g. `ANY ($1)`.
    pub fn anyParamSql(self: Adapter, comptime index: usize) []const u8 {
        return switch (self) {
            inline else => |adapter| @TypeOf(adapter).anyParamSql(index),
        };
    }

    /// SQL representing an `ORDER BY` directive, e.g. `"foo" DESC`
    pub fn orderSql(self: Adapter, Table: type, comptime order_clause: jetquery.OrderClause) []const u8 {
        return switch (self) {
            inline else => |adapter| @TypeOf(adapter).orderSql(Table, order_clause),
        };
    }

    /// SQL fragment used when generating a `COUNT` column, e.g. `COUNT(*)`
    pub fn countSql(
        self: Adapter,
        comptime distinct: ?[]const jetquery.columns.Column,
    ) []const u8 {
        return switch (self) {
            inline else => |adapter| @TypeOf(adapter).countSql(distinct),
        };
    }

    /// SQL representing an inner join, e.g. `INNER JOIN "foo" ON "bar"."baz" = "foo"."baz"`
    pub fn innerJoinSql(
        self: Adapter,
        Table: type,
        JoinTable: type,
        comptime name: []const u8,
        comptime options: JoinOptions,
    ) []const u8 {
        return switch (self) {
            inline else => |adapter| @TypeOf(adapter).innerJoinSql(Table, JoinTable, name, options),
        };
    }

    /// SQL fragment used as a `WHERE` clause when no clause has been applied by the user.
    pub fn emptyWhereSQL(self: Adapter) []const u8 {
        return switch (self) {
            inline else => |adapter| @TypeOf(adapter).emptyWhereSQL(),
        };
    }

    /// Resolve an appropriate type for a given aggregate function (e.g. COUNT, MIN, MAX, etc.).
    pub fn Aggregate(self: Adapter, context: jetquery.sql.FunctionContext) type {
        return switch (self) {
            inline else => |adapter| @TypeOf(adapter).Aggregate(context),
        };
    }
};

pub const JoinOptions = struct {
    foreign_key: ?[]const u8 = null,
    primary_key: ?[]const u8 = null,
};

pub const test_adapter = Adapter{ .postgresql = .{
    .options = undefined,
    .pool = undefined,
    .allocator = undefined,
    .connected = undefined,
} };
