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
        repo: *const jetquery.Repo,
        sql: []const u8,
        values: anytype,
    ) !jetquery.Result {
        return switch (self.*) {
            inline else => |*adapter| try adapter.execute(repo, sql, values),
        };
    }

    /// Convert a column type to a database type suitable for the active adapter.
    pub fn columnTypeSql(self: Adapter, column_type: jetquery.Column.Type) []const u8 {
        return switch (self) {
            inline else => |*adapter| adapter.columnTypeSql(column_type),
        };
    }

    /// Quote an identifier (e.g. a column name) suitable for the active adapter.
    pub fn identifier(self: Adapter, comptime name: []const u8) []const u8 {
        return switch (self) {
            inline else => |adapter| @TypeOf(adapter).identifier(name),
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

    pub fn orderSql(self: Adapter, Table: type, comptime order_clause: jetquery.OrderClause(Table)) []const u8 {
        return switch (self) {
            inline else => |adapter| @TypeOf(adapter).orderSql(Table, order_clause),
        };
    }
};

pub const test_adapter = Adapter{ .postgresql = .{
    .options = undefined,
    .pool = undefined,
    .allocator = undefined,
    .connected = undefined,
} };
