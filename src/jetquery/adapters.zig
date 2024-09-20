const jetquery = @import("../jetquery.zig");

pub const PostgresqlAdapter = @import("adapters/PostgresqlAdapter.zig");
pub const NullAdapter = @import("adapters/NullAdapter.zig");

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
    pub fn identifier(self: Adapter, name: []const u8) jetquery.Identifier {
        return switch (self) {
            inline else => |*adapter| adapter.identifier(name),
        };
    }

    /// SQL fragment used to indicate a primary key.
    pub fn primaryKeySql(self: Adapter) []const u8 {
        return switch (self) {
            inline else => |*adapter| adapter.primaryKeySql(),
        };
    }

    /// SQL representing a bind parameter, e.g. `$1`.
    pub fn paramSql(self: Adapter, buf: []u8, value: anytype, index: usize) ![]const u8 {
        return switch (self) {
            inline else => |*adapter| try adapter.paramSql(buf, value, index),
        };
    }
};

pub const test_adapter = Adapter{ .postgresql = .{
    .options = undefined,
    .pool = undefined,
    .allocator = undefined,
    .connected = undefined,
} };
