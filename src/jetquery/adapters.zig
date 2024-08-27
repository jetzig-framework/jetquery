pub const PostgresqlAdapter = @import("adapters/PostgresqlAdapter.zig");
const jetquery = @import("../jetquery.zig");

pub const Adapter = union(enum) {
    postgresql: PostgresqlAdapter,

    /// Execute SQL with the active adapter.
    pub fn execute(self: *Adapter, sql: []const u8) !jetquery.Result {
        return switch (self.*) {
            inline else => |*adapter| try adapter.execute(sql),
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
};

pub const test_adapter = Adapter{ .postgresql = .{} };
