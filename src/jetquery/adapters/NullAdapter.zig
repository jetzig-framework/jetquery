const jetquery = @import("../../jetquery.zig");

const NullAdapter = @This();

pub const Options = struct {};

pub fn execute(self: *const NullAdapter, repo: *const jetquery.Repo, sql: []const u8, values: anytype) !jetquery.Result {
    _ = self;
    _ = repo;
    _ = sql;
    _ = values;
    return error.JetQueryNullAdapterError;
}

pub fn deinit(self: *const NullAdapter) void {
    _ = self;
}

pub fn columnTypeSql(self: NullAdapter, column_type: jetquery.Column.Type) []const u8 {
    _ = self;
    _ = column_type;
    return "";
}

pub fn identifier(name: []const u8) []const u8 {
    _ = name;
    return "";
}

pub fn primaryKeySql() []const u8 {
    return "";
}

pub fn notNullSql() []const u8 {
    return "";
}

pub fn paramSql(comptime index: usize) []const u8 {
    _ = index;
    return "";
}
