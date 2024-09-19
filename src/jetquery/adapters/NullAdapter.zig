const jetquery = @import("../../jetquery.zig");

const NullAdapter = @This();

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

pub fn identifier(self: NullAdapter, name: []const u8) jetquery.Identifier {
    _ = self;
    _ = name;
    return .{ .name = "", .quote_char = 0 };
}

pub fn primaryKeySql(self: NullAdapter) []const u8 {
    _ = self;
    return "";
}

pub fn paramSql(self: NullAdapter, buf: []u8, value: jetquery.Value, index: usize) ![]const u8 {
    _ = buf;
    _ = value;
    _ = self;
    _ = index;
    return "";
}
