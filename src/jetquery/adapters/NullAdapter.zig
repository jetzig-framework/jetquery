const jetquery = @import("../../jetquery.zig");

const NullAdapter = @This();

pub fn execute(self: *const NullAdapter, sql: []const u8, repo: *const jetquery.Repo) !jetquery.Result {
    _ = self;
    _ = sql;
    _ = repo;
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
