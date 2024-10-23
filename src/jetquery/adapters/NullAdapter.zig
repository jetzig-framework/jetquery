const std = @import("std");

const jetquery = @import("../../jetquery.zig");
const fields = @import("../fields.zig");

const NullAdapter = @This();

pub const Options = struct {};

pub fn execute(self: *const NullAdapter, repo: *const jetquery.Repo, sql: []const u8, values: anytype, caller_info: ?jetquery.debug.CallerInfo) !jetquery.Result {
    _ = self;
    _ = repo;
    _ = sql;
    _ = values;
    _ = caller_info;
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

pub fn Aggregate(context: jetquery.sql.FunctionContext) type {
    _ = context;
    return usize;
}

pub fn identifier(comptime name: []const u8) []const u8 {
    _ = name;
    return "";
}

pub fn columnSql(Table: type, comptime column: jetquery.columnsColumn) []const u8 {
    _ = Table;
    _ = column;
    return "";
}

pub fn primaryKeySql() []const u8 {
    return "";
}

pub fn notNullSql() []const u8 {
    return "";
}

pub fn countSql(comptime distinct: ?[]const jetquery.columns.Column) []const u8 {
    _ = distinct;
    return "";
}

pub fn paramSql(comptime index: usize) []const u8 {
    _ = index;
    return "";
}

pub fn anyParamSql(comptime index: usize) []const u8 {
    _ = index;
    return "";
}

pub fn innerJoinSql(
    Table: type,
    JoinTable: type,
    comptime name: []const u8,
    comptime options: jetquery.adapters.JoinOptions,
) []const u8 {
    _ = Table;
    _ = JoinTable;
    _ = name;
    _ = options;

    return "";
}

pub fn outerJoinSql(
    Table: type,
    JoinTable: type,
    comptime name: []const u8,
    comptime options: jetquery.adapters.JoinOptions,
) []const u8 {
    _ = Table;
    _ = JoinTable;
    _ = name;
    _ = options;

    return "";
}

pub fn emptyWhereSql() []const u8 {
    return "";
}

pub fn indexName(
    comptime table_name: []const u8,
    comptime column_names: []const []const u8,
) [0]u8 {
    _ = table_name;
    _ = column_names;
    return .{};
}

pub fn uniqueColumnSql() []const u8 {
    return "";
}

pub fn createIndexSql(
    comptime index_name: []const u8,
    comptime table_name: []const u8,
    comptime column_names: []const []const u8,
    comptime options: jetquery.Repo.CreateIndexOptions,
) [0]u8 {
    _ = index_name;
    _ = table_name;
    _ = column_names;
    _ = options;
    return .{};
}

pub fn reflect(
    self: *const NullAdapter,
    allocator: std.mem.Allocator,
    repo: *const jetquery.Repo,
) !jetquery.Reflection {
    _ = allocator;
    _ = self;
    return .{ .allocator = repo.allocator, .tables = &.{}, .columns = &.{} };
}
