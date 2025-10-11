const std = @import("std");

const jetquery = @import("../../jetquery.zig");
const fields = @import("../fields.zig");

const NullAdapter = @This();
const AdaptedRepo = jetquery.Repo(.null, struct {});

pub const Options = struct {};

pub const name: jetquery.adapters.Name = .null;

pub fn execute(self: *const NullAdapter, repo: *const AdaptedRepo, sql: []const u8, values: anytype, caller_info: ?jetquery.debug.CallerInfo) !jetquery.Result {
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

pub fn connect(
    self: *const NullAdapter,
    options: jetquery.adapters.ConnectionOptions,
) !jetquery.Connection {
    _ = self;
    _ = options;
    return error.JetQueryNullAdapterError;
}

pub fn release(
    self: *const NullAdapter,
    connection: jetquery.Connection,
) void {
    // We don't return an error here because `release` is used in defers, but execute/connect
    // will error before we get here in usual circumstances.
    _ = self;
    _ = connection;
}

pub fn columnTypeSql(comptime column: jetquery.schema.Column) []const u8 {
    _ = column;
    return "";
}

pub fn Aggregate(context: jetquery.sql.FunctionContext) type {
    _ = context;
    return usize;
}

pub fn identifier(comptime value: []const u8) []const u8 {
    _ = value;
    return "";
}

pub fn identifierAlloc(allocator: std.mem.Allocator, value: []const u8) ![]const u8 {
    _ = allocator;
    _ = value;
    return "";
}

pub fn columnSql(comptime column: jetquery.columns.Column) []const u8 {
    _ = column;
    return "";
}

pub fn primaryKeySql(comptime column: jetquery.schema.Column) []const u8 {
    _ = column;
    return "";
}

pub fn notNullSql() []const u8 {
    return "";
}

pub fn defaultValueSql(comptime default_value: []const u8) []const u8 {
    _ = default_value;
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
    comptime relation_name: []const u8,
    comptime options: jetquery.adapters.JoinOptions,
) []const u8 {
    _ = Table;
    _ = JoinTable;
    _ = relation_name;
    _ = options;

    return "";
}

pub fn outerJoinSql(
    Table: type,
    JoinTable: type,
    comptime relation_name: []const u8,
    comptime options: jetquery.adapters.JoinOptions,
) []const u8 {
    _ = Table;
    _ = JoinTable;
    _ = relation_name;
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

pub fn referenceSql(
    comptime reference: jetquery.schema.Column.Reference,
    comptime reference_options: ?jetquery.schema.Column.ReferenceOptions,
) []const u8 {
    _ = reference;
    _ = reference_options;
    return "";
}

pub fn createIndexSql(
    comptime index_name: []const u8,
    comptime table_name: []const u8,
    comptime column_names: []const []const u8,
    comptime options: jetquery.CreateIndexOptions,
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
    repo: *const AdaptedRepo,
) !jetquery.Reflection {
    _ = allocator;
    _ = self;
    return .{ .allocator = repo.allocator, .tables = &.{}, .columns = &.{} };
}

pub fn orderSql(comptime order_clause: jetquery.sql.OrderClause) []const u8 {
    _ = order_clause;
    return "";
}
