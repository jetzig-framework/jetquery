const std = @import("std");

const pg = @import("pg");

const jetquery = @import("../../jetquery.zig");

const PostgresqlAdapter = @This();

pool: *pg.Pool,
allocator: std.mem.Allocator,

pub const Result = struct {
    result: *pg.Result,
    allocator: std.mem.Allocator,

    pub fn deinit(self: *Result) void {
        self.result.deinit();
    }

    pub fn next(self: *Result) !?jetquery.Row {
        if (try self.result.next()) |row| {
            var array = std.ArrayList(jetquery.Value).init(self.allocator);
            for (row.values) |value| try array.append(.{ .string = value.data });
            return .{
                .allocator = self.allocator,
                .columns = self.result.column_names,
                .values = try array.toOwnedSlice(),
            };
        } else {
            return null;
        }
    }
};

pub const Options = struct {
    database: []const u8,
    username: []const u8,
    password: []const u8,
    hostname: []const u8,
    port: u16 = 5432,
    pool_size: u16 = 8,
    timeout: u32 = 10_000,
};

/// Initialize a new PostgreSQL adapter and connection pool.
pub fn init(allocator: std.mem.Allocator, options: Options) !PostgresqlAdapter {
    return .{
        .allocator = allocator,
        .pool = try pg.Pool.init(allocator, .{
            .size = options.pool_size,
            .connect = .{
                .port = options.port,
                .host = options.hostname,
            },
            .auth = .{
                .username = options.username,
                .database = options.database,
                .password = options.password,
                .timeout = options.timeout,
            },
        }),
    };
}

/// Close connections and free resources.
pub fn deinit(self: *PostgresqlAdapter) void {
    self.pool.deinit();
}

/// Execute the given query with a pooled connection.
pub fn execute(self: *PostgresqlAdapter, sql: []const u8) !jetquery.Result {
    return .{
        .postgresql = .{
            .allocator = self.allocator,
            // TODO: values
            .result = try self.pool.queryOpts(sql, .{}, .{ .column_names = true }),
        },
    };
}
