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
    const options: pg.Conn.QueryOpts = .{ .release_conn = true, .column_names = true };
    var connection = try self.pool.acquire();
    errdefer self.pool.release(connection);

    return .{
        .postgresql = .{
            .allocator = self.allocator,
            // TODO: values
            .result = connection.queryOpts(sql, .{}, options) catch |err| {
                if (connection.err) |connection_error| {
                    std.debug.print("Error `{s}` while executing:\n{s}\n\n{s}\n", .{
                        @errorName(err),
                        sql,
                        connection_error.message,
                    });
                } else {
                    std.debug.print("Error `{s}` while executing:\n{s}\n", .{ @errorName(err), sql });
                }
                return err;
            },
        },
    };
}

/// Output column type as SQL.
pub fn columnTypeSql(self: PostgresqlAdapter, column_type: jetquery.Column.Type) []const u8 {
    _ = self;
    return switch (column_type) {
        .string => "VARCHAR(255)",
        .integer => "INTEGER",
        .float => "REAL",
        .decimal => "NUMERIC",
        .datetime => "TIMESTAMP",
    };
}
