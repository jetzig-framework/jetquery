const std = @import("std");

const pg = @import("pg");

const jetquery = @import("../../jetquery.zig");

const PostgresqlAdapter = @This();

pool: *pg.Pool,
allocator: std.mem.Allocator,
options: Options,
connected: bool,
lazy_connect: bool = false,

pub const Result = struct {
    result: *pg.Result,
    allocator: std.mem.Allocator,

    pub fn deinit(self: *Result) void {
        self.result.deinit();
    }

    pub fn drain(self: *Result) !void {
        try self.result.drain();
    }

    pub fn next(self: *Result, query: anytype) !?@TypeOf(query).Definition {
        if (try self.result.next()) |row| {
            var result_row: @TypeOf(query).Definition = undefined;
            for (self.result.column_names, 0..) |column_name, index| {
                inline for (std.meta.fields(@TypeOf(query).Definition)) |field| {
                    if (std.mem.eql(u8, field.name, column_name)) {
                        @field(result_row, field.name) = switch (field.type) {
                            // TODO: Other types, strict number types only
                            []const u8 => row.get([]const u8, index),
                            usize => @intCast(row.get(i32, index)),
                            else => @compileError("Unsupported type: " ++ @typeName(field.type)),
                        };
                    }
                }
            }
            return result_row;
        } else {
            return null;
        }
    }

    pub fn all(self: *Result, query: anytype) ![]const @TypeOf(query).Definition {
        var array = std.ArrayList(@TypeOf(query).Definition).init(self.allocator);
        while (try self.next(query)) |row| try array.append(row);
        return try array.toOwnedSlice();
    }

    pub fn first(self: *Result, query: anytype) !?@TypeOf(query).Definition {
        return try self.next(query);
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
    lazy_connect: bool = false,
};

/// Initialize a new PostgreSQL adapter and connection pool.
pub fn init(allocator: std.mem.Allocator, options: Options) !PostgresqlAdapter {
    if (options.lazy_connect) return .{
        .allocator = allocator,
        .options = options,
        .pool = undefined,
        .lazy_connect = true,
        .connected = false,
    };

    return .{
        .allocator = allocator,
        .options = options,
        .pool = try initPool(allocator, options),
        .connected = true,
    };
}

/// Close connections and free resources.
pub fn deinit(self: *PostgresqlAdapter) void {
    self.pool.deinit();
}

/// Execute the given query with a pooled connection.
pub fn execute(
    self: *PostgresqlAdapter,
    repo: *const jetquery.Repo,
    sql: []const u8,
    values: []const jetquery.Value,
) !jetquery.Result {
    if (!self.connected and self.lazy_connect) self.pool = try initPool(self.allocator, self.options);

    const options: pg.Conn.QueryOpts = .{ .column_names = true };
    var connection = try self.pool.acquire();
    errdefer self.pool.release(connection);

    const result = connection.queryOpts(sql, values, options) catch |err| {
        if (connection.err) |connection_error| {
            try repo.eventCallback(.{
                .sql = sql,
                .err = .{ .message = connection_error.message },
                .status = .fail,
            });
        } else {
            try repo.eventCallback(.{ .sql = sql, .err = .{ .message = "Unknown error" }, .status = .fail });
        }
        return err;
    };

    try repo.eventCallback(.{ .sql = sql });

    return .{
        .postgresql = .{
            .allocator = self.allocator,
            .result = result,
        },
    };
}

/// Output column type as SQL.
pub fn columnTypeSql(self: PostgresqlAdapter, column_type: jetquery.Column.Type) []const u8 {
    _ = self;
    return switch (column_type) {
        .string => "VARCHAR(255)",
        .integer => "INTEGER",
        .boolean => "BOOLEAN",
        .float => "REAL",
        .decimal => "NUMERIC",
        .datetime => "TIMESTAMP",
        .text => "TEXT",
    };
}

/// Output quoted identifier.
pub fn identifier(self: PostgresqlAdapter, name: []const u8) jetquery.Identifier {
    _ = self;
    return .{ .name = name, .quote_char = '"' };
}

/// SQL fragment used to indicate a primary key.
pub fn primaryKeySql(self: PostgresqlAdapter) []const u8 {
    _ = self;
    return "SERIAL PRIMARY KEY";
}

/// SQL representing a bind parameter, e.g. `$1`.
pub fn paramSql(self: PostgresqlAdapter, buf: []u8, value: jetquery.Value, index: usize) ![]const u8 {
    _ = value;
    _ = self;
    var stream = std.io.fixedBufferStream(buf);
    const writer = stream.writer();
    try writer.print("${}", .{index + 1});
    return stream.getWritten();
}

fn initPool(allocator: std.mem.Allocator, options: Options) !*pg.Pool {
    return try pg.Pool.init(allocator, .{
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
    });
}
