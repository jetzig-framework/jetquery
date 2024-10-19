const std = @import("std");

const pg = @import("pg");

const jetquery = @import("../../jetquery.zig");
const sql = @import("../sql.zig");

const PostgresqlAdapter = @This();

pool: *pg.Pool,
allocator: std.mem.Allocator,
options: Options,
connected: bool,
lazy_connect: bool = false,

pub const Count = i64;
pub const Average = i64;
pub const Sum = i64;
pub const Max = i32;
pub const Min = i32;

pub fn Aggregate(comptime context: jetquery.sql.FunctionContext) type {
    return switch (context) {
        .min => Min,
        .max => Max,
        .count => Count,
        .avg => Average,
        .sum => Sum,
    };
}

pub const Result = struct {
    result: *pg.Result,
    allocator: std.mem.Allocator,
    connection: *pg.Conn,
    repo: *jetquery.Repo,
    caller_info: ?jetquery.debug.CallerInfo,
    duration: i64,

    pub fn deinit(self: *Result) void {
        self.result.deinit();
        self.connection.release();
    }

    pub fn drain(self: *Result) !void {
        try self.result.drain();
    }

    pub fn next(self: *Result, query: anytype) !?@TypeOf(query).ResultType {
        if (try self.result.next()) |row| {
            var result_row: @TypeOf(query).ResultType = undefined;
            inline for (@TypeOf(query).ColumnInfos) |column_info| {
                if (column_info.relation) |relation| {
                    @field(
                        @field(result_row, relation.relation_name),
                        column_info.name,
                    ) = try resolvedValue(self.allocator, column_info, &row);
                } else {
                    @field(result_row, column_info.name) = try resolvedValue(
                        self.allocator,
                        column_info,
                        &row,
                    );
                }
            }
            return result_row;
        } else {
            return null;
        }
    }

    pub fn unary(self: *Result, T: type) !T {
        // This error should really never happen if used in conjunction with (e.g.) a `COUNT`
        // query, but we return an error to allow the host app (e.g. Jetzig) to handle it instead
        // of panicking.
        const row = try self.result.next() orelse return error.JetQueryMissingRowInUnaryQuery;

        if (row.values.len < 1) return error.JetQueryMissingColumnInUnaryQuery;

        return row.get(T, 0);
    }

    pub fn all(self: *Result, query: anytype) ![]@TypeOf(query).ResultType {
        defer self.deinit();

        var array = std.ArrayList(@TypeOf(query).ResultType).init(self.allocator);
        while (try self.next(query)) |row| try array.append(row);
        try self.drain();
        return try array.toOwnedSlice();
    }

    pub fn first(self: *Result, query: anytype) !?@TypeOf(query).Definition {
        return try self.next(query);
    }

    pub fn execute(
        self: *Result,
        query: []const u8,
        values: anytype,
    ) !jetquery.Result {
        return try connectionExecute(
            self.allocator,
            self.connection,
            self.repo,
            query,
            values,
            self.caller_info,
        );
    }
};

fn resolvedValue(
    allocator: std.mem.Allocator,
    column_info: sql.ColumnInfo,
    row: *const pg.Row,
) !column_info.type {
    return switch (column_info.type) {
        // TODO: pg.Numeric, pg.Cidr
        u8,
        ?u8,
        i16,
        ?i16,
        i32,
        ?i32,
        f32,
        ?f32,
        []u8,
        ?[]u8,
        i64,
        ?i64,
        bool,
        ?bool,
        []const u8,
        ?[]const u8,
        => |T| try maybeDupe(allocator, T, row.get(T, column_info.index)),
        jetquery.jetcommon.types.DateTime => |T| try T.fromUnix(
            row.get(i64, column_info.index),
            .microseconds,
        ),
        else => |T| @compileError("Unsupported type: " ++ @typeName(T)),
    };
}

fn maybeDupe(allocator: std.mem.Allocator, T: type, value: T) !T {
    return switch (T) {
        []const u8 => try allocator.dupe(u8, value),
        ?[]const u8 => if (value) |val| try allocator.dupe(u8, val) else null,
        else => value,
    };
}

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
pub fn init(allocator: std.mem.Allocator, options: Options, lazy_connect: bool) !PostgresqlAdapter {
    if (lazy_connect) return .{
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
    repo: *jetquery.Repo,
    query: []const u8,
    values: anytype,
    caller_info: ?jetquery.debug.CallerInfo,
) !jetquery.Result {
    if (!self.connected and self.lazy_connect) {
        self.pool = try initPool(self.allocator, self.options);
    }

    const connection = try self.pool.acquire();
    errdefer self.pool.release(connection);

    return try connectionExecute(repo.allocator, connection, repo, query, values, caller_info);
}

/// Output column type as SQL.
pub fn columnTypeSql(self: PostgresqlAdapter, column_type: jetquery.Column.Type) []const u8 {
    _ = self;
    return switch (column_type) {
        .string => " VARCHAR(255)",
        .integer => " INTEGER",
        .boolean => " BOOLEAN",
        .float => " REAL",
        .decimal => " NUMERIC",
        .datetime => " TIMESTAMP",
        .text => " TEXT",
    };
}

/// Output quoted identifier.
pub fn identifier(comptime name: []const u8) []const u8 {
    return std.fmt.comptimePrint(
        \\"{s}"
    , .{name});
}

/// SQL fragment used to represent a column bound to a table, e.g. `"foo"."bar"`
pub fn columnSql(Table: type, comptime column: jetquery.columns.Column) []const u8 {
    return if (column.function) |function|
        std.fmt.comptimePrint(
            \\{s}("{s}"."{s}")
        , .{
            switch (function) {
                .min => "MIN",
                .max => "MAX",
                .count => "COUNT",
                .avg => "AVG",
                .sum => "SUM",
            },
            Table.name,
            column.name,
        })
    else
        std.fmt.comptimePrint(
            \\"{s}"."{s}"
        , .{ Table.name, column.name });
}

/// SQL fragment used to indicate a primary key.
pub fn primaryKeySql() []const u8 {
    return " SERIAL PRIMARY KEY";
}

/// SQL fragment used to indicate a column whose value cannot be `NULL`.
pub fn notNullSql() []const u8 {
    return " NOT NULL";
}

/// SQL representing a bind parameter, e.g. `$1`.
pub fn paramSql(comptime index: usize) []const u8 {
    return std.fmt.comptimePrint("${}", .{index + 1});
}

/// SQL representing an array bind parameter with an `ANY` call, e.g. `ANY ($1)`.
pub fn anyParamSql(comptime index: usize) []const u8 {
    return std.fmt.comptimePrint("ANY (${})", .{index + 1});
}

pub fn orderSql(Table: type, comptime order_clause: sql.OrderClause) []const u8 {
    const direction = switch (order_clause.direction) {
        .ascending => "ASC",
        .descending => "DESC",
    };

    return std.fmt.comptimePrint(
        "{s} {s}",
        .{ columnSql(Table, order_clause.column), direction },
    );
}

pub fn countSql(comptime distinct: ?[]const jetquery.columns.Column) []const u8 {
    // TODO: Move some of this back into `sql.zig`.
    return if (comptime distinct) |distinct_columns| blk: {
        const template = "{s}.{s}{s}";
        var size: usize = 0;
        for (distinct_columns, 0..) |column, index| {
            size += std.fmt.count(
                template,
                .{
                    identifier(column.table.name),
                    identifier(column.name),
                    if (index + 1 < distinct_columns.len) ", " else "",
                },
            );
        }
        var buf: [size]u8 = undefined;
        var cursor: usize = 0;
        for (distinct_columns, 0..) |column, index| {
            const column_sql = std.fmt.comptimePrint(
                template,
                .{
                    identifier(column.table.name),
                    identifier(column.name),
                    if (index + 1 < distinct_columns.len) ", " else "",
                },
            );
            @memcpy(buf[cursor .. cursor + column_sql.len], column_sql);
            cursor += column_sql.len;
        }
        break :blk std.fmt.comptimePrint("COUNT(DISTINCT({s}))", .{buf});
    } else "COUNT(*)";
}

pub fn innerJoinSql(
    Table: type,
    JoinTable: type,
    comptime name: []const u8,
    comptime options: jetquery.adapters.JoinOptions,
) []const u8 {
    const foreign_key = options.foreign_key orelse name ++ "_id";
    const primary_key = options.primary_key orelse "id";

    return std.fmt.comptimePrint(
        \\ INNER JOIN "{s}" ON "{s}"."{s}" = "{s}"."{s}"
    ,
        .{
            JoinTable.name,
            Table.name,
            foreign_key,
            JoinTable.name,
            primary_key,
        },
    );
}

pub fn emptyWhereSql() []const u8 {
    return "(1 = 1)";
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

fn connectionExecute(
    allocator: std.mem.Allocator,
    connection: *pg.Conn,
    repo: *jetquery.Repo,
    query: []const u8,
    values: anytype,
    caller_info: ?jetquery.debug.CallerInfo,
) !jetquery.Result {
    const start_time = std.time.nanoTimestamp();

    const result = connection.queryOpts(query, values, .{}) catch |err| {
        if (connection.err) |connection_error| {
            try repo.eventCallback(.{
                .sql = query,
                .err = .{ .message = connection_error.message },
                .status = .fail,
                .caller_info = caller_info,
            });
        } else {
            try repo.eventCallback(.{
                .sql = query,
                .err = .{ .message = "Unknown error" },
                .status = .fail,
                .caller_info = caller_info,
            });
        }

        return err;
    };

    const duration: i64 = @intCast(std.time.nanoTimestamp() - start_time);

    try repo.eventCallback(.{ .sql = query, .caller_info = caller_info, .duration = duration });

    return .{
        .postgresql = .{
            .allocator = allocator,
            .result = result,
            .repo = repo,
            .connection = connection,
            .caller_info = caller_info,
            .duration = duration,
        },
    };
}
