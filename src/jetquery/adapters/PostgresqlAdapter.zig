const std = @import("std");
const builtin = @import("builtin");

const pg = @import("pg");

const jetquery = @import("../../jetquery.zig");

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
pub const max_identifier_len = 63;

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
    column_info: jetquery.sql.ColumnInfo,
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
    database: ?[]const u8 = null,
    username: ?[]const u8 = null,
    password: ?[]const u8 = null,
    hostname: ?[]const u8 = null,
    port: ?u16 = null,
    pool_size: ?u16 = null,
    timeout: ?u32 = null,

    pub fn defaultValue(T: type, comptime name: []const u8) T {
        const tag = std.enums.nameCast(std.meta.FieldEnum(Options), name);
        return switch (tag) {
            .database, .username, .password => null,
            .hostname => "localhost",
            .port => 5432,
            .pool_size => 8,
            .timeout => 10_000,
        };
    }
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
    // TODO: Table is redundant as column contains the table already.
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

pub fn orderSql(comptime order_clause: jetquery.sql.OrderClause) []const u8 {
    const direction = switch (order_clause.direction) {
        .ascending, .asc => "ASC",
        .descending, .desc => "DESC",
    };

    return std.fmt.comptimePrint(
        "{s} {s}",
        .{ columnSql(order_clause.column.table, order_clause.column), direction },
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

pub fn outerJoinSql(
    Table: type,
    JoinTable: type,
    comptime name: []const u8,
    comptime options: jetquery.adapters.JoinOptions,
) []const u8 {
    const foreign_key = options.foreign_key orelse name ++ "_id";
    const primary_key = options.primary_key orelse "id";

    return std.fmt.comptimePrint(
        \\ LEFT OUTER JOIN "{s}" ON "{s}"."{s}" = "{s}"."{s}"
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

pub fn indexName(
    comptime table_name: []const u8,
    comptime column_names: []const []const u8,
) *const [indexNameSize(table_name, column_names)]u8 {
    comptime {
        var buf: [indexNameSize(table_name, column_names)]u8 = undefined;
        const prefix = std.fmt.comptimePrint("index_{s}_", .{table_name});
        @memcpy(buf[0..prefix.len], prefix);

        var cursor: usize = prefix.len;
        for (column_names, 0..) |column_name, index| {
            const separator = if (index + 1 < column_names.len) "_" else "";
            const column_suffix = std.fmt.comptimePrint("{s}{s}", .{ column_name, separator });
            @memcpy(buf[cursor .. cursor + column_suffix.len], column_suffix);
            cursor += column_suffix.len;
        }
        const final = buf;
        return &final;
    }
}

fn indexNameSize(comptime table_name: []const u8, comptime column_names: []const []const u8) usize {
    comptime {
        var size: usize = 0;
        size += std.fmt.comptimePrint("index_{s}_", .{table_name}).len;
        for (column_names, 0..) |column_name, index| {
            const separator = if (index + 1 < column_names.len) "_" else "";
            size += std.fmt.comptimePrint("{s}{s}", .{ column_name, separator }).len;
        }
        if (size > max_identifier_len) {
            @compileError(
                std.fmt.comptimePrint(
                    "Generated index name length {} longer than {} characters. Specify `.index_name` to manually set a name for this index.",
                    .{ size, max_identifier_len },
                ),
            );
        }
        return size;
    }
}

pub fn createIndexSql(
    comptime index_name: []const u8,
    comptime table_name: []const u8,
    comptime column_names: []const []const u8,
    comptime options: jetquery.Repo.CreateIndexOptions,
) *const [createIndexSqlSize(index_name, table_name, column_names, options)]u8 {
    comptime {
        var buf: [createIndexSqlSize(index_name, table_name, column_names, options)]u8 = undefined;
        const statement = std.fmt.comptimePrint(
            "CREATE {s}INDEX {s} ON {s} (",
            .{ if (options.unique) "UNIQUE " else "", identifier(index_name), identifier(table_name) },
        );
        @memcpy(buf[0..statement.len], statement);

        var cursor: usize = statement.len;
        for (column_names, 0..) |column_name, index| {
            const separator = if (index + 1 < column_names.len) ", " else "";
            const column = std.fmt.comptimePrint("{s}{s}", .{ column_name, separator });
            @memcpy(buf[cursor .. cursor + column.len], column);
            cursor += column.len;
        }
        buf[cursor] = ')';
        const final = buf;
        return &final;
    }
}

fn createIndexSqlSize(
    comptime index_name: []const u8,
    comptime table_name: []const u8,
    comptime column_names: []const []const u8,
    comptime options: jetquery.Repo.CreateIndexOptions,
) usize {
    comptime {
        var size: usize = 0;
        size += std.fmt.comptimePrint(
            "CREATE {s}INDEX {s} ON {s} (",
            .{ if (options.unique) "UNIQUE " else "", identifier(index_name), identifier(table_name) },
        ).len;
        for (column_names, 0..) |column_name, index| {
            const separator = if (index + 1 < column_names.len) ", " else "";
            size += std.fmt.comptimePrint("{s}{s}", .{ column_name, separator }).len;
        }
        size += ")".len;
        return size;
    }
}

pub fn uniqueColumnSql() []const u8 {
    return " UNIQUE";
}

pub fn referenceSql(comptime reference: jetquery.Column.Reference) []const u8 {
    return std.fmt.comptimePrint(
        " REFERENCES {s}({s})",
        .{ comptime identifier(reference[0]), comptime identifier(reference[1]) },
    );
}

pub fn reflect(
    self: *PostgresqlAdapter,
    allocator: std.mem.Allocator,
    repo: *jetquery.Repo,
) !jetquery.Reflection {
    const tables = try self.reflectTables(allocator, repo);
    const columns = try self.reflectColumns(allocator, repo);
    return .{ .allocator = self.allocator, .tables = tables, .columns = columns };
}

pub fn reflectTables(
    self: *PostgresqlAdapter,
    allocator: std.mem.Allocator,
    repo: *jetquery.Repo,
) ![]const jetquery.Reflection.TableInfo {
    const sql =
        \\SELECT "table_name" FROM "information_schema"."tables" WHERE "table_schema" = 'public' AND "table_name" <> 'jetquery_migrations' ORDER BY "table_name"
    ;
    var result = try self.execute(repo, sql, .{}, null);
    defer result.deinit();

    var tables = std.ArrayList(jetquery.Reflection.TableInfo).init(allocator);
    while (try result.postgresql.result.next()) |row| {
        try tables.append(.{
            .name = try allocator.dupe(u8, row.get([]const u8, 0)),
        });
    }
    try result.drain();

    return try tables.toOwnedSlice();
}

pub fn reflectColumns(
    self: *PostgresqlAdapter,
    allocator: std.mem.Allocator,
    repo: *jetquery.Repo,
) ![]const jetquery.Reflection.ColumnInfo {
    const sql =
        \\SELECT "table_name", "column_name", "data_type", "is_nullable" FROM "information_schema"."columns" WHERE "table_schema" = 'public' ORDER BY "table_name", "ordinal_position"
    ;
    var result = try self.execute(repo, sql, .{}, null);
    defer result.deinit();

    var columns = std.ArrayList(jetquery.Reflection.ColumnInfo).init(allocator);
    while (try result.postgresql.result.next()) |row| {
        try columns.append(.{
            .table = try allocator.dupe(u8, row.get([]const u8, 0)),
            .name = try allocator.dupe(u8, row.get([]const u8, 1)),
            .type = translateColumnType(row.get([]const u8, 2)),
            .null = std.mem.eql(u8, row.get([]const u8, 3), "YES"),
        });
    }
    try result.drain();

    return try columns.toOwnedSlice();
}

fn translateColumnType(name: []const u8) jetquery.Column.Type {
    // TODO
    const types = std.StaticStringMap(jetquery.Column.Type).initComptime(.{
        .{ "integer", jetquery.Column.Type.integer },
        .{ "real", jetquery.Column.Type.float },
        .{ "boolean", jetquery.Column.Type.boolean },
        .{ "numeric", jetquery.Column.Type.decimal },
        .{ "character varying", jetquery.Column.Type.string },
        .{ "text", jetquery.Column.Type.text },
        .{ "timestamp without time zone", jetquery.Column.Type.datetime },
        .{ "timestamp with time zone", jetquery.Column.Type.datetime },
    });
    return types.get(name) orelse {
        std.log.err("Unsupported column type: `{s}`\n", .{name});
        unreachable;
    };
}

fn initPool(allocator: std.mem.Allocator, options: Options) !*pg.Pool {
    return try pg.Pool.init(allocator, .{
        .size = options.pool_size.?,
        .connect = .{
            .port = options.port.?,
            .host = options.hostname.?,
        },
        .auth = .{
            .username = options.username orelse return configError("username"),
            .database = options.database orelse return configError("database"),
            .password = options.password orelse return configError("password"),
            .timeout = options.timeout.?,
        },
    });
}

fn configError(comptime name: []const u8) error{JetQueryConfigError} {
    const message = "Missing expected configuration value: `" ++ name ++ "`";
    if (builtin.is_test) { // https://github.com/ziglang/zig/issues/5738
        std.log.warn(message, .{});
    } else {
        std.log.err(message, .{});
    }
    return error.JetQueryConfigError;
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
