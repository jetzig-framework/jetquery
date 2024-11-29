const std = @import("std");

const jetquery = @import("../jetquery.zig");

pub const Connection = union(enum) {
    postgresql: jetquery.adapters.PostgresqlAdapter.Connection,

    pub fn execute(
        self: Connection,
        query: anytype,
        caller_info: ?jetquery.debug.CallerInfo,
        repo: anytype,
    ) !switch (@TypeOf(query).ResultContext) {
        .one => ?@TypeOf(query).ResultType,
        .many => jetquery.Result(@TypeOf(repo.*)),
        .none => void,
    } {
        return switch (self) {
            .postgresql => |*connection| result_blk: {
                try query.validateValues();
                try query.validateDelete();
                var result = try connection.execute(
                    query.sql,
                    query.values,
                    caller_info,
                    repo,
                );
                break :result_blk switch (@TypeOf(query).ResultContext) {
                    .one => blk: {
                        // TODO: Create a new ResultContext `.unary` instead of hacking it in here.
                        if (query.query_context == .count) {
                            defer result.deinit();
                            const unary = try result.unary(@TypeOf(query).ResultType);
                            try result.drain();
                            break :blk unary;
                        } else {
                            // We use `all` instead of `next` here because we only expect one
                            // result. Using `next` when we are preloading relations (e.g. with
                            // `include`) creates an extra connection so that we can continue
                            // streaming the results of the primary query with subsequent calls
                            // to `next()`. Since there is only one expected result, `all()`
                            // should always return a slice with length 1, so we can use a single
                            // connection to fetch the primary result row + relations, freeing up
                            // a connection from the pool.
                            const results = try result.all(query);
                            defer repo.allocator.free(results);
                            std.debug.assert(results.len == 1 or results.len == 0);
                            break :blk if (results.len == 1) results[0] else null;
                        }
                    },
                    .many => result,
                    .none => blk: {
                        try result.drain();
                        defer result.deinit();
                        break :blk {};
                    },
                };
            },
        };
    }

    /// Execute SQL with the active adapter without returning a result.
    pub fn executeVoid(
        self: Connection,
        sql: []const u8,
        values: anytype,
        caller_info: ?jetquery.debug.CallerInfo,
        repo: anytype,
    ) !void {
        var result = switch (self) {
            inline else => |connection| try connection.execute(sql, values, caller_info, repo),
        };
        try result.drain();
        result.deinit();
    }

    /// Execute SQL with the active adapter and return a result (same as `execute` but accepts an
    /// SQL string and values instead of a generated query).
    pub fn executeSql(
        self: Connection,
        sql: []const u8,
        values: anytype,
        caller_info: ?jetquery.debug.CallerInfo,
        repo: anytype,
    ) !jetquery.Result(@TypeOf(repo.*)) {
        return switch (self) {
            inline else => |connection| try connection.execute(sql, values, caller_info, repo),
        };
    }

    /// Release connection to the pool.
    pub fn release(self: Connection) void {
        switch (self) {
            inline else => |connection| connection.release(),
        }
    }

    pub fn isAvailable(self: Connection) bool {
        return switch (self) {
            inline else => |connection| connection.isAvailable(),
        };
    }

    pub fn executeVoidRuntimeBind(
        self: Connection,
        sql: []const u8,
        values: anytype,
        comptime Args: type,
        args: Args,
        field_states: []const jetquery.sql.FieldState,
        caller_info: ?jetquery.debug.CallerInfo,
        repo: anytype,
    ) !void {
        var result = switch (self) {
            inline else => |connection| try connection.executeRuntimeBind(
                sql,
                values,
                Args,
                args,
                field_states,
                caller_info,
                repo,
            ),
        };
        try result.drain();
        result.deinit();
    }
};
