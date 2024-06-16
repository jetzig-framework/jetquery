const std = @import("std");

const jetquery = @import("../jetquery.zig");

const Result = struct {
    x: ?usize = null,
};

const Adapter = union(enum) {
    postgresql: PostgreSQLAdapter,

    pub fn execute(self: Adapter, sql: []const u8) !Result {
        _ = self;
        _ = sql;
        return .{};
    }
};

const PostgreSQLAdapter = struct {
    database: []const u8,
    username: []const u8,
    password: []const u8,
    hostname: []const u8,
    port: u16 = 5432,
};

allocator: std.mem.Allocator,
adapter: Adapter,

const Repo = @This();

/// Initialize a new Repo for executing queries.
pub fn init(allocator: std.mem.Allocator, adapter: Adapter) Repo {
    return .{ .allocator = allocator, .adapter = adapter };
}

/// Execute the given query and return results.
pub fn execute(self: Repo, query: anytype) !Result {
    var buf: [4096]u8 = undefined;
    return try self.adapter.execute(try query.toSql(&buf));
}

test "repo" {
    const repo = Repo.init(
        std.testing.allocator,
        .{
            .postgresql = .{
                .database = "testdb",
                .username = "testuser",
                .hostname = "127.0.0.1",
                .password = "testpass",
                .port = 1025,
            },
        },
    );

    const Schema = struct {
        pub const Cats = jetquery.Table("cats", struct { name: []const u8, paws: usize }, .{});
    };

    const query = jetquery.Query(Schema.Cats).init(std.testing.allocator).select(&.{ .name, .paws });
    defer query.deinit();

    const result = try repo.execute(query);
    try std.testing.expect(result.x == null);
}
