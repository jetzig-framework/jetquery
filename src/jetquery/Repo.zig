const std = @import("std");

const jetquery = @import("../jetquery.zig");

const Adapter = union(enum) {
    postgresql: jetquery.adapters.PostgresqlAdapter,

    pub fn execute(self: Adapter, sql: []const u8) !jetquery.Result {
        return switch (self) {
            inline else => |adapter| try adapter.execute(sql),
        };
    }
};

allocator: std.mem.Allocator,
adapter: Adapter,

const Repo = @This();

/// Initialize a new Repo for executing queries.
pub fn init(allocator: std.mem.Allocator, adapter: Adapter) Repo {
    return .{ .allocator = allocator, .adapter = adapter };
}

/// Execute the given query and return results.
pub fn execute(self: Repo, query: anytype) !jetquery.Result {
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
