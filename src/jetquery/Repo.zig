const std = @import("std");

const jetquery = @import("../jetquery.zig");

const Adapter = union(enum) {
    postgresql: jetquery.adapters.PostgresqlAdapter,

    pub fn execute(self: *Adapter, sql: []const u8) !jetquery.Result {
        return switch (self.*) {
            inline else => |*adapter| try adapter.execute(sql),
        };
    }
};

allocator: std.mem.Allocator,
adapter: Adapter,

const Repo = @This();

const Options = union(enum) {
    postgresql: jetquery.adapters.PostgresqlAdapter.Options,
};

/// Initialize a new Repo for executing queries.
pub fn init(allocator: std.mem.Allocator, options: Options) !Repo {
    return .{
        .allocator = allocator,
        .adapter = switch (options) {
            .postgresql => |adapter_options| .{
                .postgresql = try jetquery.adapters.PostgresqlAdapter.init(
                    allocator,
                    adapter_options,
                ),
            },
        },
    };
}

/// Close connections and free resources.
pub fn deinit(self: *Repo) void {
    switch (self.adapter) {
        inline else => |*adapter| adapter.deinit(),
    }
}

/// Execute the given query and return results.
pub fn execute(self: *Repo, query: anytype) !jetquery.Result {
    var buf: [4096]u8 = undefined;
    return try self.adapter.execute(try query.toSql(&buf));
}

test "repo" {
    var repo = try Repo.init(
        std.testing.allocator,
        .{
            .postgresql = .{
                .database = "postgres",
                .username = "postgres",
                .hostname = "127.0.0.1",
                .password = "password",
                .port = 5432,
            },
        },
    );
    defer repo.deinit();

    const Schema = struct {
        pub const Cats = jetquery.Table("cats", struct { name: []const u8, paws: usize }, .{});
    };

    var create_table = try repo.adapter.execute("create table cats (name varchar(255), paws int)");
    defer create_table.deinit();
    var insert = try repo.adapter.execute("insert into cats (name, paws) values ('Hercules', 4)");
    defer insert.deinit();

    const query = jetquery.Query(Schema.Cats).init(std.testing.allocator).select(&.{ .name, .paws });
    defer query.deinit();

    var result = try repo.execute(query);
    defer result.deinit();

    while (try result.next()) |row| {
        defer row.deinit();
        const value = row.get([]const u8, "name") orelse return std.testing.expect(false);
        try std.testing.expectEqualStrings("Hercules", value);
        break;
    } else {
        try std.testing.expect(false);
    }
}
