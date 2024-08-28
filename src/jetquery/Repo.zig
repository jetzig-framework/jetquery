const std = @import("std");

const jetquery = @import("../jetquery.zig");

allocator: std.mem.Allocator,
adapter: jetquery.adapters.Adapter,

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
    return try self.adapter.execute(try query.toSql(&buf, self.adapter));
}

pub fn createTable(self: *Repo, name: []const u8, columns: []const jetquery.Column) !void {
    var buf = std.ArrayList(u8).init(self.allocator);
    defer buf.deinit();

    const writer = buf.writer();

    try writer.print(
        \\create table "{s}" (
    , .{name});

    for (columns, 0..) |column, index| {
        if (column.timestamps) {
            try writer.print(
                \\"created_at" {0s}, "updated_at" {0s}{1s}
            , .{
                self.adapter.columnTypeSql(.datetime),
                if (index < columns.len - 1) ", " else "",
            });
        } else {
            try writer.print(
                \\"{s}" {s}{s}
            , .{
                column.name,
                self.adapter.columnTypeSql(column.type),
                if (index < columns.len - 1) ", " else "",
            });
        }
    }

    try writer.print(")", .{});
    var result = try self.adapter.execute(buf.items);
    defer result.deinit();
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

    while (try result.next(query)) |row| {
        try std.testing.expectEqualStrings("Hercules", row.name);
        break;
    } else {
        try std.testing.expect(false);
    }
}
