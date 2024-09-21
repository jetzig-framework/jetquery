const std = @import("std");

const jetquery = @import("../jetquery.zig");

allocator: std.mem.Allocator,
adapter: jetquery.adapters.Adapter,
eventCallback: *const fn (event: jetquery.events.Event) anyerror!void = jetquery.events.defaultCallback,

const Repo = @This();

const Options = struct {
    adapter: union(enum) {
        postgresql: jetquery.adapters.PostgresqlAdapter.Options,
        null,
    },
    eventCallback: *const fn (event: jetquery.events.Event) anyerror!void = jetquery.events.defaultCallback,
};

/// Initialize a new Repo for executing queries.
pub fn init(allocator: std.mem.Allocator, options: Options) !Repo {
    return .{
        .allocator = allocator,
        .adapter = switch (options.adapter) {
            .postgresql => |adapter_options| .{
                .postgresql = try jetquery.adapters.PostgresqlAdapter.init(
                    allocator,
                    adapter_options,
                ),
            },
            .null => .{ .null = jetquery.adapters.NullAdapter{} },
        },
        .eventCallback = options.eventCallback,
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
    try query.validateValues();
    return try self.adapter.execute(self, try query.toSql(&buf, self.adapter), query.field_values);
}

pub const CreateTableOptions = struct { if_not_exists: bool = false };

/// Create a database table named `nme`. Pass `.{ .if_not_exists = true }` to use
/// `CREATE TABLE IF NOT EXISTS` syntax.
pub fn createTable(self: *Repo, name: []const u8, columns: []const jetquery.Column, options: CreateTableOptions) !void {
    var buf = std.ArrayList(u8).init(self.allocator);
    defer buf.deinit();

    const writer = buf.writer();

    try writer.print(
        \\CREATE TABLE{s} {} (
    , .{ if (options.if_not_exists) " IF NOT EXISTS" else "", self.adapter.identifier(name) });

    for (columns, 0..) |column, index| {
        if (column.timestamps) {
            try writer.print(
                \\{0} {2s}, {1} {2s}{3s}
            , .{
                self.adapter.identifier("created_at"),
                self.adapter.identifier("updated_at"),
                self.adapter.columnTypeSql(.datetime),
                if (index < columns.len - 1) ", " else "",
            });
        } else {
            try writer.print(
                \\{0} {1s} {2s}{3s}
            , .{
                self.adapter.identifier(column.name),
                if (column.primary_key) "" else self.adapter.columnTypeSql(column.type),
                if (column.primary_key) self.adapter.primaryKeySql() else "",
                if (index < columns.len - 1) ", " else "",
            });
        }
    }

    try writer.print(")", .{});
    var result = try self.adapter.execute(self, buf.items, &.{});
    try result.drain();
    defer result.deinit();
}

pub const DropTableOptions = struct { if_exists: bool = false };

/// Drop a database table named `name`. Pass `.{ .if_exists = true }` to use
/// `DROP TABLE IF EXISTS` syntax.
pub fn dropTable(self: *Repo, name: []const u8, options: DropTableOptions) !void {
    var buf = std.ArrayList(u8).init(self.allocator);
    defer buf.deinit();

    const writer = buf.writer();

    try writer.print(
        \\DROP TABLE{s} "{s}"
    , .{ if (options.if_exists) " IF EXISTS" else "", name });

    var result = try self.adapter.execute(self, buf.items, &.{});
    try result.drain();
    defer result.deinit();
}

test "repo" {
    var repo = try Repo.init(
        std.testing.allocator,
        .{
            .adapter = .{
                .postgresql = .{
                    .database = "postgres",
                    .username = "postgres",
                    .hostname = "127.0.0.1",
                    .password = "password",
                    .port = 5432,
                },
            },
        },
    );
    defer repo.deinit();

    const Schema = struct {
        pub const Cats = jetquery.Table("cats", struct { name: []const u8, paws: usize }, .{});
    };

    var drop_table = try repo.adapter.execute(&repo, "drop table if exists cats", &.{});
    defer drop_table.deinit();

    var create_table = try repo.adapter.execute(&repo, "create table cats (name varchar(255), paws int)", &.{});
    defer create_table.deinit();
    var insert = try repo.adapter.execute(&repo, "insert into cats (name, paws) values ('Hercules', 4)", &.{});
    defer insert.deinit();

    const query = jetquery.Query(Schema.Cats)
        .select(&.{ .name, .paws })
        .where(.{ .name = "Hercules", .paws = 4 });

    var result = try repo.execute(query);
    defer result.deinit();

    while (try result.next(query)) |row| {
        try std.testing.expectEqualStrings("Hercules", row.name);
        try std.testing.expectEqual(4, row.paws);
        break;
    } else {
        try std.testing.expect(false);
    }
}
