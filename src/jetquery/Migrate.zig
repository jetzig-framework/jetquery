const std = @import("std");

const migrations = @import("migrations").migrations;
const Migration = @import("migrations").Migration;
const jetquery = @import("jetquery");

const Migrate = @This();

repo: *jetquery.Repo,

pub fn init(repo: *jetquery.Repo) Migrate {
    return .{ .repo = repo };
}

const Schema = struct {
    pub const Migrations = jetquery.Table("jetquery_migrations", struct { version: []const u8 }, .{});
};

/// Run migrations. Create `jetquery_migrations` table if it does not exist. Skip migrations
/// already present in `jetquery_migrations`.
pub fn run(self: Migrate) !void {
    try self.createMigrationsTable();

    for (migrations) |migration| {
        if (try self.isMigrated(migration)) continue;

        const query = jetquery.Query(Schema.Migrations).init(self.repo.allocator)
            .insert(.{ .version = migration.version });
        defer query.deinit();

        try migration.upFn(self.repo);

        var result = try self.repo.execute(query);
        defer result.deinit();
    }
}

fn isMigrated(self: Migrate, migration: Migration) !bool {
    const query = jetquery.Query(Schema.Migrations).init(self.repo.allocator)
        .select(&.{.version})
        .where(.{ .version = migration.version });
    defer query.deinit();

    var result = try self.repo.execute(query);
    defer result.deinit();

    while (try result.next()) |row| {
        defer row.deinit();
        return true;
    }

    return false;
}

fn createMigrationsTable(self: Migrate) !void {
    try self.repo.createTable(
        "jetquery_migrations",
        &.{
            jetquery.table.column("version", .string, .{}),
        },
    );
}

test "migrate" {
    var repo = try jetquery.Repo.init(
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
    const migrate = Migrate.init(&repo);
    try migrate.run();
}
