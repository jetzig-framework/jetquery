const std = @import("std");

const migrations = @import("migrations").migrations;
const jetquery = @import("jetquery");

const Migrate = @This();

repo: *jetquery.Repo,

pub fn init(repo: *jetquery.Repo) Migrate {
    return .{ .repo = repo };
}

/// Run migrations. Create `jetquery_migrations` table if it does not exist.
pub fn run(self: Migrate) !void {
    try self.createMigrationsTable();
    for (migrations) |migration| {
        try migration.upFn(self.repo);
        std.debug.print("migration: {any}\n", .{migration});
    }
}

fn createMigrationsTable(self: Migrate) !void {
    _ = self;
    // TODO
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
