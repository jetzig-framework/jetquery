const std = @import("std");

const jetcommon = @import("jetcommon");

const migrations = @import("migrations").migrations;
const Migration = @import("migrations").Migration;
const jetquery = @import("jetquery");

const Migrate = @This();

repo: *jetquery.Repo,

pub fn init(repo: *jetquery.Repo) Migrate {
    return .{ .repo = repo };
}

const Schema = struct {
    pub const Migrations = jetquery.Table(
        @This(),
        "jetquery_migrations",
        struct { version: []const u8 },
        .{},
    );
};

/// Run migrations. Create `jetquery_migrations` table if it does not exist. Skip migrations
/// already present in `jetquery_migrations`.
pub fn run(self: Migrate) !void {
    try self.createMigrationsTable();

    try self.repo.eventCallback(.{
        .context = .migration,
        .message = "Running migrations.",
    });

    inline for (migrations) |migration| {
        if (!try self.isMigrated(migration)) {
            try self.repo.eventCallback(.{
                .context = .migration,
                .message = "Executing migration: " ++ migration.name,
            });

            try jetquery.Query(Schema, .Migrations)
                .insert(.{ .version = migration.version })
                .execute(self.repo);

            try migration.upFn(self.repo);

            try self.repo.eventCallback(.{
                .context = .migration,
                .message = "Completed migration: " ++ migration.name,
            });
        }
    }

    try self.repo.eventCallback(.{
        .context = .migration,
        .message = "Migrations completed.",
    });
}

fn isMigrated(self: Migrate, migration: Migration) !bool {
    const query = jetquery.Query(Schema, .Migrations)
        .select(.{.version})
        .where(.{ .version = migration.version });

    var result = try self.repo.execute(query);
    defer result.deinit();

    while (try result.next(query)) |_| {
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
        .{ .if_not_exists = true },
    );
}

test "migrate" {
    var repo = try jetquery.Repo.init(
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

    try repo.dropTable("jetquery_migrations", .{ .if_exists = true });
    try repo.dropTable("cats", .{ .if_exists = true });
    try repo.dropTable("humans", .{ .if_exists = true });

    const migrate = Migrate.init(&repo);
    try migrate.run();

    const query1 = jetquery.Query(Schema, .Migrations)
        .select(.{.version});
    var result1 = try repo.execute(query1);
    defer result1.deinit();

    while (try result1.next(query1)) |row| {
        defer repo.free(row);
        try std.testing.expectEqualStrings("2024-08-26_13-18-52", row.version);
        break;
    } else {
        try std.testing.expect(false);
    }

    const TestSchema = struct {
        pub const Cat = jetquery.Table(@This(), "cats", struct {
            name: []const u8,
            paws: usize,
            created_at: jetcommon.types.DateTime,
            updated_at: jetcommon.types.DateTime,
        }, .{});
    };
    const query2 = jetquery.Query(TestSchema, .Cat)
        .select(.{ .name, .paws, .created_at, .updated_at });
    var result2 = try repo.execute(query2);
    defer result2.deinit();
}
