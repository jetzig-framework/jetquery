const std = @import("std");

const jetcommon = @import("jetcommon");

const migrations = @import("migrations").migrations;
const Migration = @import("migrations").Migration;
const jetquery = @import("jetquery");

const Migrate = @This();

repo: *jetquery.Repo(jetquery.adapter),

pub fn init(repo: *jetquery.Repo(jetquery.adapter)) Migrate {
    return .{ .repo = repo };
}

const Schema = struct {
    pub const Migrations = jetquery.Table(
        @This(),
        "jetquery_migrations",
        struct {
            version: []const u8,
            name: []const u8,
            created_at: jetquery.DateTime,
        },
        .{ .primary_key = .version },
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
                .insert(.{ .version = migration.version, .name = migration.name })
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

pub fn rollback(self: Migrate) !void {
    if (migrations.len == 0) return;

    const last_migration = try jetquery.Query(Schema, .Migrations)
        .orderBy(.{ .version = .desc })
        .first(self.repo) orelse return;
    defer self.repo.free(last_migration);

    var applied = false;
    inline for (migrations) |migration| {
        if (!applied and std.mem.eql(u8, migration.version, last_migration.version)) {
            try migration.downFn(self.repo);
            try self.repo.delete(last_migration);
            // Just in case we somehow end up with two migrations with the same version (e.g.
            // user manually copied a file), we want to only apply one of them:
            applied = true;
        }
    }
}

fn isMigrated(self: Migrate, migration: Migration) !bool {
    const result = try jetquery.Query(Schema, .Migrations)
        .findBy(.{ .version = migration.version }).execute(self.repo);

    return result != null;
}

fn createMigrationsTable(self: Migrate) !void {
    try self.repo.createTable(
        "jetquery_migrations",
        &.{
            jetquery.schema.table.primaryKey("version", .{ .type = .string }),
            jetquery.schema.table.column("name", .string, .{ .not_null = true, .length = 1024 }),
            jetquery.schema.table.timestamps(.{ .updated_at = false }),
        },
        .{ .if_not_exists = true },
    );
}

test "migrate" {
    try resetDatabase();
    var repo = try jetquery.Repo(.postgresql).init(
        std.testing.allocator,
        .{
            .adapter = .{
                .database = "migrate_test",
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

    const migration = try jetquery.Query(Schema, .Migrations)
        .findBy(.{ .version = "2024-08-26_13-18-52" })
        .execute(&repo);
    defer repo.free(migration);

    try std.testing.expect(migration != null);

    const TestSchema = struct {
        pub const Cat = jetquery.Table(@This(), "cats", struct {
            id: i32,
            name: []const u8,
            paws: i8,
            created_at: jetcommon.types.DateTime,
            updated_at: jetcommon.types.DateTime,
            human_id: i32,
        }, .{});
        pub const Human = jetquery.Table(
            @This(),
            "humans",
            struct {
                id: i32,
                name: []const u8,
            },
            .{ .relations = .{ .cats = jetquery.relation.hasMany(.Cat, .{}) } },
        );
    };

    const query = jetquery.Query(TestSchema, .Human)
        .join(.inner, .cats)
        .select(.{ .id, .name, .{ .cats = .{ .name, .paws, .created_at, .updated_at } } });
    var result = try repo.execute(query);
    try result.drain();
    defer result.deinit();

    try migrate.rollback();

    try std.testing.expectError(error.PG, repo.execute(query));

    {
        const humans = try jetquery.Query(TestSchema, .Human).all(&repo);
        defer repo.free(humans);
    }

    try migrate.rollback();

    {
        const humans = jetquery.Query(TestSchema, .Human).all(&repo);
        try std.testing.expectError(error.PG, humans);
    }
}

fn resetDatabase() !void {
    var repo = try jetquery.Repo(.postgresql).init(
        std.testing.allocator,
        .{
            .adapter = .{
                .database = "postgres",
                .username = "postgres",
                .hostname = "127.0.0.1",
                .password = "password",
                .port = 5432,
            },
        },
    );
    defer repo.deinit();
    try repo.dropDatabase("migrate_test", .{ .if_exists = true });
    try repo.createDatabase("migrate_test", .{});
}
