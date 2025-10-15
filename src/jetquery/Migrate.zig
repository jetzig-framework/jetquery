const std = @import("std");

const jetcommon = @import("jetcommon");

const migrations = @import("migrations").migrations;
const Migration = @import("migrations").Migration;
const jetquery = @import("jetquery");

pub const MigrateSchema = struct {
    pub const Migrations = jetquery.Model(
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

pub fn Migrate(adapter_name: jetquery.adapters.Name) type {
    return struct {
        const Self = @This();
        const AdaptedRepo = jetquery.Repo(adapter_name, MigrateSchema);

        repo: *AdaptedRepo,

        pub fn init(repo: *AdaptedRepo) Self {
            return .{ .repo = repo };
        }

        /// Run migrations. Create `jetquery_migrations` table if it does not exist. Skip migrations
        /// already present in `jetquery_migrations`.
        pub fn migrate(self: Self) !void {
            try self.createMigrationsTable();

            log("\n* Running migrations.\n", .{});

            var count: usize = 0;

            inline for (migrations) |migration| {
                if (!try self.isMigrated(migration)) {
                    log("\n=== [MIGRATE:BEGIN] {s} ===\n", .{migration.name});

                    try migration.upFn(self.repo);
                    try self.repo.Query(.Migrations)
                        .insert(.{ .version = migration.version, .name = migration.name })
                        .execute(self.repo);

                    count += 1;

                    log("\n=== [MIGRATE:COMPLETE] {s} ===\n", .{migration.name});
                }
            }

            log("\n* Applied {} migration(s).\n", .{count});
        }

        pub fn rollback(self: Self) !void {
            if (migrations.len == 0) {
                log("No applied migrations detected. Exiting.", .{});
                return;
            }

            const last_migration = try self.repo.Query(.Migrations)
                .orderBy(.{ .version = .desc })
                .first(self.repo) orelse return;
            defer self.repo.free(last_migration);

            var applied = false;
            inline for (migrations) |migration| {
                if (!applied and std.mem.eql(u8, migration.version, last_migration.version)) {
                    log("\n=== [ROLLBACK:BEGIN] {s} ===\n", .{migration.name});
                    try migration.downFn(self.repo);
                    try self.repo.delete(last_migration);
                    // Just in case we somehow end up with two migrations with the same version (e.g.
                    // user manually copied a file), we want to only apply one of them:
                    applied = true;
                    log("\n=== [ROLLBACK:COMPLETE] {s} ===\n", .{migration.name});
                }
            }
        }

        fn isMigrated(self: Self, migration: Migration) !bool {
            const result = try self.repo.Query(.Migrations)
                .findBy(.{ .version = migration.version }).execute(self.repo);
            defer self.repo.free(result);

            return result != null;
        }

        fn createMigrationsTable(self: Self) !void {
            try self.repo.createTable(
                "jetquery_migrations",
                &.{
                    jetquery.schema.table.primaryKey("version", .{ .type = .string }),
                    jetquery.schema.table.column("name", .string, .{ .length = 1024 }),
                    jetquery.schema.table.timestamps(.{ .updated_at = false }),
                },
                .{ .if_not_exists = true },
            );
        }

        fn log(comptime message: []const u8, args: anytype) void {
            std.debug.print(message ++ "\n", args);
        }
    };
}

test "migrate" {
    try resetDatabase();

    const TestSchema = struct {
        pub const Cat = jetquery.Model(@This(), "cats", struct {
            id: i32,
            name: []const u8,
            paws: i8,
            created_at: jetcommon.types.DateTime,
            updated_at: jetcommon.types.DateTime,
            human_id: i32,
        }, .{});
        pub const Human = jetquery.Model(
            @This(),
            "humans",
            struct {
                id: i32,
                name: []const u8,
            },
            .{ .relations = .{ .cats = jetquery.relation.hasMany(.Cat, .{}) } },
        );
        pub const DefaultsTest = jetquery.Model(
            @This(),
            "defaults_test",
            struct {
                id: i32,
                name: []const u8,
                count: i32,
                active: bool,
                description: []const u8,
                score: f32,
                no_default: ?[]const u8,
                price: f64,
                small_count: i16,
                big_count: i64,
                precise_value: f64,
                last_update: jetcommon.types.DateTime,
                created_at: jetcommon.types.DateTime,
                updated_at: jetcommon.types.DateTime,
            },
            .{},
        );
    };

    var migrate_repo = try jetquery.Repo(.postgresql, MigrateSchema).init(
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
    defer migrate_repo.deinit();

    const migrate = Migrate(.postgresql).init(&migrate_repo);
    try migrate.migrate();

    var test_repo = try jetquery.Repo(.postgresql, TestSchema).init(
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
    defer test_repo.deinit();

    const migration = try migrate_repo.Query(.Migrations)
        .findBy(.{ .version = "2024-08-26_13-18-52" })
        .execute(&migrate_repo);
    defer migrate_repo.free(migration);

    try std.testing.expect(migration != null);

    const human_cats_query = test_repo.Query(.Human)
        .join(.inner, .cats)
        .select(.{ .id, .name, .{ .cats = .{ .name, .paws, .created_at, .updated_at } } });
    var result = try test_repo.execute(human_cats_query);
    try result.drain();
    defer result.deinit();

    {
        const defaults_query = test_repo.Query(.DefaultsTest)
            .select(.{ .id, .name, .count, .active, .description, .score, .no_default, .price, .small_count, .big_count, .precise_value, .last_update, .created_at, .updated_at });
        var defaults_result = try test_repo.execute(defaults_query);
        defer defaults_result.deinit();
        try defaults_result.drain();
    }

    {
        const jq = test_repo.Query(.DefaultsTest);

        // Insert a new row with default values, overriding the name
        try jq.insert(.{ .name = "Jane Smith" }).execute(&test_repo);

        const inserted = try test_repo.Query(.DefaultsTest).first(&test_repo) orelse return error.NotFound;
        defer test_repo.free(inserted);

        try std.testing.expectEqual(42, inserted.count);
        try std.testing.expect(inserted.active);
        try std.testing.expectEqualStrings("Jane Smith", inserted.name); // Overriden
        try std.testing.expectEqual(3.14, inserted.score);
        try std.testing.expectEqual(19.99, inserted.price);
        try std.testing.expectEqual(5, inserted.small_count);
        try std.testing.expectEqual(9223372036854775807, inserted.big_count);
        try std.testing.expectEqual(3.141592653589793, inserted.precise_value);
        try std.testing.expectEqualStrings("This is a default description with 'quotes' and other special characters!", inserted.description);
    }

    try migrate.rollback();

    const new_defaults_query = test_repo.Query(.DefaultsTest)
        .select(.{ .id, .name, .count });
    const new_defaults_result = test_repo.execute(new_defaults_query);
    try std.testing.expect(new_defaults_result == error.PG);

    // Human-cats query should still work after first rollback
    var human_cats_result = try test_repo.execute(human_cats_query);
    try human_cats_result.drain();
    defer human_cats_result.deinit();

    // After rolling back defaults_test, human table should still exist
    {
        const humans = try test_repo.Query(.Human).all(&test_repo);
        defer test_repo.free(humans);
    }

    // Rollback the create_cats migration
    try migrate.rollback();

    // After rolling back cats, human table should still exist but can't join to cats
    const human_cats_result2 = test_repo.execute(human_cats_query);
    try std.testing.expect(human_cats_result2 == error.PG);

    // Rollback the create_humans migration
    try migrate.rollback();

    // After rolling back humans, human table should no longer exist
    {
        const humans = test_repo.Query(.Human).all(&test_repo);
        try std.testing.expect(humans == error.PG);
    }
}

fn resetDatabase() !void {
    const S = struct {};
    var repo = try jetquery.Repo(.postgresql, S).init(
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
