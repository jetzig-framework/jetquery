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
                    log("\nExecuting migration: === {s} ===\n", .{migration.name});

                    try migration.upFn(self.repo);
                    try self.repo.Query(.Migrations)
                        .insert(.{ .version = migration.version, .name = migration.name })
                        .execute(self.repo);

                    count += 1;

                    log("\nCompleted migration: === {s} ===\n", .{migration.name});
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
                    log("\nRolling back migration: === {s} ===\n", .{migration.name});
                    try migration.downFn(self.repo);
                    try self.repo.delete(last_migration);
                    // Just in case we somehow end up with two migrations with the same version (e.g.
                    // user manually copied a file), we want to only apply one of them:
                    applied = true;
                    log("\nCompleted rollback: === {s} ===\n", .{migration.name});
                }
            }
        }

        fn isMigrated(self: Self, migration: Migration) !bool {
            const result = try self.repo.Query(.Migrations)
                .findBy(.{ .version = migration.version }).execute(self.repo);

            return result != null;
        }

        fn createMigrationsTable(self: Self) !void {
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

    const query = test_repo.Query(.Human)
        .join(.inner, .cats)
        .select(.{ .id, .name, .{ .cats = .{ .name, .paws, .created_at, .updated_at } } });
    var result = try test_repo.execute(query);
    try result.drain();
    defer result.deinit();

    try migrate.rollback();

    try std.testing.expectError(error.PG, test_repo.execute(query));

    {
        const humans = try test_repo.Query(.Human).all(&test_repo);
        defer test_repo.free(humans);
    }

    try migrate.rollback();

    {
        const humans = test_repo.Query(.Human).all(&test_repo);
        try std.testing.expectError(error.PG, humans);
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
