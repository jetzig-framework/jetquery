const std = @import("std");

const jetcommon = @import("jetcommon");

const seeders = @import("seeders").seeders;
const Seeder = @import("seeders").Seeder;
const jetquery = @import("jetquery");

pub const SeederSchema = struct {};

pub fn Seed(adapter_name: jetquery.adapters.Name, schema: anytype) type {
    return struct {
        const Self = @This();
        const AdaptedRepo = jetquery.Repo(adapter_name, schema);

        repo: *AdaptedRepo,

        pub fn init(repo: *AdaptedRepo) Self {
            return .{ .repo = repo };
        }

        pub fn seed(self: Self) !void {
            log("\n* Running seeds.\n", .{});

            var count: usize = 0;
            inline for (seeders) |item| {
                log("\n=== [SEED:BEGIN] {s} ===\n", .{item.name});

                try item.runFn(self.repo);
                count += 1;

                log("\n=== [SEED:COMPLETE] {s} ===\n", .{item.name});
            }

            log("\n* Applied {} seed(s).\n", .{count});
        }

        fn log(comptime message: []const u8, args: anytype) void {
            std.debug.print(message ++ "\n", args);
        }
    };
}

test "seed" {
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
                created_at: jetcommon.types.DateTime,
                updated_at: jetcommon.types.DateTime,
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

    {
        const Migration = @import("jetquery_migrate");
        var migrate_repo = try jetquery.Repo(.postgresql, Migration.MigrateSchema).init(
            std.testing.allocator,
            .{
                .adapter = .{
                    .database = "seeders_test",
                    .username = "postgres",
                    .hostname = "127.0.0.1",
                    .password = "password",
                    .port = 5432,
                },
            },
        );
        defer migrate_repo.deinit();

        const migrate = Migration.Migrate(.postgresql).init(&migrate_repo);
        try migrate.migrate();
    }

    var seeder_repo = try jetquery.Repo(.postgresql, TestSchema).init(
        std.testing.allocator,
        .{
            .adapter = .{
                .database = "seeders_test",
                .username = "postgres",
                .hostname = "127.0.0.1",
                .password = "password",
                .port = 5432,
            },
        },
    );
    defer seeder_repo.deinit();

    {
        const human_count = try seeder_repo.execute(seeder_repo.Query(.Human).count());
        try std.testing.expect(human_count != null);
        try std.testing.expect(human_count.? == 0);

        const cat_count = try seeder_repo.execute(seeder_repo.Query(.Cat).count());
        try std.testing.expect(cat_count != null);
        try std.testing.expect(cat_count.? == 0);
    }

    const seeder = Seed(.postgresql, TestSchema).init(&seeder_repo);
    try seeder.seed();

    var test_repo = try jetquery.Repo(.postgresql, TestSchema).init(
        std.testing.allocator,
        .{
            .adapter = .{
                .database = "seeders_test",
                .username = "postgres",
                .hostname = "127.0.0.1",
                .password = "password",
                .port = 5432,
            },
        },
    );
    defer test_repo.deinit();

    {
        const human_count = try test_repo.execute(seeder_repo.Query(.Human).count());
        try std.testing.expect(human_count != null);
        try std.testing.expect(human_count.? > 0);

        const cat_count = try test_repo.execute(seeder_repo.Query(.Cat).count());
        try std.testing.expect(cat_count != null);
        try std.testing.expect(cat_count.? > 0);
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
    try repo.dropDatabase("seeders_test", .{ .if_exists = true });
    try repo.createDatabase("seeders_test", .{});
}
