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

    const migration = try seeder_repo.Query(.SeedersTest)
        .findBy(.{ .version = "2024-08-26_13-18-52" })
        .execute(&seeder_repo);
    defer seeder_repo.free(migration);

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

    try seeder.rollback();

    const new_defaults_query = test_repo.Query(.DefaultsTest)
        .select(.{ .id, .name, .count });
    try std.testing.expectError(error.PG, test_repo.execute(new_defaults_query));

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
    try seeder.rollback();

    // After rolling back cats, human table should still exist but can't join to cats
    try std.testing.expectError(error.PG, test_repo.execute(human_cats_query));

    // Rollback the create_humans migration
    try seeder.rollback();

    // After rolling back humans, human table should no longer exist
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
    try repo.dropDatabase("seeders_test", .{ .if_exists = true });
    try repo.createDatabase("seeders_test", .{});
}
