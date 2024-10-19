const std = @import("std");

pub const jetcommon = @import("jetcommon");

pub const config = @import("jetquery.config");

pub const Repo = @import("jetquery/Repo.zig");
pub const adapters = @import("jetquery/adapters.zig");
pub const sql = @import("jetquery/sql.zig");
pub const relation = @import("jetquery/relation.zig");
pub const Migration = @import("jetquery/Migration.zig");
pub const table = @import("jetquery/schema/table.zig");
pub const fields = @import("jetquery/fields.zig");
pub const columns = @import("jetquery/columns.zig");
pub const default_column_names = @import("jetquery/default_column_names.zig");
pub const Row = @import("jetquery/Row.zig");
pub const Result = @import("jetquery/Result.zig").Result;
pub const events = @import("jetquery/events.zig");
pub const Query = @import("jetquery/Query.zig").Query;
pub const Table = @import("jetquery/Table.zig").Table;
pub const Column = @import("jetquery/schema/Column.zig");
pub const Value = @import("jetquery/Value.zig").Value;
pub const DateTime = jetcommon.types.DateTime;
pub const debug = @import("jetquery/debug.zig");

pub const adapter = std.enums.nameCast(adapters.Name, config.database.adapter);
pub const timestamp_updated_column_name = "updated_at";
pub const timestamp_created_column_name = "created_at";
pub const original_prefix = "__original_";

// Can be switched to `std.meta.DeclEnum` if https://github.com/ziglang/zig/pull/21331 is merged
// (or fixed otherwise) to prevent overflow on empty struct.
pub fn DeclEnum(T: type) type {
    comptime {
        const decls = std.meta.declarations(T);
        var enum_fields: [decls.len]std.builtin.Type.EnumField = undefined;
        const TagType = std.math.IntFittingRange(
            0,
            if (decls.len == 0) 0 else decls.len - 1,
        );

        for (decls, 0..) |decl, index| {
            enum_fields[index] = .{ .name = decl.name, .value = index };
        }

        return @Type(.{
            .@"enum" = .{
                .tag_type = TagType,
                .fields = &enum_fields,
                .decls = &.{},
                .is_exhaustive = true,
            },
        });
    }
}

test {
    std.testing.refAllDecls(@This());
}

test "select" {
    const Schema = struct {
        pub const Cat = Table(@This(), "cats", struct { name: []const u8, paws: i32 }, .{});
    };
    const query = Query(Schema, .Cat).select(.{ .name, .paws });

    try std.testing.expectEqualStrings(
        \\SELECT "cats"."name", "cats"."paws" FROM "cats" WHERE (1 = 1)
    , query.sql);
}

test "select (all)" {
    const Schema = struct {
        pub const Cat = Table(@This(), "cats", struct { name: []const u8, paws: i32 }, .{});
    };
    const query = Query(Schema, .Cat).select(.{});

    try std.testing.expectEqualStrings(
        \\SELECT "cats"."name", "cats"."paws" FROM "cats" WHERE (1 = 1)
    , query.sql);
}

test "select (with `where`)" {
    const Schema = struct {
        pub const Cat = Table(@This(), "cats", struct { name: []const u8, paws: i32 }, .{});
    };

    const paws = 4;
    const query = Query(Schema, .Cat)
        .select(.{ .name, .paws })
        .where(.{ .name = "bar", .paws = paws });

    try std.testing.expectEqualStrings(
        \\SELECT "cats"."name", "cats"."paws" FROM "cats" WHERE ("cats"."name" = $1 AND "cats"."paws" = $2)
    , query.sql);
}

test "where" {
    const Schema = struct {
        pub const Cat = Table(@This(), "cats", struct { name: []const u8, paws: i32, color: []const u8 }, .{});
    };

    const paws = 4;
    const query = Query(Schema, .Cat)
        .where(.{ .name = "bar", .paws = paws });

    try std.testing.expectEqualStrings(
        \\SELECT "cats"."name", "cats"."paws", "cats"."color" FROM "cats" WHERE ("cats"."name" = $1 AND "cats"."paws" = $2)
    , query.sql);
}

test "where (multiple)" {
    const Schema = struct {
        pub const Cat = Table(@This(), "cats", struct { name: []const u8, paws: i32 }, .{});
    };

    const query = Query(Schema, .Cat)
        .select(.{ .name, .paws })
        .where(.{ .name = "bar" })
        .where(.{ .paws = 4 });

    try std.testing.expectEqualStrings(
        \\SELECT "cats"."name", "cats"."paws" FROM "cats" WHERE "cats"."name" = $1 AND "cats"."paws" = $2
    , query.sql);
}

test "limit" {
    const Schema = struct {
        pub const Cat = Table(@This(), "cats", struct { name: []const u8, paws: i32 }, .{});
    };
    const query = Query(Schema, .Cat)
        .select(.{ .name, .paws })
        .limit(100);

    try std.testing.expectEqualStrings(
        \\SELECT "cats"."name", "cats"."paws" FROM "cats" WHERE (1 = 1) LIMIT $1
    , query.sql);
}

test "order by" {
    const Schema = struct {
        pub const Cat = Table(@This(), "cats", struct { name: []const u8, paws: i32 }, .{});
    };
    const query = Query(Schema, .Cat)
        .select(.{ .name, .paws })
        .orderBy(.{ .name = .ascending });

    try std.testing.expectEqualStrings(
        \\SELECT "cats"."name", "cats"."paws" FROM "cats" WHERE (1 = 1) ORDER BY "cats"."name" ASC
    , query.sql);
}

test "insert" {
    const Schema = struct {
        pub const Cat = Table(@This(), "cats", struct { name: []const u8, paws: i32 }, .{});
    };
    const query = Query(Schema, .Cat)
        .insert(.{ .name = "Hercules", .paws = 4 });

    try std.testing.expectEqualStrings(
        \\INSERT INTO "cats" ("name", "paws") VALUES ($1, $2)
    , query.sql);
}

test "update" {
    const Schema = struct {
        pub const Cat = Table(@This(), "cats", struct { name: []const u8, paws: i32 }, .{});
    };
    const query = Query(Schema, .Cat)
        .update(.{ .name = "Heracles", .paws = 2 })
        .where(.{ .name = "Hercules" });

    try std.testing.expectEqualStrings(
        \\UPDATE "cats" SET "name" = $1, "paws" = $2 WHERE "cats"."name" = $3
    ,
        query.sql,
    );
}

test "delete" {
    const Schema = struct {
        pub const Cat = Table(@This(), "cats", struct { name: []const u8, paws: i32 }, .{});
    };
    const query = Query(Schema, .Cat)
        .delete()
        .where(.{ .name = "Hercules" });

    try std.testing.expectEqualStrings(
        \\DELETE FROM "cats" WHERE "cats"."name" = $1
    , query.sql);
}

test "delete (without where clause)" {
    const Schema = struct {
        pub const Cat = Table(@This(), "cats", struct { name: []const u8, paws: i32 }, .{});
    };
    const query = Query(Schema, .Cat).delete();

    try std.testing.expectError(error.JetQueryUnsafeDelete, query.validateDelete());
}

test "deleteAll" {
    const Schema = struct {
        pub const Cat = Table(@This(), "cats", struct { name: []const u8, paws: i32 }, .{});
    };
    const query = Query(Schema, .Cat)
        .deleteAll();

    try std.testing.expectEqualStrings(
        \\DELETE FROM "cats" WHERE (1 = 1)
    , query.sql);
}

test "find" {
    const Schema = struct {
        pub const Cat = Table(@This(), "cats", struct { id: i32, name: []const u8, paws: i32 }, .{});
    };
    const query = Query(Schema, .Cat).find(1000);

    try std.testing.expectEqualStrings(
        \\SELECT "cats"."id", "cats"."name", "cats"."paws" FROM "cats" WHERE "cats"."id" = $1 LIMIT $2
    , query.sql);
}

test "find (with coerced id)" {
    const Schema = struct {
        pub const Cat = Table(@This(), "cats", struct { id: i32, name: []const u8, paws: i32 }, .{});
    };
    const query = Query(Schema, .Cat).find("1000");

    try std.testing.expectEqualStrings(
        \\SELECT "cats"."id", "cats"."name", "cats"."paws" FROM "cats" WHERE "cats"."id" = $1 LIMIT $2
    , query.sql);
}

test "findBy" {
    const Schema = struct {
        pub const Cat = Table(@This(), "cats", struct { id: i32, name: []const u8, paws: i32 }, .{});
    };
    const query = Query(Schema, .Cat).findBy(.{ .name = "Hercules", .paws = 4 });

    try std.testing.expectEqualStrings(
        \\SELECT "cats"."id", "cats"."name", "cats"."paws" FROM "cats" WHERE ("cats"."name" = $1 AND "cats"."paws" = $2) LIMIT $3
    , query.sql);
}

test "count()" {
    const Schema = struct {
        pub const Cat = Table(@This(), "cats", struct { id: i32, name: []const u8, paws: i32 }, .{});
    };
    const query = Query(Schema, .Cat).where(.{ .name = "Hercules", .paws = 4 }).count();

    try std.testing.expectEqualStrings(
        \\SELECT COUNT(*) FROM "cats" WHERE ("cats"."name" = $1 AND "cats"."paws" = $2)
    , query.sql);
}

test "distinct().count()" {
    const Schema = struct {
        pub const Cat = Table(@This(), "cats", struct { id: i32, name: []const u8, paws: i32 }, .{});
    };
    const query = Query(Schema, .Cat).where(.{ .name = "Hercules", .paws = 4 }).distinct(.{.name}).count();

    try std.testing.expectEqualStrings(
        \\SELECT COUNT(DISTINCT("cats"."name")) FROM "cats" WHERE ("cats"."name" = $1 AND "cats"."paws" = $2)
    , query.sql);
}

test "nested distinct().count()" {
    const Schema = struct {
        pub const Cat = Table(@This(), "cats", struct { id: i32, name: []const u8, paws: i32 }, .{});
        pub const Human = Table(
            @This(),
            "humans",
            struct { cat_id: i32, name: []const u8 },
            .{
                .relations = .{ .cat = relation.belongsTo(.Cat, .{}) },
            },
        );
    };
    const query = Query(Schema, .Human).include(.cat, .{}).where(.{ .name = "Bob" }).distinct(.{ .name, .{ .cat = .{.name} } }).count();

    try std.testing.expectEqualStrings(
        \\SELECT COUNT(DISTINCT("humans"."name", "cats"."name")) FROM "humans" INNER JOIN "cats" ON "humans"."cat_id" = "cats"."id" WHERE "humans"."name" = $1
    , query.sql);
}

test "combined" {
    const Schema = struct {
        pub const Cat = Table(@This(), "cats", struct { id: i32, name: []const u8, paws: i32 }, .{});
    };
    const query = Query(Schema, .Cat)
        .select(.{ .name, .paws })
        .where(.{ .name = "Hercules" })
        .limit(10)
        .orderBy(.{ .name = .ascending });

    try std.testing.expectEqualStrings(
        \\SELECT "cats"."name", "cats"."paws" FROM "cats" WHERE "cats"."name" = $1 ORDER BY "cats"."name" ASC LIMIT $2
    , query.sql);
}

test "runtime field values" {
    const Schema = struct {
        pub const Cat = Table(@This(), "cats", struct { name: []const u8, paws: i32 }, .{});
    };
    var hercules_buf: [8]u8 = undefined;
    const hercules = try std.fmt.bufPrint(&hercules_buf, "{s}", .{"Hercules"});
    var heracles_buf: [8]u8 = undefined;
    const heracles = try std.fmt.bufPrint(&heracles_buf, "{s}", .{"Heracles"});
    const query = Query(Schema, .Cat)
        .update(.{ .name = heracles, .paws = 2 })
        .where(.{ .name = hercules });
    const values = query.values();
    try std.testing.expectEqualStrings("Heracles", values.@"0");
    try std.testing.expectEqual(2, values.@"1");
    try std.testing.expectEqualStrings("Hercules", values.@"2");
}

test "boolean coercion" {
    const Schema = struct {
        pub const Cat = Table(@This(), "cats", struct { name: []const u8, intelligent: bool }, .{});
    };
    const query = Query(Schema, .Cat)
        .select(.{.name})
        .where(.{ .intelligent = "1" });

    try std.testing.expectEqual(query.values().@"0", true);
}

test "integer coercion" {
    const Schema = struct {
        pub const Cat = Table(@This(), "cats", struct { name: []const u8, paws: i32 }, .{});
    };
    const query = Query(Schema, .Cat)
        .select(.{.name})
        .where(.{ .paws = "4" });

    try std.testing.expectEqual(query.values().@"0", 4);
}

test "float coercion" {
    const Schema = struct {
        pub const Cat = Table(@This(), "cats", struct { name: []const u8, intelligence: f64 }, .{});
    };
    const query = Query(Schema, .Cat)
        .select(.{.name})
        .where(.{ .intelligence = "10.2" });

    try std.testing.expectEqual(query.values().@"0", 10.2);
}

test "toJetQuery()" {
    const Schema = struct {
        pub const Cat = Table(@This(), "cats", struct { name: []const u8, paws: i32, color: []const u8 }, .{});
    };

    const Name = struct {
        pub fn toJetQuery(self: @This(), T: type) !T {
            _ = self;
            return switch (T) {
                []const u8 => "Hercules",
                else => @compileError("Cannot coerce to " ++ @typeName(T)),
            };
        }
    };

    const Paws = struct {
        pub fn toJetQuery(self: @This(), T: type) !T {
            _ = self;
            return switch (T) {
                i32 => 4,
                else => @compileError("Cannot coerce to " ++ @typeName(T)),
            };
        }
    };

    const Color = struct {
        pub fn toJetQuery(self: @This(), T: type) !T {
            _ = self;
            return switch (T) {
                []const u8 => "Black and white",
                else => @compileError("Cannot coerce to " ++ @typeName(T)),
            };
        }
    };

    const name = Name{};
    const paws = Paws{};
    const color = Color{};

    const query = Query(Schema, .Cat)
        .insert(.{ .name = name, .paws = &paws, .color = color });
    try std.testing.expectEqualStrings(
        \\INSERT INTO "cats" ("name", "paws", "color") VALUES ($1, $2, $3)
    , query.sql);
    const values = query.values();
    try std.testing.expectEqualStrings(values.@"0", "Hercules");
    try std.testing.expectEqual(values.@"1", 4);
}

test "failed coercion (bool)" {
    const Schema = struct {
        pub const Cat = Table(@This(), "cats", struct { name: []const u8, intelligent: bool }, .{});
    };
    const query = Query(Schema, .Cat)
        .select(.{.name})
        .where(.{ .intelligent = "not a bool" });

    try std.testing.expectError(error.JetQueryInvalidBooleanString, query.validateValues());
}

test "failed coercion (int)" {
    const Schema = struct {
        pub const Cat = Table(@This(), "cats", struct { name: []const u8, paws: i32 }, .{});
    };
    const query = Query(Schema, .Cat)
        .select(.{.name})
        .where(.{ .paws = "not an int" });

    try std.testing.expectError(error.JetQueryInvalidIntegerString, query.validateValues());
}

test "failed coercion (float)" {
    const Schema = struct {
        pub const Cat = Table(@This(), "cats", struct { name: []const u8, intelligence: f64 }, .{});
    };
    const query = Query(Schema, .Cat)
        .select(.{.name})
        .where(.{ .intelligence = "not a float" });

    try std.testing.expectError(error.JetQueryInvalidFloatString, query.validateValues());
}

test "timestamps (create)" {
    const Schema = struct {
        pub const Cat = Table(
            @This(),
            "cats",
            struct { name: []const u8, paws: i32, created_at: i64, updated_at: i64 },
            .{},
        );
    };
    const query = Query(Schema, .Cat)
        .insert(.{ .name = "Hercules", .paws = 4 });

    try std.testing.expectEqualStrings(
        \\INSERT INTO "cats" ("name", "paws", "created_at", "updated_at") VALUES ($1, $2, $3, $4)
    , query.sql);
}

test "timestamps (update)" {
    const Schema = struct {
        pub const Cat = Table(
            @This(),
            "cats",
            struct { name: []const u8, paws: i32, created_at: i64, updated_at: i64 },
            .{},
        );
    };
    const query = Query(Schema, .Cat)
        .update(.{ .name = "Heracles", .paws = 2 })
        .where(.{ .name = "Hercules" });

    try std.testing.expectEqualStrings(
        \\UPDATE "cats" SET "name" = $1, "paws" = $2, "updated_at" = $3 WHERE "cats"."name" = $4
    ,
        query.sql,
    );
}

test "belongsTo" {
    const Schema = struct {
        pub const Human = Table(
            @This(),
            "humans",
            struct { id: i32, cat_id: i32, name: []const u8 },
            .{ .relations = .{ .cat = relation.belongsTo(.Cat, .{}) } },
        );

        pub const Cat = Table(
            @This(),
            "cats",
            struct { id: i32, name: []const u8, paws: i32, created_at: i64, updated_at: i64 },
            .{ .relations = .{} },
        );
    };
    const query = Query(Schema, .Human)
        .include(.cat, .{})
        .findBy(.{ .name = "Bob" });

    try std.testing.expectEqualStrings(
        \\SELECT "humans"."id", "humans"."cat_id", "humans"."name", "cats"."id", "cats"."name", "cats"."paws", "cats"."created_at", "cats"."updated_at" FROM "humans" INNER JOIN "cats" ON "humans"."cat_id" = "cats"."id" WHERE "humans"."name" = $1 LIMIT $2
    ,
        query.sql,
    );
}

test "belongsTo (multiple)" {
    const Schema = struct {
        pub const Human = Table(
            @This(),
            "humans",
            struct { id: i32, family_id: i32, cat_id: i32, name: []const u8 },
            .{
                .relations = .{
                    .cat = relation.belongsTo(.Cat, .{}),
                    .family = relation.belongsTo(.Family, .{}),
                },
            },
        );

        pub const Cat = Table(
            @This(),
            "cats",
            struct { id: i32, name: []const u8, paws: i32, created_at: i64, updated_at: i64 },
            .{},
        );

        pub const Family = Table(
            @This(),
            "families",
            struct { id: i32, name: []const u8 },
            .{},
        );
    };
    const query = Query(Schema, .Human)
        .include(.cat, .{})
        .include(.family, .{})
        .findBy(.{ .name = "Bob" });

    try std.testing.expectEqualStrings(
        \\SELECT "humans"."id", "humans"."family_id", "humans"."cat_id", "humans"."name", "cats"."id", "cats"."name", "cats"."paws", "cats"."created_at", "cats"."updated_at", "families"."id", "families"."name" FROM "humans" INNER JOIN "cats" ON "humans"."cat_id" = "cats"."id" INNER JOIN "families" ON "humans"."family_id" = "families"."id" WHERE "humans"."name" = $1 LIMIT $2
    ,
        query.sql,
    );
}

test "belongsTo (with specified columns)" {
    const Schema = struct {
        pub const Human = Table(
            @This(),
            "humans",
            struct { id: i32, family_id: i32, cat_id: i32, name: []const u8 },
            .{
                .relations = .{
                    .cat = relation.belongsTo(.Cat, .{}),
                    .family = relation.belongsTo(.Family, .{}),
                },
            },
        );

        pub const Cat = Table(
            @This(),
            "cats",
            struct { id: i32, name: []const u8, paws: i32, created_at: i64, updated_at: i64 },
            .{},
        );

        pub const Family = Table(
            @This(),
            "families",
            struct { id: i32, name: []const u8 },
            .{},
        );
    };
    const query = Query(Schema, .Human)
        .include(.cat, .{ .name, .paws })
        .include(.family, .{.name})
        .select(.{.name})
        .findBy(.{ .name = "Bob" });

    try std.testing.expectEqualStrings(
        \\SELECT "humans"."name", "cats"."name", "cats"."paws", "families"."name" FROM "humans" INNER JOIN "cats" ON "humans"."cat_id" = "cats"."id" INNER JOIN "families" ON "humans"."family_id" = "families"."id" WHERE "humans"."name" = $1 LIMIT $2
    ,
        query.sql,
    );
}

test "hasMany" {
    const Schema = struct {
        pub const Human = Table(
            @This(),
            "humans",
            struct { id: i32, cat_id: i32, name: []const u8 },
            .{ .relations = .{ .cat = relation.belongsTo(.Cat, .{}) } },
        );

        pub const Cat = Table(
            @This(),
            "cats",
            struct { id: i32, name: []const u8, paws: i32, created_at: i64, updated_at: i64 },
            .{ .relations = .{ .humans = relation.hasMany(.Human, .{}) } },
        );
    };
    const query = Query(Schema, .Cat)
        .include(.humans, .{})
        .findBy(.{ .name = "Hercules" });

    try std.testing.expectEqualStrings(
        \\SELECT "cats"."id", "cats"."name", "cats"."paws", "cats"."created_at", "cats"."updated_at" FROM "cats" WHERE "cats"."name" = $1 LIMIT $2
    ,
        query.sql,
    );

    try std.testing.expectEqualStrings(
        \\SELECT "humans"."id", "humans"."cat_id", "humans"."name" FROM "humans" WHERE "humans"."cat_id" = $1
    ,
    // Only the base query is generated at this point, the repo appends the where clause
    // after fetching results of the first query. This is tested more thoroughly in `Repo.zig`
        query.auxiliary_queries[0].query.where(.{ .cat_id = 1 }).sql,
    );
}

test "nested where" {
    const Schema = struct {
        pub const Human = Table(
            @This(),
            "humans",
            struct { id: i32, family_id: i32, cat_id: i32, name: []const u8 },
            .{
                .relations = .{
                    .cat = relation.belongsTo(.Cat, .{}),
                    .family = relation.belongsTo(.Family, .{}),
                },
            },
        );

        pub const Cat = Table(
            @This(),
            "cats",
            struct { id: i32, name: []const u8, paws: i32, created_at: i64, updated_at: i64 },
            .{},
        );

        pub const Family = Table(
            @This(),
            "families",
            struct { id: i32, name: []const u8 },
            .{},
        );
    };
    const query = Query(Schema, .Human)
        .include(.cat, .{})
        .include(.family, .{})
        .where(.{
        .name = "Bob",
        .cat = .{ .name = "Hercules" },
        .family = .{ .name = "Farrell" },
    });

    try std.testing.expectEqualStrings(
        \\SELECT "humans"."id", "humans"."family_id", "humans"."cat_id", "humans"."name", "cats"."id", "cats"."name", "cats"."paws", "cats"."created_at", "cats"."updated_at", "families"."id", "families"."name" FROM "humans" INNER JOIN "cats" ON "humans"."cat_id" = "cats"."id" INNER JOIN "families" ON "humans"."family_id" = "families"."id" WHERE ("humans"."name" = $1 AND "cats"."name" = $2 AND "families"."name" = $3)
    ,
        query.sql,
    );
}

test "operator logic" {
    const Schema = struct {
        pub const Human = Table(
            @This(),
            "humans",
            struct { id: i32, family_id: i32, cat_id: i32, name: []const u8 },
            .{
                .relations = .{
                    .cat = relation.belongsTo(.Cat, .{}),
                    .family = relation.belongsTo(.Family, .{}),
                },
            },
        );

        pub const Cat = Table(
            @This(),
            "cats",
            struct { id: i32, name: []const u8, paws: i32, created_at: i64, updated_at: i64 },
            .{},
        );

        pub const Family = Table(
            @This(),
            "families",
            struct { id: i32, name: []const u8 },
            .{},
        );
    };
    const query = Query(Schema, .Human)
        .include(.cat, .{})
        .include(.family, .{})
        .where(.{
        .{ .name = "Bob" },
        .OR,
        .{ .name = "T-Rex" },
        .OR,
        .{ .cat = .{ .name = "Hercules" } },
        .NOT,
        .{ .family = .{ .name = "Farrell" } },
    });

    try std.testing.expectEqualStrings(
        \\SELECT "humans"."id", "humans"."family_id", "humans"."cat_id", "humans"."name", "cats"."id", "cats"."name", "cats"."paws", "cats"."created_at", "cats"."updated_at", "families"."id", "families"."name" FROM "humans" INNER JOIN "cats" ON "humans"."cat_id" = "cats"."id" INNER JOIN "families" ON "humans"."family_id" = "families"."id" WHERE ("humans"."name" = $1 OR "humans"."name" = $2 OR "cats"."name" = $3 AND NOT "families"."name" = $4)
    ,
        query.sql,
    );
}

test "slice of []const u8 in whereclause" {
    const Schema = struct {
        pub const Human = Table(
            @This(),
            "humans",
            struct { name: []const u8 },
            .{},
        );
    };
    var array = std.ArrayList([]const u8).init(std.testing.allocator);
    defer array.deinit();

    try array.append("Bob");
    try array.append("Jane");

    const query = Query(Schema, .Human).where(.{ .name = array.items });
    try std.testing.expectEqualStrings(
        \\SELECT "humans"."name" FROM "humans" WHERE "humans"."name" = ANY ($1)
    , query.sql);
}

test "slice of int in whereclause" {
    const Schema = struct {
        pub const Human = Table(
            @This(),
            "humans",
            struct { cats: u128 },
            .{},
        );
    };
    var array = std.ArrayList(u128).init(std.testing.allocator);
    defer array.deinit();

    try array.append(2);
    try array.append(1231231238128381283);

    const query = Query(Schema, .Human).where(.{ .cats = array.items });
    try std.testing.expectEqualStrings(
        \\SELECT "humans"."cats" FROM "humans" WHERE "humans"."cats" = ANY ($1)
    , query.sql);
}

test "slice of float in whereclause" {
    const Schema = struct {
        pub const Human = Table(
            @This(),
            "humans",
            struct { favorite_number: f64 },
            .{},
        );
    };
    var array = std.ArrayList(f64).init(std.testing.allocator);
    defer array.deinit();

    try array.append(3.1415926535897932);
    try array.append(2.7182818284590452);

    const query = Query(Schema, .Human).where(.{ .favorite_number = array.items });
    try std.testing.expectEqualStrings(
        \\SELECT "humans"."favorite_number" FROM "humans" WHERE "humans"."favorite_number" = ANY ($1)
    , query.sql);
}

test "slice of bool in whereclause" {
    const Schema = struct {
        pub const Human = Table(
            @This(),
            "humans",
            struct { has_cats: bool },
            .{},
        );
    };
    var array = std.ArrayList(bool).init(std.testing.allocator);
    defer array.deinit();

    try array.append(true);
    try array.append(false);

    const query = Query(Schema, .Human).where(.{ .has_cats = array.items });
    try std.testing.expectEqualStrings(
        \\SELECT "humans"."has_cats" FROM "humans" WHERE "humans"."has_cats" = ANY ($1)
    , query.sql);
}

test "null in whereclause" {
    const Schema = struct {
        pub const Human = Table(
            @This(),
            "humans",
            struct { name: []const u8 },
            .{},
        );
    };

    const query = Query(Schema, .Human).where(.{ .{ .name = null }, .OR, .{ .name = "baz" } });
    try std.testing.expectEqualStrings(
        \\SELECT "humans"."name" FROM "humans" WHERE ("humans"."name" IS NULL OR "humans"."name" = $1)
    , query.sql);
}

test "groupBy" {
    const Schema = struct {
        pub const Cat = Table(
            @This(),
            "cats",
            struct { name: []const u8 },
            .{},
        );
    };

    const query = Query(Schema, .Cat).groupBy(.{.name});
    try std.testing.expectEqualStrings(
        \\SELECT FROM "cats" WHERE (1 = 1) GROUP BY "cats"."name"
    , query.sql);
}

test "aggregate max()" {
    const Schema = struct {
        pub const Cat = Table(
            @This(),
            "cats",
            struct { name: []const u8, paws: usize },
            .{},
        );
    };

    const query = Query(Schema, .Cat)
        .select(.{ .name, sql.max(.paws) })
        .groupBy(.{.name});
    try std.testing.expectEqualStrings(
        \\SELECT "cats"."name", MAX("cats"."paws") FROM "cats" WHERE (1 = 1) GROUP BY "cats"."name"
    , query.sql);
}

test "aggregate min()" {
    const Schema = struct {
        pub const Cat = Table(
            @This(),
            "cats",
            struct { name: []const u8, paws: usize },
            .{},
        );
    };

    const query = Query(Schema, .Cat)
        .select(.{ .name, sql.min(.paws) })
        .groupBy(.{.name});
    try std.testing.expectEqualStrings(
        \\SELECT "cats"."name", MIN("cats"."paws") FROM "cats" WHERE (1 = 1) GROUP BY "cats"."name"
    , query.sql);
}

test "aggregate count()" {
    const Schema = struct {
        pub const Cat = Table(
            @This(),
            "cats",
            struct { name: []const u8, paws: usize },
            .{},
        );
    };

    const query = Query(Schema, .Cat)
        .select(.{ .name, sql.count(.paws) })
        .groupBy(.{.name});
    try std.testing.expectEqualStrings(
        \\SELECT "cats"."name", COUNT("cats"."paws") FROM "cats" WHERE (1 = 1) GROUP BY "cats"."name"
    , query.sql);
}

test "aggregate avg()" {
    const Schema = struct {
        pub const Cat = Table(
            @This(),
            "cats",
            struct { name: []const u8, paws: usize },
            .{},
        );
    };

    const query = Query(Schema, .Cat)
        .select(.{ .name, sql.avg(.paws) })
        .groupBy(.{.name});
    try std.testing.expectEqualStrings(
        \\SELECT "cats"."name", AVG("cats"."paws") FROM "cats" WHERE (1 = 1) GROUP BY "cats"."name"
    , query.sql);
}

test "aggregate sum()" {
    const Schema = struct {
        pub const Cat = Table(
            @This(),
            "cats",
            struct { name: []const u8, paws: usize },
            .{},
        );
    };

    const query = Query(Schema, .Cat)
        .select(.{ .name, sql.sum(.paws) })
        .groupBy(.{.name});
    try std.testing.expectEqualStrings(
        \\SELECT "cats"."name", SUM("cats"."paws") FROM "cats" WHERE (1 = 1) GROUP BY "cats"."name"
    , query.sql);
}

test "like/ilike" {
    const Schema = struct {
        pub const Cat = Table(
            @This(),
            "cats",
            struct { name: []const u8, paws: usize },
            .{},
        );
    };

    const query = Query(Schema, .Cat)
        .select(.{.name})
        .where(.{ .{ .name, .like, "Herc%" }, .OR, .{ .name, .ilike, "princ%" } });
    try std.testing.expectEqualStrings(
        \\SELECT "cats"."name" FROM "cats" WHERE ("cats"."name" LIKE $1 OR "cats"."name" ILIKE $2)
    , query.sql);
}

test "inner join" {
    const Schema = struct {
        pub const Human = Table(
            @This(),
            "humans",
            struct { id: i32, family_id: i32, cat_id: i32, name: []const u8 },
            .{
                .relations = .{
                    .cat = relation.belongsTo(.Cat, .{}),
                    .family = relation.belongsTo(.Family, .{}),
                },
            },
        );

        pub const Cat = Table(
            @This(),
            "cats",
            struct { id: i32, name: []const u8, paws: i32, created_at: i64, updated_at: i64 },
            .{},
        );
    };

    const query = Query(Schema, .Human)
        .join(.inner, .cat)
        .select(.{.name});
    try std.testing.expectEqualStrings(
        \\SELECT "humans"."name" FROM "humans" INNER JOIN "cats" ON "humans"."cat_id" = "cats"."id" WHERE (1 = 1)
    , query.sql);
}

test "outer join" {
    const Schema = struct {
        pub const Human = Table(
            @This(),
            "humans",
            struct { id: i32, family_id: i32, cat_id: i32, name: []const u8 },
            .{
                .relations = .{
                    .cat = relation.belongsTo(.Cat, .{}),
                    .family = relation.belongsTo(.Family, .{}),
                },
            },
        );

        pub const Cat = Table(
            @This(),
            "cats",
            struct { id: i32, name: []const u8, paws: i32, created_at: i64, updated_at: i64 },
            .{},
        );
    };

    const query = Query(Schema, .Human)
        .join(.outer, .cat)
        .select(.{.name});
    try std.testing.expectEqualStrings(
        \\SELECT "humans"."name" FROM "humans" LEFT OUTER JOIN "cats" ON "humans"."cat_id" = "cats"."id" WHERE (1 = 1)
    , query.sql);
}

test "inner and outer join" {
    const Schema = struct {
        pub const Human = Table(
            @This(),
            "humans",
            struct { id: i32, family_id: i32, cat_id: i32, name: []const u8 },
            .{
                .relations = .{
                    .cat = relation.belongsTo(.Cat, .{}),
                    .family = relation.belongsTo(.Family, .{}),
                },
            },
        );

        pub const Cat = Table(
            @This(),
            "cats",
            struct { id: i32, name: []const u8, paws: i32, created_at: i64, updated_at: i64 },
            .{},
        );

        pub const Family = Table(
            @This(),
            "families",
            struct { id: i32, name: []const u8 },
            .{},
        );
    };

    const query = Query(Schema, .Human)
        .join(.inner, .cat)
        .join(.outer, .family)
        .select(.{.name});
    try std.testing.expectEqualStrings(
        \\SELECT "humans"."name" FROM "humans" INNER JOIN "cats" ON "humans"."cat_id" = "cats"."id" LEFT OUTER JOIN "families" ON "humans"."family_id" = "families"."id" WHERE (1 = 1)
    , query.sql);
}

test "inner and outer join with select on relation columns" {
    const Schema = struct {
        pub const Human = Table(
            @This(),
            "humans",
            struct { id: i32, family_id: i32, cat_id: i32, name: []const u8 },
            .{
                .relations = .{
                    .cat = relation.belongsTo(.Cat, .{}),
                    .family = relation.belongsTo(.Family, .{}),
                },
            },
        );

        pub const Cat = Table(
            @This(),
            "cats",
            struct { id: i32, name: []const u8, paws: i32, created_at: i64, updated_at: i64 },
            .{},
        );

        pub const Family = Table(
            @This(),
            "families",
            struct { id: i32, name: []const u8 },
            .{},
        );
    };

    const query = Query(Schema, .Human)
        .join(.inner, .cat)
        .join(.outer, .family)
        .select(.{ .name, .{ .family = .{.id}, .cat = .{.paws} } });
    try std.testing.expectEqualStrings(
        \\SELECT "humans"."name", "families"."id", "cats"."paws" FROM "humans" INNER JOIN "cats" ON "humans"."cat_id" = "cats"."id" LEFT OUTER JOIN "families" ON "humans"."family_id" = "families"."id" WHERE (1 = 1)
    , query.sql);
}
