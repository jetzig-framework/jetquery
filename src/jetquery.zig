const std = @import("std");

pub const jetcommon = @import("jetcommon");

pub const config = @import("jetquery.config");

pub const Repo = @import("jetquery/Repo.zig");
pub const adapters = @import("jetquery/adapters.zig");
pub const sql = @import("jetquery/sql.zig");
pub const relation = @import("jetquery/relation.zig");
pub const Migration = @import("jetquery/Migration.zig");
pub const table = @import("jetquery/table.zig");
pub const column_names = @import("jetquery/column_names.zig");
pub const Row = @import("jetquery/Row.zig");
pub const Result = @import("jetquery/Result.zig").Result;
pub const events = @import("jetquery/events.zig");
pub const Query = @import("jetquery/Query.zig").Query;
pub const Table = @import("jetquery/Table.zig").Table;
pub const Column = @import("jetquery/Column.zig");
pub const Value = @import("jetquery/Value.zig").Value;

pub const adapter = std.enums.nameCast(adapters.Name, config.database.adapter);
pub const timestamp_updated_column_name = "updated_at";
pub const timestamp_created_column_name = "created_at";

// Can be switched to `std.meta.DeclEnum` if https://github.com/ziglang/zig/pull/21331 is merged
// (or fixed otherwise) to prevent overflow on empty struct.
pub fn DeclEnum(T: type) type {
    comptime {
        const decls = std.meta.declarations(T);
        var fields: [decls.len]std.builtin.Type.EnumField = undefined;
        const TagType = std.math.IntFittingRange(
            0,
            if (decls.len == 0) 0 else decls.len - 1,
        );

        for (decls, 0..) |decl, index| {
            fields[index] = .{ .name = decl.name, .value = index };
        }

        return @Type(.{
            .@"enum" = .{
                .tag_type = TagType,
                .fields = &fields,
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
        pub const Cat = Table("cats", struct { name: []const u8, paws: i32 }, .{});
    };
    const query = Query(Schema, .Cat).select(&.{ .name, .paws });

    try std.testing.expectEqualStrings(
        \\SELECT "cats"."name", "cats"."paws" FROM "cats"
    , query.sql);
}

test "select (all)" {
    const Schema = struct {
        pub const Cat = Table("cats", struct { name: []const u8, paws: i32 }, .{});
    };
    const query = Query(Schema, .Cat).select(&.{});

    try std.testing.expectEqualStrings(
        \\SELECT "cats"."name", "cats"."paws" FROM "cats"
    , query.sql);
}

test "select (with `where`)" {
    const Schema = struct {
        pub const Cat = Table("cats", struct { name: []const u8, paws: i32 }, .{});
    };

    const paws = 4;
    const query = Query(Schema, .Cat)
        .select(&.{ .name, .paws })
        .where(.{ .name = "bar", .paws = paws });

    try std.testing.expectEqualStrings(
        \\SELECT "cats"."name", "cats"."paws" FROM "cats" WHERE "cats"."name" = $1 AND "cats"."paws" = $2
    , query.sql);
}

test "where" {
    const Schema = struct {
        pub const Cat = Table("cats", struct { name: []const u8, paws: i32, color: []const u8 }, .{});
    };

    const paws = 4;
    const query = Query(Schema, .Cat)
        .where(.{ .name = "bar", .paws = paws });

    try std.testing.expectEqualStrings(
        \\SELECT "cats"."name", "cats"."paws", "cats"."color" FROM "cats" WHERE "cats"."name" = $1 AND "cats"."paws" = $2
    , query.sql);
}

test "where (multiple)" {
    const Schema = struct {
        pub const Cat = Table("cats", struct { name: []const u8, paws: i32 }, .{});
    };

    const query = Query(Schema, .Cat)
        .select(&.{ .name, .paws })
        .where(.{ .name = "bar" })
        .where(.{ .paws = 4 });

    try std.testing.expectEqualStrings(
        \\SELECT "cats"."name", "cats"."paws" FROM "cats" WHERE "cats"."name" = $1 AND "cats"."paws" = $2
    , query.sql);
}

test "limit" {
    const Schema = struct {
        pub const Cat = Table("cats", struct { name: []const u8, paws: i32 }, .{});
    };
    const query = Query(Schema, .Cat)
        .select(&.{ .name, .paws })
        .limit(100);

    try std.testing.expectEqualStrings(
        \\SELECT "cats"."name", "cats"."paws" FROM "cats" LIMIT $1
    , query.sql);
}

test "order by" {
    const Schema = struct {
        pub const Cat = Table("cats", struct { name: []const u8, paws: i32 }, .{});
    };
    const query = Query(Schema, .Cat)
        .select(&.{ .name, .paws })
        .orderBy(.{ .name = .ascending });

    try std.testing.expectEqualStrings(
        \\SELECT "cats"."name", "cats"."paws" FROM "cats" ORDER BY "cats"."name" ASC
    , query.sql);
}

test "insert" {
    const Schema = struct {
        pub const Cat = Table("cats", struct { name: []const u8, paws: i32 }, .{});
    };
    const query = Query(Schema, .Cat)
        .insert(.{ .name = "Hercules", .paws = 4 });

    try std.testing.expectEqualStrings(
        \\INSERT INTO "cats" ("name", "paws") VALUES ($1, $2)
    , query.sql);
}

test "update" {
    const Schema = struct {
        pub const Cat = Table("cats", struct { name: []const u8, paws: i32 }, .{});
    };
    const query = Query(Schema, .Cat)
        .update(.{ .name = "Heracles", .paws = 2 })
        .where(.{ .name = "Hercules" });

    try std.testing.expectEqualStrings(
        \\UPDATE "cats" SET "cats"."name" = $1, "cats"."paws" = $2 WHERE "cats"."name" = $3
    ,
        query.sql,
    );
}

test "delete" {
    const Schema = struct {
        pub const Cat = Table("cats", struct { name: []const u8, paws: i32 }, .{});
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
        pub const Cat = Table("cats", struct { name: []const u8, paws: i32 }, .{});
    };
    const query = Query(Schema, .Cat).delete();

    try std.testing.expectError(error.JetQueryUnsafeDelete, query.validateDelete());
}

test "deleteAll" {
    const Schema = struct {
        pub const Cat = Table("cats", struct { name: []const u8, paws: i32 }, .{});
    };
    const query = Query(Schema, .Cat)
        .deleteAll();

    try std.testing.expectEqualStrings(
        \\DELETE FROM "cats"
    , query.sql);
}

test "find" {
    const Schema = struct {
        pub const Cat = Table("cats", struct { id: i32, name: []const u8, paws: i32 }, .{});
    };
    const query = Query(Schema, .Cat).find(1000);

    try std.testing.expectEqualStrings(
        \\SELECT "cats"."id", "cats"."name", "cats"."paws" FROM "cats" WHERE "cats"."id" = $1 LIMIT $2
    , query.sql);
}

test "find (with coerced id)" {
    const Schema = struct {
        pub const Cat = Table("cats", struct { id: i32, name: []const u8, paws: i32 }, .{});
    };
    const query = Query(Schema, .Cat).find("1000");

    try std.testing.expectEqualStrings(
        \\SELECT "cats"."id", "cats"."name", "cats"."paws" FROM "cats" WHERE "cats"."id" = $1 LIMIT $2
    , query.sql);
}

test "findBy" {
    const Schema = struct {
        pub const Cat = Table("cats", struct { id: i32, name: []const u8, paws: i32 }, .{});
    };
    const query = Query(Schema, .Cat).findBy(.{ .name = "Hercules", .paws = 4 });

    try std.testing.expectEqualStrings(
        \\SELECT "cats"."id", "cats"."name", "cats"."paws" FROM "cats" WHERE "cats"."name" = $1 AND "cats"."paws" = $2 LIMIT $3
    , query.sql);
}

test "count()" {
    const Schema = struct {
        pub const Cat = Table("cats", struct { id: i32, name: []const u8, paws: i32 }, .{});
    };
    const query = Query(Schema, .Cat).where(.{ .name = "Hercules", .paws = 4 }).count();

    try std.testing.expectEqualStrings(
        \\SELECT COUNT(*) FROM "cats" WHERE "cats"."name" = $1 AND "cats"."paws" = $2
    , query.sql);
}

test "distinct().count()" {
    const Schema = struct {
        pub const Cat = Table("cats", struct { id: i32, name: []const u8, paws: i32 }, .{});
    };
    const query = Query(Schema, .Cat).where(.{ .name = "Hercules", .paws = 4 }).distinct(.{.name}).count();

    try std.testing.expectEqualStrings(
        \\SELECT COUNT(DISTINCT("cats"."name")) FROM "cats" WHERE "cats"."name" = $1 AND "cats"."paws" = $2
    , query.sql);
}

test "nested distinct().count()" {
    const Schema = struct {
        pub const Cat = Table("cats", struct { id: i32, name: []const u8, paws: i32 }, .{});
        pub const Human = Table(
            "humans",
            struct { cat_id: i32, name: []const u8 },
            .{
                .relations = .{ .cat = relation.belongsTo(.Cat, .{}) },
            },
        );
    };
    const query = Query(Schema, .Human).include(.cat, &.{}).where(.{ .name = "Bob" }).distinct(.{ .name, .{ .cat = .{.name} } }).count();

    try std.testing.expectEqualStrings(
        \\SELECT COUNT(DISTINCT("humans"."name", "cats"."name")) FROM "humans" INNER JOIN "cats" ON "humans"."cat_id" = "cats"."id" WHERE "humans"."name" = $1
    , query.sql);
}

test "combined" {
    const Schema = struct {
        pub const Cat = Table("cats", struct { id: i32, name: []const u8, paws: i32 }, .{});
    };
    const query = Query(Schema, .Cat)
        .select(&.{ .name, .paws })
        .where(.{ .name = "Hercules" })
        .limit(10)
        .orderBy(.{ .name = .ascending });

    try std.testing.expectEqualStrings(
        \\SELECT "cats"."name", "cats"."paws" FROM "cats" WHERE "cats"."name" = $1 ORDER BY "cats"."name" ASC LIMIT $2
    , query.sql);
}

test "runtime field values" {
    const Schema = struct {
        pub const Cat = Table("cats", struct { name: []const u8, paws: i32 }, .{});
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
        pub const Cat = Table("cats", struct { name: []const u8, intelligent: bool }, .{});
    };
    const query = Query(Schema, .Cat)
        .select(&.{.name})
        .where(.{ .intelligent = "1" });

    try std.testing.expectEqual(query.values().@"0", true);
}

test "integer coercion" {
    const Schema = struct {
        pub const Cat = Table("cats", struct { name: []const u8, paws: i32 }, .{});
    };
    const query = Query(Schema, .Cat)
        .select(&.{.name})
        .where(.{ .paws = "4" });

    try std.testing.expectEqual(query.values().@"0", 4);
}

test "float coercion" {
    const Schema = struct {
        pub const Cat = Table("cats", struct { name: []const u8, intelligence: f64 }, .{});
    };
    const query = Query(Schema, .Cat)
        .select(&.{.name})
        .where(.{ .intelligence = "10.2" });

    try std.testing.expectEqual(query.values().@"0", 10.2);
}

test "toJetQuery()" {
    const Schema = struct {
        pub const Cat = Table("cats", struct { name: []const u8, paws: i32 }, .{});
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

    const name = Name{};
    const paws = Paws{};

    const query = Query(Schema, .Cat)
        .insert(.{ .name = name, .paws = &paws });
    try std.testing.expectEqualStrings(
        \\INSERT INTO "cats" ("name", "paws") VALUES ($1, $2)
    , query.sql);
    const values = query.values();
    try std.testing.expectEqualStrings(values.@"0", "Hercules");
    try std.testing.expectEqual(values.@"1", 4);
}

test "failed coercion (bool)" {
    const Schema = struct {
        pub const Cat = Table("cats", struct { name: []const u8, intelligent: bool }, .{});
    };
    const query = Query(Schema, .Cat)
        .select(&.{.name})
        .where(.{ .intelligent = "not a bool" });

    try std.testing.expectError(error.JetQueryInvalidBooleanString, query.validateValues());
}

test "failed coercion (int)" {
    const Schema = struct {
        pub const Cat = Table("cats", struct { name: []const u8, paws: i32 }, .{});
    };
    const query = Query(Schema, .Cat)
        .select(&.{.name})
        .where(.{ .paws = "not an int" });

    try std.testing.expectError(error.JetQueryInvalidIntegerString, query.validateValues());
}

test "failed coercion (float)" {
    const Schema = struct {
        pub const Cat = Table("cats", struct { name: []const u8, intelligence: f64 }, .{});
    };
    const query = Query(Schema, .Cat)
        .select(&.{.name})
        .where(.{ .intelligence = "not a float" });

    try std.testing.expectError(error.JetQueryInvalidFloatString, query.validateValues());
}

test "timestamps (create)" {
    const Schema = struct {
        pub const Cat = Table(
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
            "cats",
            struct { name: []const u8, paws: i32, created_at: i64, updated_at: i64 },
            .{},
        );
    };
    const query = Query(Schema, .Cat)
        .update(.{ .name = "Heracles", .paws = 2 })
        .where(.{ .name = "Hercules" });

    try std.testing.expectEqualStrings(
        \\UPDATE "cats" SET "cats"."name" = $1, "cats"."paws" = $2, "cats"."updated_at" = $3 WHERE "cats"."name" = $4
    ,
        query.sql,
    );
}

test "belongsTo" {
    const Schema = struct {
        pub const Human = Table(
            "humans",
            struct { id: i32, cat_id: i32, name: []const u8 },
            .{ .relations = .{ .cat = relation.belongsTo(.Cat, .{}) } },
        );

        pub const Cat = Table(
            "cats",
            struct { id: i32, name: []const u8, paws: i32, created_at: i64, updated_at: i64 },
            .{ .relations = .{} },
        );
    };
    const query = Query(Schema, .Human)
        .include(.cat, &.{})
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
            "cats",
            struct { id: i32, name: []const u8, paws: i32, created_at: i64, updated_at: i64 },
            .{},
        );

        pub const Family = Table(
            "families",
            struct { id: i32, name: []const u8 },
            .{},
        );
    };
    const query = Query(Schema, .Human)
        .include(.cat, &.{})
        .include(.family, &.{})
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
            "cats",
            struct { id: i32, name: []const u8, paws: i32, created_at: i64, updated_at: i64 },
            .{},
        );

        pub const Family = Table(
            "families",
            struct { id: i32, name: []const u8 },
            .{},
        );
    };
    const query = Query(Schema, .Human)
        .include(.cat, &.{ .name, .paws })
        .include(.family, &.{.name})
        .select(&.{.name})
        .findBy(.{ .name = "Bob" });

    try std.testing.expectEqualStrings(
        \\SELECT "humans"."name", "cats"."name", "cats"."paws", "families"."name" FROM "humans" INNER JOIN "cats" ON "humans"."cat_id" = "cats"."id" INNER JOIN "families" ON "humans"."family_id" = "families"."id" WHERE "humans"."name" = $1 LIMIT $2
    ,
        query.sql,
    );
}
