const std = @import("std");

pub const jetcommon = @import("jetcommon");

pub const config = @import("jetquery.config");

pub const Repo = @import("jetquery/Repo.zig").Repo;

pub const Connection = @import("jetquery/Connection.zig").Connection;
pub const adapters = @import("jetquery/adapters.zig");
pub const sql = @import("jetquery/sql.zig");
pub const relation = @import("jetquery/relation.zig");
pub const Migration = @import("jetquery/Migration.zig");
pub const schema = @import("jetquery/schema.zig");
pub const fields = @import("jetquery/fields.zig");
pub const columns = @import("jetquery/columns.zig");
pub const default_column_names = @import("jetquery/default_column_names.zig");
pub const Row = @import("jetquery/Row.zig");
pub const Result = @import("jetquery/Result.zig").Result;
pub const events = @import("jetquery/events.zig");
pub const Query = @import("jetquery/Query.zig").Query;
pub const Model = @import("jetquery/Model.zig").Model;
pub const Value = @import("jetquery/Value.zig").Value;
pub const DateTime = jetcommon.types.DateTime;
pub const debug = @import("jetquery/debug.zig");
pub const Reflection = @import("jetquery/reflection/Reflection.zig");
pub const util = @import("jetquery/util.zig");
pub const Environment = std.meta.FieldEnum(@TypeOf(config.database));
pub const hasMany = relation.hasMany;
pub const belongsTo = relation.belongsTo;
pub const CallbackFn = *const fn (event: events.Event) anyerror!void;

pub const CreateTableOptions = struct { if_not_exists: bool = false };
pub const DropTableOptions = struct { if_exists: bool = false };
pub const AlterTableOptions = struct {
    columns: AlterTableColumnOptions = .{},
    rename: ?[]const u8 = null,

    pub const AlterTableColumnOptions = struct {
        add: []const schema.Column = &.{},
        drop: []const []const u8 = &.{},
        rename: ?RenameColumn = null,

        pub const RenameColumn = struct { from: []const u8, to: []const u8 };
    };
};
pub const DropDatabaseOptions = struct { if_exists: bool = false };
pub const CreateIndexOptions = struct {
    unique: bool = false,
    name: ?[]const u8 = null,
    if_not_exists: bool = false,
};
pub const Context = enum { migration, query, cli };

pub const original_prefix = "__original_";

const TestAdapter: adapters.Name = .postgresql;

test {
    std.testing.refAllDecls(@This());
}

test "select" {
    const Schema = struct {
        pub const Cat = Model(@This(), "cats", struct { name: []const u8, paws: i32 }, .{});
    };
    const query = Query(TestAdapter, Schema, .Cat).select(.{ .name, .paws });

    try std.testing.expectEqualStrings(
        \\SELECT "cats"."name", "cats"."paws" FROM "cats" WHERE (1 = 1)
    , query.sql);
    try std.testing.expect(query.isValid());
}

test "select (all)" {
    const Schema = struct {
        pub const Cat = Model(@This(), "cats", struct { name: []const u8, paws: i32 }, .{});
    };
    const query = Query(TestAdapter, Schema, .Cat).select(.{});

    try std.testing.expectEqualStrings(
        \\SELECT "cats"."name", "cats"."paws" FROM "cats" WHERE (1 = 1)
    , query.sql);
    try std.testing.expect(query.isValid());
}

test "select (with `where`)" {
    const Schema = struct {
        pub const Cat = Model(@This(), "cats", struct { name: []const u8, paws: i32 }, .{});
    };

    const paws = 4;
    const query = Query(TestAdapter, Schema, .Cat)
        .select(.{ .name, .paws })
        .where(.{ .name = "bar", .paws = paws });

    try std.testing.expectEqualStrings(
        \\SELECT "cats"."name", "cats"."paws" FROM "cats" WHERE ("cats"."name" = $1 AND "cats"."paws" = $2)
    , query.sql);
    try std.testing.expect(query.isValid());
}

test "where" {
    const Schema = struct {
        pub const Cat = Model(@This(), "cats", struct { name: []const u8, paws: i32, color: []const u8 }, .{});
    };

    const paws = 4;
    const query = Query(TestAdapter, Schema, .Cat)
        .where(.{ .name = "bar", .paws = paws });

    try std.testing.expectEqualStrings(
        \\SELECT "cats"."name", "cats"."paws", "cats"."color" FROM "cats" WHERE ("cats"."name" = $1 AND "cats"."paws" = $2)
    , query.sql);
    try std.testing.expect(query.isValid());
}

test "where (multiple)" {
    const Schema = struct {
        pub const Cat = Model(@This(), "cats", struct { name: []const u8, paws: i32 }, .{});
    };

    const query = Query(TestAdapter, Schema, .Cat)
        .select(.{ .name, .paws })
        .where(.{ .name = "bar" })
        .where(.{ .paws = 4 });

    try std.testing.expectEqualStrings(
        \\SELECT "cats"."name", "cats"."paws" FROM "cats" WHERE "cats"."name" = $1 AND "cats"."paws" = $2
    , query.sql);
    try std.testing.expect(query.isValid());
}

test "limit" {
    const Schema = struct {
        pub const Cat = Model(@This(), "cats", struct { name: []const u8, paws: i32 }, .{});
    };
    const query = Query(TestAdapter, Schema, .Cat)
        .select(.{ .name, .paws })
        .limit(100);

    try std.testing.expectEqualStrings(
        \\SELECT "cats"."name", "cats"."paws" FROM "cats" WHERE (1 = 1) LIMIT $1
    , query.sql);
    try std.testing.expect(query.isValid());
}

test "offset" {
    const Schema = struct {
        pub const Cat = Model(@This(), "cats", struct { name: []const u8, paws: i32 }, .{});
    };
    const query = Query(TestAdapter, Schema, .Cat)
        .select(.{ .name, .paws })
        .limit(100)
        .offset(50);

    try std.testing.expectEqualStrings(
        \\SELECT "cats"."name", "cats"."paws" FROM "cats" WHERE (1 = 1) LIMIT $1 OFFSET $2
    , query.sql);
    try std.testing.expect(query.isValid());
}

test "order by" {
    const Schema = struct {
        pub const Cat = Model(@This(), "cats", struct { name: []const u8, paws: i32 }, .{});
    };
    const query = Query(TestAdapter, Schema, .Cat)
        .select(.{ .name, .paws })
        .orderBy(.{ .name = .ascending });

    try std.testing.expectEqualStrings(
        \\SELECT "cats"."name", "cats"."paws" FROM "cats" WHERE (1 = 1) ORDER BY "cats"."name" ASC
    , query.sql);
    try std.testing.expect(query.isValid());
}

test "order by (aliased desc = descending)" {
    const Schema = struct {
        pub const Cat = Model(@This(), "cats", struct { name: []const u8, paws: i32 }, .{});
    };
    const query = Query(TestAdapter, Schema, .Cat)
        .select(.{ .name, .paws })
        .orderBy(.{ .name = .desc });

    try std.testing.expectEqualStrings(
        \\SELECT "cats"."name", "cats"."paws" FROM "cats" WHERE (1 = 1) ORDER BY "cats"."name" DESC
    , query.sql);
    try std.testing.expect(query.isValid());
}

test "order by (aliased asc = ascending)" {
    const Schema = struct {
        pub const Cat = Model(@This(), "cats", struct { name: []const u8, paws: i32 }, .{});
    };
    const query = Query(TestAdapter, Schema, .Cat)
        .select(.{ .name, .paws })
        .orderBy(.{ .name = .asc });

    try std.testing.expectEqualStrings(
        \\SELECT "cats"."name", "cats"."paws" FROM "cats" WHERE (1 = 1) ORDER BY "cats"."name" ASC
    , query.sql);
    try std.testing.expect(query.isValid());
}

test "order by (short-hand)" {
    const Schema = struct {
        pub const Cat = Model(@This(), "cats", struct { name: []const u8, paws: i32 }, .{});
    };
    const query = Query(TestAdapter, Schema, .Cat)
        .select(.{ .name, .paws })
        .orderBy(.name);

    try std.testing.expectEqualStrings(
        \\SELECT "cats"."name", "cats"."paws" FROM "cats" WHERE (1 = 1) ORDER BY "cats"."name" ASC
    , query.sql);
    try std.testing.expect(query.isValid());
}

test "order by with relations (short-hand)" {
    const Schema = struct {
        pub const Human = Model(
            @This(),
            "humans",
            struct { id: i32, cat_id: i32, name: []const u8 },
            .{ .relations = .{ .cat = relation.belongsTo(.Cat, .{}) } },
        );

        pub const Cat = Model(
            @This(),
            "cats",
            struct { id: i32, name: []const u8, paws: i32 },
            .{},
        );
    };

    const query = Query(TestAdapter, Schema, .Human)
        .join(.inner, .cat)
        .orderBy(.{ .cat = .name });

    try std.testing.expectEqualStrings(
        \\SELECT "humans"."id", "humans"."cat_id", "humans"."name" FROM "humans" INNER JOIN "cats" ON "humans"."cat_id" = "cats"."id" WHERE (1 = 1) ORDER BY "cats"."name" ASC
    ,
        query.sql,
    );
    try std.testing.expect(query.isValid());
}

test "order by with relations (explicit form)" {
    const Schema = struct {
        pub const Human = Model(
            @This(),
            "humans",
            struct { id: i32, cat_id: i32, name: []const u8 },
            .{ .relations = .{ .cat = relation.belongsTo(.Cat, .{}) } },
        );

        pub const Cat = Model(
            @This(),
            "cats",
            struct { id: i32, name: []const u8, paws: i32 },
            .{},
        );
    };

    const query = Query(TestAdapter, Schema, .Human)
        .join(.inner, .cat)
        .orderBy(.{ .cat = .{ .name = .descending } });

    try std.testing.expectEqualStrings(
        \\SELECT "humans"."id", "humans"."cat_id", "humans"."name" FROM "humans" INNER JOIN "cats" ON "humans"."cat_id" = "cats"."id" WHERE (1 = 1) ORDER BY "cats"."name" DESC
    ,
        query.sql,
    );
    try std.testing.expect(query.isValid());
}

test "order by with relations and base table, short + explicit forms" {
    const Schema = struct {
        pub const Human = Model(
            @This(),
            "humans",
            struct { id: i32, cat_id: i32, family_id: i32, name: []const u8 },
            .{
                .relations = .{
                    .cat = relation.belongsTo(.Cat, .{}),
                    .family = relation.belongsTo(.Family, .{}),
                },
            },
        );

        pub const Cat = Model(
            @This(),
            "cats",
            struct { id: i32, name: []const u8, paws: i32 },
            .{},
        );

        pub const Family = Model(
            @This(),
            "families",
            struct { id: i32, name: []const u8 },
            .{},
        );
    };

    const query = Query(TestAdapter, Schema, .Human)
        .join(.inner, .cat)
        .join(.inner, .family)
        .orderBy(.{
        .id = .descending,
        .cat = .{ .name = .descending },
        .family = .{ .id, .name },
    });

    try std.testing.expectEqualStrings(
        \\SELECT "humans"."id", "humans"."cat_id", "humans"."family_id", "humans"."name" FROM "humans" INNER JOIN "cats" ON "humans"."cat_id" = "cats"."id" INNER JOIN "families" ON "humans"."family_id" = "families"."id" WHERE (1 = 1) ORDER BY "humans"."id" DESC, "cats"."name" DESC, "families"."id" ASC, "families"."name" ASC
    ,
        query.sql,
    );
    try std.testing.expect(query.isValid());
}

test "insert" {
    const Schema = struct {
        pub const Cat = Model(@This(), "cats", struct { name: []const u8, paws: i32 }, .{});
    };
    const query = Query(TestAdapter, Schema, .Cat)
        .insert(.{ .name = "Hercules", .paws = 4 });

    try std.testing.expectEqualStrings(
        \\INSERT INTO "cats" ("name", "paws") VALUES ($1, $2)
    , query.sql);
    try std.testing.expect(query.isValid());
}

test "update" {
    const Schema = struct {
        pub const Cat = Model(@This(), "cats", struct { name: []const u8, paws: i32 }, .{});
    };
    const query = Query(TestAdapter, Schema, .Cat)
        .update(.{ .name = "Heracles", .paws = 2 })
        .where(.{ .name = "Hercules" });

    try std.testing.expectEqualStrings(
        \\UPDATE "cats" SET "name" = $1, "paws" = $2 WHERE "cats"."name" = $3
    ,
        query.sql,
    );
    try std.testing.expect(query.isValid());
}

test "update (without whereclause)" {
    const Schema = struct {
        pub const Cat = Model(@This(), "cats", struct { name: []const u8, paws: i32 }, .{});
    };
    const query = Query(TestAdapter, Schema, .Cat)
        .update(.{ .name = "Heracles", .paws = 2 });

    try std.testing.expectEqualStrings(
        \\UPDATE "cats" SET "name" = $1, "paws" = $2 WHERE (1 = 1)
    ,
        query.sql,
    );
    try std.testing.expectEqual(false, query.isValid());
    try std.testing.expectError(error.JetQueryUnsafeUpdate, query.validate());
}

test "updateAll" {
    const Schema = struct {
        pub const Cat = Model(@This(), "cats", struct { name: []const u8, paws: i32 }, .{});
    };
    const query = Query(TestAdapter, Schema, .Cat)
        .updateAll(.{ .name = "Heracles", .paws = 2 });

    try std.testing.expectEqualStrings(
        \\UPDATE "cats" SET "name" = $1, "paws" = $2 WHERE (1 = 1)
    ,
        query.sql,
    );
    try std.testing.expect(query.isValid());
}

test "delete" {
    const Schema = struct {
        pub const Cat = Model(@This(), "cats", struct { name: []const u8, paws: i32 }, .{});
    };
    const query = Query(TestAdapter, Schema, .Cat)
        .delete()
        .where(.{ .name = "Hercules" });

    try std.testing.expectEqualStrings(
        \\DELETE FROM "cats" WHERE "cats"."name" = $1
    , query.sql);
    try std.testing.expect(query.isValid());
}

test "delete (without where clause)" {
    const Schema = struct {
        pub const Cat = Model(@This(), "cats", struct { name: []const u8, paws: i32 }, .{});
    };
    const query = Query(TestAdapter, Schema, .Cat).delete();

    try std.testing.expectEqual(false, query.isValid());
    try std.testing.expectError(error.JetQueryUnsafeDelete, query.validate());
}

test "deleteAll" {
    const Schema = struct {
        pub const Cat = Model(@This(), "cats", struct { name: []const u8, paws: i32 }, .{});
    };
    const query = Query(TestAdapter, Schema, .Cat)
        .deleteAll();

    try std.testing.expectEqualStrings(
        \\DELETE FROM "cats" WHERE (1 = 1)
    , query.sql);
    try std.testing.expect(query.isValid());
}

test "find" {
    const Schema = struct {
        pub const Cat = Model(@This(), "cats", struct { id: i32, name: []const u8, paws: i32 }, .{});
    };
    const query = Query(TestAdapter, Schema, .Cat).find(1000);

    try std.testing.expectEqualStrings(
        \\SELECT "cats"."id", "cats"."name", "cats"."paws" FROM "cats" WHERE "cats"."id" = $1 ORDER BY "cats"."id" ASC LIMIT $2
    , query.sql);
    try std.testing.expect(query.isValid());
}

test "find (with coerced id)" {
    const Schema = struct {
        pub const Cat = Model(@This(), "cats", struct { id: i32, name: []const u8, paws: i32 }, .{});
    };
    const query = Query(TestAdapter, Schema, .Cat).find("1000");

    try std.testing.expectEqualStrings(
        \\SELECT "cats"."id", "cats"."name", "cats"."paws" FROM "cats" WHERE "cats"."id" = $1 ORDER BY "cats"."id" ASC LIMIT $2
    , query.sql);
    try std.testing.expect(query.isValid());
}

test "findBy" {
    const Schema = struct {
        pub const Cat = Model(@This(), "cats", struct { id: i32, name: []const u8, paws: i32 }, .{});
    };
    const query = Query(TestAdapter, Schema, .Cat).findBy(.{ .name = "Hercules", .paws = 4 });

    try std.testing.expectEqualStrings(
        \\SELECT "cats"."id", "cats"."name", "cats"."paws" FROM "cats" WHERE ("cats"."name" = $1 AND "cats"."paws" = $2) ORDER BY "cats"."id" ASC LIMIT $3
    , query.sql);
    try std.testing.expect(query.isValid());
}

test "count()" {
    const Schema = struct {
        pub const Cat = Model(@This(), "cats", struct { id: i32, name: []const u8, paws: i32 }, .{});
    };
    const query = Query(TestAdapter, Schema, .Cat).where(.{ .name = "Hercules", .paws = 4 }).count();

    try std.testing.expectEqualStrings(
        \\SELECT COUNT(*) FROM "cats" WHERE ("cats"."name" = $1 AND "cats"."paws" = $2)
    , query.sql);
    try std.testing.expect(query.isValid());
}

test "count() without whereclause" {
    const Schema = struct {
        pub const Cat = Model(@This(), "cats", struct { id: i32, name: []const u8, paws: i32 }, .{});
    };
    const query = Query(TestAdapter, Schema, .Cat).count();

    try std.testing.expectEqualStrings(
        \\SELECT COUNT(*) FROM "cats" WHERE (1 = 1)
    , query.sql);
    try std.testing.expect(query.isValid());
}

test "distinct().count()" {
    const Schema = struct {
        pub const Cat = Model(@This(), "cats", struct { id: i32, name: []const u8, paws: i32 }, .{});
    };
    const query = Query(TestAdapter, Schema, .Cat).where(.{ .name = "Hercules", .paws = 4 }).distinct(.{.name}).count();

    try std.testing.expectEqualStrings(
        \\SELECT COUNT(DISTINCT("cats"."name")) FROM "cats" WHERE ("cats"."name" = $1 AND "cats"."paws" = $2)
    , query.sql);
    try std.testing.expect(query.isValid());
}

test "nested distinct().count()" {
    const Schema = struct {
        pub const Cat = Model(@This(), "cats", struct { id: i32, name: []const u8, paws: i32 }, .{});
        pub const Human = Model(
            @This(),
            "humans",
            struct { cat_id: i32, name: []const u8 },
            .{
                .relations = .{ .cat = relation.belongsTo(.Cat, .{}) },
            },
        );
    };
    const query = Query(TestAdapter, Schema, .Human).include(.cat, .{}).where(.{ .name = "Bob" }).distinct(.{ .name, .{ .cat = .{.name} } }).count();

    try std.testing.expectEqualStrings(
        \\SELECT COUNT(DISTINCT("humans"."name", "cats"."name")) FROM "humans" INNER JOIN "cats" ON "humans"."cat_id" = "cats"."id" WHERE "humans"."name" = $1
    , query.sql);
    try std.testing.expect(query.isValid());
}

test "combined" {
    const Schema = struct {
        pub const Cat = Model(@This(), "cats", struct { id: i32, name: []const u8, paws: i32 }, .{});
    };
    const query = Query(TestAdapter, Schema, .Cat)
        .select(.{ .name, .paws })
        .where(.{ .name = "Hercules" })
        .limit(10)
        .orderBy(.{ .name = .ascending });

    try std.testing.expectEqualStrings(
        \\SELECT "cats"."name", "cats"."paws" FROM "cats" WHERE "cats"."name" = $1 ORDER BY "cats"."name" ASC LIMIT $2
    , query.sql);
    try std.testing.expect(query.isValid());
}

test "runtime field values" {
    const Schema = struct {
        pub const Cat = Model(@This(), "cats", struct { name: []const u8, paws: i32 }, .{});
    };
    var hercules_buf: [8]u8 = undefined;
    const hercules = try std.fmt.bufPrint(&hercules_buf, "{s}", .{"Hercules"});
    const paws: u16 = std.crypto.random.int(u8);
    var heracles_buf: [8]u8 = undefined;
    const heracles = try std.fmt.bufPrint(&heracles_buf, "{s}", .{"Heracles"});
    const query = Query(TestAdapter, Schema, .Cat)
        .update(.{ .name = heracles, .paws = paws + 2 })
        .where(.{ .name = hercules, .paws = paws });
    try std.testing.expectEqualStrings("Heracles", query.values.@"0");
    try std.testing.expectEqual(paws + 2, query.values.@"1");
    try std.testing.expectEqualStrings("Hercules", query.values.@"2");
    try std.testing.expect(query.isValid());
}

test "boolean coercion" {
    const Schema = struct {
        pub const Cat = Model(@This(), "cats", struct { name: []const u8, intelligent: bool }, .{});
    };
    const query = Query(TestAdapter, Schema, .Cat)
        .select(.{.name})
        .where(.{ .intelligent = "1" });

    try std.testing.expectEqual(query.values.@"0", true);
    try std.testing.expect(query.isValid());
}

test "integer coercion" {
    const Schema = struct {
        pub const Cat = Model(@This(), "cats", struct { name: []const u8, paws: i32 }, .{});
    };
    const query = Query(TestAdapter, Schema, .Cat)
        .select(.{.name})
        .where(.{ .paws = "4" });

    try std.testing.expectEqual(query.values.@"0", 4);
    try std.testing.expect(query.isValid());
}

test "float coercion" {
    const Schema = struct {
        pub const Cat = Model(@This(), "cats", struct { name: []const u8, intelligence: f64 }, .{});
    };
    const query = Query(TestAdapter, Schema, .Cat)
        .select(.{.name})
        .where(.{ .intelligence = "10.2" });

    try std.testing.expectEqual(query.values.@"0", 10.2);
    try std.testing.expect(query.isValid());
}

test "toJetQuery()" {
    const Schema = struct {
        pub const Cat = Model(@This(), "cats", struct { name: []const u8, paws: i32, color: []const u8 }, .{});
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

    const query = Query(TestAdapter, Schema, .Cat)
        .insert(.{ .name = name, .paws = &paws, .color = color });
    try std.testing.expectEqualStrings(
        \\INSERT INTO "cats" ("name", "paws", "color") VALUES ($1, $2, $3)
    , query.sql);
    try std.testing.expectEqualStrings(query.values.@"0", "Hercules");
    try std.testing.expectEqual(query.values.@"1", 4);
    try std.testing.expect(query.isValid());
}

test "failed coercion (bool)" {
    const Schema = struct {
        pub const Cat = Model(@This(), "cats", struct { name: []const u8, intelligent: bool }, .{});
    };
    const query = Query(TestAdapter, Schema, .Cat)
        .select(.{.name})
        .where(.{ .intelligent = "not a bool" });

    try std.testing.expectError(error.JetQueryInvalidBooleanString, query.validateValues());
    try std.testing.expectEqual(false, query.isValid());
}

test "failed coercion (int)" {
    const Schema = struct {
        pub const Cat = Model(@This(), "cats", struct { name: []const u8, paws: i32 }, .{});
    };
    const query = Query(TestAdapter, Schema, .Cat)
        .select(.{.name})
        .where(.{ .paws = "not an int" });

    try std.testing.expectError(error.JetQueryInvalidIntegerString, query.validateValues());
    try std.testing.expectEqual(false, query.isValid());
}

test "failed coercion (float)" {
    const Schema = struct {
        pub const Cat = Model(@This(), "cats", struct { name: []const u8, intelligence: f64 }, .{});
    };
    const query = Query(TestAdapter, Schema, .Cat)
        .select(.{.name})
        .where(.{ .intelligence = "not a float" });

    try std.testing.expectError(error.JetQueryInvalidFloatString, query.validateValues());
    try std.testing.expectEqual(false, query.isValid());
}

test "timestamps (create)" {
    const Schema = struct {
        pub const Cat = Model(
            @This(),
            "cats",
            struct { name: []const u8, paws: i32, created_at: i64, updated_at: i64 },
            .{},
        );
    };
    const query = Query(TestAdapter, Schema, .Cat)
        .insert(.{ .name = "Hercules", .paws = 4 });

    try std.testing.expectEqualStrings(
        \\INSERT INTO "cats" ("name", "paws", "created_at", "updated_at") VALUES ($1, $2, $3, $4)
    , query.sql);
    try std.testing.expect(query.isValid());
}

test "timestamps (update)" {
    const Schema = struct {
        pub const Cat = Model(
            @This(),
            "cats",
            struct { name: []const u8, paws: i32, created_at: i64, updated_at: i64 },
            .{},
        );
    };
    const query = Query(TestAdapter, Schema, .Cat)
        .update(.{ .name = "Heracles", .paws = 2 })
        .where(.{ .name = "Hercules" });

    try std.testing.expectEqualStrings(
        \\UPDATE "cats" SET "name" = $1, "paws" = $2, "updated_at" = $3 WHERE "cats"."name" = $4
    ,
        query.sql,
    );
    try std.testing.expect(query.isValid());
}

test "belongsTo" {
    const Schema = struct {
        pub const Human = Model(
            @This(),
            "humans",
            struct { id: i32, cat_id: i32, name: []const u8 },
            .{ .relations = .{ .cat = relation.belongsTo(.Cat, .{}) } },
        );

        pub const Cat = Model(
            @This(),
            "cats",
            struct { id: i32, name: []const u8, paws: i32, created_at: i64, updated_at: i64 },
            .{ .relations = .{} },
        );
    };
    const query = Query(TestAdapter, Schema, .Human)
        .include(.cat, .{})
        .findBy(.{ .name = "Bob" });

    try std.testing.expectEqualStrings(
        \\SELECT "humans"."id", "humans"."cat_id", "humans"."name", "cats"."id", "cats"."name", "cats"."paws", "cats"."created_at", "cats"."updated_at" FROM "humans" INNER JOIN "cats" ON "humans"."cat_id" = "cats"."id" WHERE "humans"."name" = $1 ORDER BY "humans"."id" ASC LIMIT $2
    ,
        query.sql,
    );
    try std.testing.expect(query.isValid());
}

test "belongsTo (multiple)" {
    const Schema = struct {
        pub const Human = Model(
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

        pub const Cat = Model(
            @This(),
            "cats",
            struct { id: i32, name: []const u8, paws: i32, created_at: i64, updated_at: i64 },
            .{},
        );

        pub const Family = Model(
            @This(),
            "families",
            struct { id: i32, name: []const u8 },
            .{},
        );
    };
    const query = Query(TestAdapter, Schema, .Human)
        .include(.cat, .{})
        .include(.family, .{})
        .findBy(.{ .name = "Bob" });

    try std.testing.expectEqualStrings(
        \\SELECT "humans"."id", "humans"."family_id", "humans"."cat_id", "humans"."name", "cats"."id", "cats"."name", "cats"."paws", "cats"."created_at", "cats"."updated_at", "families"."id", "families"."name" FROM "humans" INNER JOIN "cats" ON "humans"."cat_id" = "cats"."id" INNER JOIN "families" ON "humans"."family_id" = "families"."id" WHERE "humans"."name" = $1 ORDER BY "humans"."id" ASC LIMIT $2
    ,
        query.sql,
    );
    try std.testing.expect(query.isValid());
}

test "belongsTo (with specified columns)" {
    const Schema = struct {
        pub const Human = Model(
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

        pub const Cat = Model(
            @This(),
            "cats",
            struct { id: i32, name: []const u8, paws: i32, created_at: i64, updated_at: i64 },
            .{},
        );

        pub const Family = Model(
            @This(),
            "families",
            struct { id: i32, name: []const u8 },
            .{},
        );
    };
    const query = Query(TestAdapter, Schema, .Human)
        .include(.cat, .{ .select = .{ .name, .paws } })
        .include(.family, .{ .select = .{.name} })
        .select(.{.name})
        .findBy(.{ .name = "Bob" });

    try std.testing.expectEqualStrings(
        \\SELECT "humans"."name", "cats"."name", "cats"."paws", "families"."name" FROM "humans" INNER JOIN "cats" ON "humans"."cat_id" = "cats"."id" INNER JOIN "families" ON "humans"."family_id" = "families"."id" WHERE "humans"."name" = $1 ORDER BY "humans"."id" ASC LIMIT $2
    ,
        query.sql,
    );
    try std.testing.expect(query.isValid());
}

test "hasMany" {
    const Schema = struct {
        pub const Human = Model(
            @This(),
            "humans",
            struct { id: i32, cat_id: i32, name: []const u8 },
            .{ .relations = .{ .cat = relation.belongsTo(.Cat, .{}) } },
        );

        pub const Cat = Model(
            @This(),
            "cats",
            struct { id: i32, name: []const u8, paws: i32, created_at: i64, updated_at: i64 },
            .{ .relations = .{ .humans = relation.hasMany(.Human, .{}) } },
        );
    };
    const query = Query(TestAdapter, Schema, .Cat)
        .include(.humans, .{})
        .findBy(.{ .name = "Hercules" });

    try std.testing.expectEqualStrings(
        \\SELECT "cats"."id", "cats"."name", "cats"."paws", "cats"."created_at", "cats"."updated_at" FROM "cats" WHERE "cats"."name" = $1 ORDER BY "cats"."id" ASC LIMIT $2
    ,
        query.sql,
    );

    try std.testing.expectEqualStrings(
        \\SELECT "humans"."id", "humans"."cat_id", "humans"."name" FROM "humans" WHERE "humans"."cat_id" = $1 ORDER BY "humans"."id" ASC
    ,
    // Only the base query is generated at this point, the repo appends the where clause
    // after fetching results of the first query. This is tested more thoroughly in `Repo.zig`
        query.auxiliary_queries[0].query.where(.{ .cat_id = 1 }).sql,
    );
    try std.testing.expect(query.isValid());
}

test "nested where" {
    const Schema = struct {
        pub const Human = Model(
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

        pub const Cat = Model(
            @This(),
            "cats",
            struct { id: i32, name: []const u8, paws: i32, created_at: i64, updated_at: i64 },
            .{},
        );

        pub const Family = Model(
            @This(),
            "families",
            struct { id: i32, name: []const u8 },
            .{},
        );
    };
    const query = Query(TestAdapter, Schema, .Human)
        .include(.cat, .{})
        .include(.family, .{})
        .where(.{
        .name = "Bob",
        .cat = .{ .name = "Hercules" },
        .family = .{ .name = "Farrell" },
    });

    try std.testing.expectEqualStrings(
        \\SELECT "humans"."id", "humans"."family_id", "humans"."cat_id", "humans"."name", "cats"."id", "cats"."name", "cats"."paws", "cats"."created_at", "cats"."updated_at", "families"."id", "families"."name" FROM "humans" INNER JOIN "cats" ON "humans"."cat_id" = "cats"."id" INNER JOIN "families" ON "humans"."family_id" = "families"."id" WHERE ("humans"."name" = $1 AND "cats"."name" = $2 AND "families"."name" = $3) ORDER BY "humans"."id" ASC
    ,
        query.sql,
    );
    try std.testing.expect(query.isValid());
}

test "operator logic" {
    const Schema = struct {
        pub const Human = Model(
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

        pub const Cat = Model(
            @This(),
            "cats",
            struct { id: i32, name: []const u8, paws: i32, created_at: i64, updated_at: i64 },
            .{},
        );

        pub const Family = Model(
            @This(),
            "families",
            struct { id: i32, name: []const u8 },
            .{},
        );
    };
    const query = Query(TestAdapter, Schema, .Human)
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
        \\SELECT "humans"."id", "humans"."family_id", "humans"."cat_id", "humans"."name", "cats"."id", "cats"."name", "cats"."paws", "cats"."created_at", "cats"."updated_at", "families"."id", "families"."name" FROM "humans" INNER JOIN "cats" ON "humans"."cat_id" = "cats"."id" INNER JOIN "families" ON "humans"."family_id" = "families"."id" WHERE ("humans"."name" = $1 OR "humans"."name" = $2 OR "cats"."name" = $3 AND NOT "families"."name" = $4) ORDER BY "humans"."id" ASC
    ,
        query.sql,
    );
    try std.testing.expect(query.isValid());
}

test "slice of []const u8 in whereclause" {
    const Schema = struct {
        pub const Human = Model(
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

    const query = Query(TestAdapter, Schema, .Human).where(.{ .name = array.items });
    try std.testing.expectEqualStrings(
        \\SELECT "humans"."name" FROM "humans" WHERE "humans"."name" = ANY ($1)
    , query.sql);
    try std.testing.expect(query.isValid());
}

test "slice of int in whereclause" {
    const Schema = struct {
        pub const Human = Model(
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

    const query = Query(TestAdapter, Schema, .Human).where(.{ .cats = array.items });
    try std.testing.expectEqualStrings(
        \\SELECT "humans"."cats" FROM "humans" WHERE "humans"."cats" = ANY ($1)
    , query.sql);
    try std.testing.expect(query.isValid());
}

test "slice of float in whereclause" {
    const Schema = struct {
        pub const Human = Model(
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

    const query = Query(TestAdapter, Schema, .Human).where(.{ .favorite_number = array.items });
    try std.testing.expectEqualStrings(
        \\SELECT "humans"."favorite_number" FROM "humans" WHERE "humans"."favorite_number" = ANY ($1)
    , query.sql);
    try std.testing.expect(query.isValid());
}

test "slice of bool in whereclause" {
    const Schema = struct {
        pub const Human = Model(
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

    const query = Query(TestAdapter, Schema, .Human).where(.{ .has_cats = array.items });
    try std.testing.expectEqualStrings(
        \\SELECT "humans"."has_cats" FROM "humans" WHERE "humans"."has_cats" = ANY ($1)
    , query.sql);
    try std.testing.expect(query.isValid());
}

test "null in whereclause" {
    const Schema = struct {
        pub const Human = Model(
            @This(),
            "humans",
            struct { name: []const u8 },
            .{},
        );
    };

    const query = Query(TestAdapter, Schema, .Human).where(.{ .{ .name = null }, .OR, .{ .name = "baz" } });
    try std.testing.expectEqualStrings(
        \\SELECT "humans"."name" FROM "humans" WHERE ("humans"."name" IS NULL OR "humans"."name" = $1)
    , query.sql);
    try std.testing.expect(query.isValid());
}

test "groupBy" {
    const Schema = struct {
        pub const Cat = Model(
            @This(),
            "cats",
            struct { name: []const u8 },
            .{},
        );
    };

    const query = Query(TestAdapter, Schema, .Cat).groupBy(.{.name});
    try std.testing.expectEqualStrings(
        \\SELECT FROM "cats" WHERE (1 = 1) GROUP BY "cats"."name"
    , query.sql);
    try std.testing.expect(query.isValid());
}

test "aggregate max()" {
    const Schema = struct {
        pub const Cat = Model(
            @This(),
            "cats",
            struct { name: []const u8, paws: usize },
            .{},
        );
    };

    const query = Query(TestAdapter, Schema, .Cat)
        .select(.{ .name, sql.max(.paws) })
        .groupBy(.{.name});
    try std.testing.expectEqualStrings(
        \\SELECT "cats"."name", MAX("cats"."paws") FROM "cats" WHERE (1 = 1) GROUP BY "cats"."name"
    , query.sql);
    try std.testing.expect(query.isValid());
}

test "aggregate min()" {
    const Schema = struct {
        pub const Cat = Model(
            @This(),
            "cats",
            struct { name: []const u8, paws: usize },
            .{},
        );
    };

    const query = Query(TestAdapter, Schema, .Cat)
        .select(.{ .name, sql.min(.paws) })
        .groupBy(.{.name});
    try std.testing.expectEqualStrings(
        \\SELECT "cats"."name", MIN("cats"."paws") FROM "cats" WHERE (1 = 1) GROUP BY "cats"."name"
    , query.sql);
    try std.testing.expect(query.isValid());
}

test "aggregate count()" {
    const Schema = struct {
        pub const Cat = Model(
            @This(),
            "cats",
            struct { name: []const u8, paws: usize },
            .{},
        );
    };

    const query = Query(TestAdapter, Schema, .Cat)
        .select(.{ .name, sql.count(.paws) })
        .groupBy(.{.name});
    try std.testing.expectEqualStrings(
        \\SELECT "cats"."name", COUNT("cats"."paws") FROM "cats" WHERE (1 = 1) GROUP BY "cats"."name"
    , query.sql);
    try std.testing.expect(query.isValid());
}

test "aggregate avg()" {
    const Schema = struct {
        pub const Cat = Model(
            @This(),
            "cats",
            struct { name: []const u8, paws: usize },
            .{},
        );
    };

    const query = Query(TestAdapter, Schema, .Cat)
        .select(.{ .name, sql.avg(.paws) })
        .groupBy(.{.name});
    try std.testing.expectEqualStrings(
        \\SELECT "cats"."name", AVG("cats"."paws") FROM "cats" WHERE (1 = 1) GROUP BY "cats"."name"
    , query.sql);
    try std.testing.expect(query.isValid());
}

test "aggregate sum()" {
    const Schema = struct {
        pub const Cat = Model(
            @This(),
            "cats",
            struct { name: []const u8, paws: usize },
            .{},
        );
    };

    const query = Query(TestAdapter, Schema, .Cat)
        .select(.{ .name, sql.sum(.paws) })
        .groupBy(.{.name});
    try std.testing.expectEqualStrings(
        \\SELECT "cats"."name", SUM("cats"."paws") FROM "cats" WHERE (1 = 1) GROUP BY "cats"."name"
    , query.sql);
    try std.testing.expect(query.isValid());
}

test "like/ilike" {
    const Schema = struct {
        pub const Cat = Model(
            @This(),
            "cats",
            struct { name: []const u8, paws: usize },
            .{},
        );
    };

    const query = Query(TestAdapter, Schema, .Cat)
        .select(.{.name})
        .where(.{ .{ .name, .like, "Herc%" }, .OR, .{ .name, .ilike, "princ%" } });
    try std.testing.expectEqualStrings(
        \\SELECT "cats"."name" FROM "cats" WHERE ("cats"."name" LIKE $1 OR "cats"."name" ILIKE $2)
    , query.sql);
    try std.testing.expect(query.isValid());
}

test "inner join" {
    const Schema = struct {
        pub const Human = Model(
            @This(),
            "humans",
            struct { id: i32, family_id: i32, cat_id: i32, name: []const u8 },
            .{
                .relations = .{
                    .cat = relation.belongsTo(.Cat, .{}),
                    .computers = relation.hasMany(.Computer, .{}),
                },
            },
        );

        pub const Cat = Model(
            @This(),
            "cats",
            struct { id: i32, name: []const u8, paws: i32, created_at: i64, updated_at: i64 },
            .{},
        );

        pub const Computer = Model(
            @This(),
            "computers",
            struct { id: i32, human_id: i32 },
            .{},
        );
    };

    const query = Query(TestAdapter, Schema, .Human)
        .join(.inner, .cat)
        .join(.inner, .computers)
        .select(.{.name});
    try std.testing.expectEqualStrings(
        \\SELECT "humans"."name" FROM "humans" INNER JOIN "cats" ON "humans"."cat_id" = "cats"."id" INNER JOIN "computers" ON "humans"."id" = "computers"."human_id" WHERE (1 = 1) ORDER BY "humans"."id" ASC
    , query.sql);
    try std.testing.expect(query.isValid());
}

test "outer join" {
    const Schema = struct {
        pub const Human = Model(
            @This(),
            "humans",
            struct { id: i32, family_id: i32, cat_id: i32, name: []const u8 },
            .{
                .relations = .{
                    .cat = relation.belongsTo(.Cat, .{}),
                    .computers = relation.hasMany(.Computer, .{}),
                },
            },
        );

        pub const Cat = Model(
            @This(),
            "cats",
            struct { id: i32, name: []const u8, paws: i32, created_at: i64, updated_at: i64 },
            .{},
        );

        pub const Computer = Model(
            @This(),
            "computers",
            struct { id: i32, human_id: i32 },
            .{},
        );
    };

    const query = Query(TestAdapter, Schema, .Human)
        .join(.outer, .cat)
        .join(.outer, .computers)
        .select(.{.name});
    try std.testing.expectEqualStrings(
        \\SELECT "humans"."name" FROM "humans" LEFT OUTER JOIN "cats" ON "humans"."cat_id" = "cats"."id" LEFT OUTER JOIN "computers" ON "humans"."id" = "computers"."human_id" WHERE (1 = 1) ORDER BY "humans"."id" ASC
    , query.sql);
    try std.testing.expect(query.isValid());
}

test "inner and outer join" {
    const Schema = struct {
        pub const Human = Model(
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

        pub const Cat = Model(
            @This(),
            "cats",
            struct { id: i32, name: []const u8, paws: i32, created_at: i64, updated_at: i64 },
            .{},
        );

        pub const Family = Model(
            @This(),
            "families",
            struct { id: i32, name: []const u8 },
            .{},
        );
    };

    const query = Query(TestAdapter, Schema, .Human)
        .join(.inner, .cat)
        .join(.outer, .family)
        .select(.{.name});
    try std.testing.expectEqualStrings(
        \\SELECT "humans"."name" FROM "humans" INNER JOIN "cats" ON "humans"."cat_id" = "cats"."id" LEFT OUTER JOIN "families" ON "humans"."family_id" = "families"."id" WHERE (1 = 1) ORDER BY "humans"."id" ASC
    , query.sql);
    try std.testing.expect(query.isValid());
}

test "inner and outer join with select on relation columns" {
    const Schema = struct {
        pub const Human = Model(
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

        pub const Cat = Model(
            @This(),
            "cats",
            struct { id: i32, name: []const u8, paws: i32, created_at: i64, updated_at: i64 },
            .{},
        );

        pub const Family = Model(
            @This(),
            "families",
            struct { id: i32, name: []const u8 },
            .{},
        );
    };

    const query = Query(TestAdapter, Schema, .Human)
        .join(.inner, .cat)
        .join(.outer, .family)
        .select(.{ .name, .{ .family = .{.id}, .cat = .{.paws} } });
    try std.testing.expectEqualStrings(
        \\SELECT "humans"."name", "families"."id", "cats"."paws" FROM "humans" INNER JOIN "cats" ON "humans"."cat_id" = "cats"."id" LEFT OUTER JOIN "families" ON "humans"."family_id" = "families"."id" WHERE (1 = 1) ORDER BY "humans"."id" ASC
    , query.sql);
    try std.testing.expect(query.isValid());
}

test "default order by (no order clauses, default primary key present)" {
    const Schema = struct {
        pub const Human = Model(
            @This(),
            "humans",
            struct { id: i32 },
            .{},
        );
    };
    const query = Query(TestAdapter, Schema, .Human).select(.{});
    try std.testing.expectEqualStrings(
        \\SELECT "humans"."id" FROM "humans" WHERE (1 = 1) ORDER BY "humans"."id" ASC
    , query.sql);
    try std.testing.expect(query.isValid());
}

test "default order by (no order clauses, default primary key not present)" {
    const Schema = struct {
        pub const Human = Model(
            @This(),
            "humans",
            struct { name: []const u8 },
            .{},
        );
    };
    const query = Query(TestAdapter, Schema, .Human).select(.{});
    try std.testing.expectEqualStrings(
        \\SELECT "humans"."name" FROM "humans" WHERE (1 = 1)
    , query.sql);
    try std.testing.expect(query.isValid());
}

test "default order by (no order clauses, custom primary key present)" {
    const Schema = struct {
        pub const Human = Model(
            @This(),
            "humans",
            struct { name: []const u8 },
            .{ .primary_key = "name" },
        );
    };
    const query = Query(TestAdapter, Schema, .Human).select(.{});
    try std.testing.expectEqualStrings(
        \\SELECT "humans"."name" FROM "humans" WHERE (1 = 1) ORDER BY "humans"."name" ASC
    , query.sql);
    try std.testing.expect(query.isValid());
}

test "default order by (no order clauses, custom primary key not present)" {
    const Schema = struct {
        pub const Human = Model(
            @This(),
            "humans",
            struct { id: i32 },
            .{ .primary_key = "name" },
        );
    };
    const query = Query(TestAdapter, Schema, .Human).select(.{});
    try std.testing.expectEqualStrings(
        \\SELECT "humans"."id" FROM "humans" WHERE (1 = 1)
    , query.sql);
    try std.testing.expect(query.isValid());
}

test "raw whereclause" {
    const Schema = struct {
        pub const Human = Model(
            @This(),
            "humans",
            struct { id: i32 },
            .{},
        );
    };
    const qux = std.crypto.random.int(u8);
    var buf: [4]u8 = undefined;
    const quux = try std.fmt.bufPrint(&buf, "{s}", .{"quux"});
    const query = Query(TestAdapter, Schema, .Human).where(.{
        "foo = ? and bar = ? or baz = ? and qux = ? and quux = ? and a = ? and b = ? and c = ? and d = ? and e = ? and f = ? and g = ? and h = ? and i = ? and j = ? and k = ?",
        .{ "qux", 100, false, qux, quux, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11 },
    });
    try std.testing.expectEqualStrings(
        \\SELECT "humans"."id" FROM "humans" WHERE foo = $1 and bar = $2 or baz = $3 and qux = $4 and quux = $5 and a = $6 and b = $7 and c = $8 and d = $9 and e = $10 and f = $11 and g = $12 and h = $13 and i = $14 and j = $15 and k = $16 ORDER BY "humans"."id" ASC
    , query.sql);
    try std.testing.expect(query.isValid());
}

test "raw select column" {
    const Schema = struct {
        pub const Human = Model(
            @This(),
            "humans",
            struct { id: i32 },
            .{},
        );
    };
    const query = Query(TestAdapter, Schema, .Human).select(.{
        sql.column(u32, "foo(bar + baz)").as(.foo),
        sql.column([]const u8, "qux(quux, corge)").as(.bar),
    });
    try std.testing.expectEqualStrings(
        \\SELECT foo(bar + baz), qux(quux, corge) FROM "humans" WHERE (1 = 1) ORDER BY "humans"."id" ASC
    , query.sql);
    try std.testing.expect(query.isValid());
}

test "complex whereclause" {
    const Schema = struct {
        pub const Cat = Model(
            @This(),
            "cats",
            struct {
                id: i32,
                name: []const u8,
                age: i32,
                favorite_sport: []const u8,
                status: []const u8,
            },
            .{ .relations = .{ .homes = hasMany(.Home, .{}) } },
        );

        pub const Home = Model(@This(), "homes", struct { id: i32, cat_id: i32, zip_code: []const u8 }, .{});
    };
    const query = Query(TestAdapter, Schema, .Cat).join(.inner, .homes).where(.{
        .{ .name = "Hercules" },                                                             .OR,                                               .{ .name = "Heracles" },
        .{ .{ .age, .gt, 4 }, .{ .age, .lt, 10 } },                                          .{ .favorite_sport, .like, "%ball" },              .{ .favorite_sport, .not_eql, "basketball" },
        .{ "my_sql_function(age)", .eql, 100 },                                              .{ .NOT, .{ .{ .age = 1 }, .OR, .{ .age = 2 } } }, .{ "age / paws = ? or age * paws < ?", .{ 2, 10 } },
        .{ .{ .status = null }, .OR, .{ .status = [_][]const u8{ "sleeping", "eating" } } }, .{ .homes = .{ .zip_code = "10304" } },
    });
    try std.testing.expect(query.isValid());
    try std.testing.expectEqualStrings(
        \\SELECT "cats"."id", "cats"."name", "cats"."age", "cats"."favorite_sport", "cats"."status" FROM "cats" INNER JOIN "homes" ON "cats"."id" = "homes"."cat_id" WHERE ("cats"."name" = $1 OR "cats"."name" = $2 AND ("cats"."age" > $3 AND "cats"."age" < $4) AND "cats"."favorite_sport" LIKE $5 AND "cats"."favorite_sport" <> $6 AND my_sql_function(age) = $7 AND ( NOT ("cats"."age" = $8 OR "cats"."age" = $9)) AND age / paws = $10 or age * paws < $11 AND ("cats"."status" IS NULL OR "cats"."status" = ANY ($12)) AND "homes"."zip_code" = $13) ORDER BY "cats"."id" ASC
    , query.sql);
}

test "boolean coercion https://github.com/jetzig-framework/jetquery/issues/1" {
    const Schema = struct {
        pub const Cat = Model(
            @This(),
            "cats",
            struct {
                a: bool,
                b: bool,
                c: bool,
                d: bool,
            },
            .{},
        );
    };
    const query = Query(TestAdapter, Schema, .Cat).insert(.{ .a = false, .b = 1, .c = true, .d = "1" });
    try std.testing.expect(query.isValid());
    try std.testing.expectEqualStrings(
        \\INSERT INTO "cats" ("a", "b", "c", "d") VALUES ($1, $2, $3, $4)
    , query.sql);
    try std.testing.expectEqual(query.values.@"0", false);
    try std.testing.expectEqual(query.values.@"1", true);
    try std.testing.expectEqual(query.values.@"2", true);
    try std.testing.expectEqual(query.values.@"3", true);
}

test "optionals" {
    const Schema = struct {
        pub const Thing = Model(
            @This(),
            "things",
            struct {
                a: ?bool,
                b: ?[]const u8,
                c: ?i32,
                d: ?i64,
                e: ?f32,
                f: ?f64,
                g: ?DateTime,
            },
            .{},
        );
    };
    const query1 = Query(TestAdapter, Schema, .Thing).insert(.{
        .a = @as(?bool, false),
        .b = @as(?[]const u8, "foo"),
        .c = @as(?i32, 100),
        .d = @as(?i64, 10000000000000),
        .e = @as(?f32, 100.1),
        .f = @as(?f64, 10000000000000.1),
        .g = @as(?DateTime, DateTime.now()),
    });
    try std.testing.expect(query1.isValid());
    try std.testing.expectEqualStrings(
        \\INSERT INTO "things" ("a", "b", "c", "d", "e", "f", "g") VALUES ($1, $2, $3, $4, $5, $6, $7)
    , query1.sql);

    const query2 = Query(TestAdapter, Schema, .Thing).where(.{
        .a = @as(?bool, false),
        .b = @as(?[]const u8, "foo"),
        .c = @as(?i32, 100),
        .d = @as(?i64, 10000000000000),
        .e = @as(?f32, 100.1),
        .f = @as(?f64, 10000000000000.1),
        .g = @as(?DateTime, DateTime.now()),
    });
    try std.testing.expect(query2.isValid());
    try std.testing.expectEqualStrings(
        \\SELECT "things"."a", "things"."b", "things"."c", "things"."d", "things"."e", "things"."f", "things"."g" FROM "things" WHERE ("things"."a" IS NOT DISTINCT FROM $1 AND "things"."b" IS NOT DISTINCT FROM $2 AND "things"."c" IS NOT DISTINCT FROM $3 AND "things"."d" IS NOT DISTINCT FROM $4 AND "things"."e" IS NOT DISTINCT FROM $5 AND "things"."f" IS NOT DISTINCT FROM $6 AND "things"."g" IS NOT DISTINCT FROM $7)
    , query2.sql);

    const query3 = Query(TestAdapter, Schema, .Thing).where(.{
        .a = @as(?bool, null),
        .b = @as(?[]const u8, null),
        .c = @as(?i32, null),
        .d = @as(?i64, null),
        .e = @as(?f32, null),
        .f = @as(?f64, null),
        .g = @as(?DateTime, null),
    });
    try std.testing.expect(query3.isValid());
    try std.testing.expectEqualStrings(
        \\SELECT "things"."a", "things"."b", "things"."c", "things"."d", "things"."e", "things"."f", "things"."g" FROM "things" WHERE ("things"."a" IS NOT DISTINCT FROM $1 AND "things"."b" IS NOT DISTINCT FROM $2 AND "things"."c" IS NOT DISTINCT FROM $3 AND "things"."d" IS NOT DISTINCT FROM $4 AND "things"."e" IS NOT DISTINCT FROM $5 AND "things"."f" IS NOT DISTINCT FROM $6 AND "things"."g" IS NOT DISTINCT FROM $7)
    , query3.sql);
}
