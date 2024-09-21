const std = @import("std");

pub const Repo = @import("jetquery/Repo.zig");
pub const adapters = @import("jetquery/adapters.zig");
pub const Migration = @import("jetquery/Migration.zig");
pub const table = @import("jetquery/table.zig");
pub const Row = @import("jetquery/Row.zig");
pub const Result = @import("jetquery/Result.zig").Result;
pub const DateTime = @import("jetquery/DateTime.zig");
pub const events = @import("jetquery/events.zig");
pub const Query = @import("jetquery/Query.zig").Query;
pub const Table = @import("jetquery/Table.zig").Table;
pub const Identifier = @import("jetquery/Identifier.zig");
pub const Column = @import("jetquery/Column.zig");
pub const Value = @import("jetquery/Value.zig").Value;

test {
    std.testing.refAllDecls(@This());
}

test "select" {
    const Schema = struct {
        pub const Cats = Table("cats", struct { name: []const u8, paws: usize }, .{});
    };
    const query = Query(Schema.Cats).select(&.{ .name, .paws });

    var buf: [1024]u8 = undefined;
    const sql = try query.toSql(&buf, adapters.test_adapter);
    try std.testing.expectEqualStrings(
        \\SELECT "name", "paws" FROM "cats"
    , sql);
}

test "select (all)" {
    const Schema = struct {
        pub const Cats = Table("cats", struct { name: []const u8, paws: usize }, .{});
    };
    const query = Query(Schema.Cats).select(&.{});

    var buf: [1024]u8 = undefined;
    const sql = try query.toSql(&buf, adapters.test_adapter);
    try std.testing.expectEqualStrings(
        \\SELECT "name", "paws" FROM "cats"
    , sql);
}

test "where" {
    const Schema = struct {
        pub const Cats = Table("cats", struct { name: []const u8, paws: usize }, .{});
    };

    const paws = 4;
    const query = Query(Schema.Cats)
        .select(&.{ .name, .paws })
        .where(.{ .name = "bar", .paws = paws });

    var buf: [1024]u8 = undefined;
    const sql = try query.toSql(&buf, adapters.test_adapter);
    try std.testing.expectEqualStrings(
        \\SELECT "name", "paws" FROM "cats" WHERE "name" = $1 AND "paws" = $2
    , sql);
}

test "where (multiple)" {
    const Schema = struct {
        pub const Cats = Table("cats", struct { name: []const u8, paws: usize }, .{});
    };

    const query = Query(Schema.Cats)
        .select(&.{ .name, .paws })
        .where(.{ .name = "bar" })
        .where(.{ .paws = 4 });

    var buf: [1024]u8 = undefined;
    const sql = try query.toSql(&buf, adapters.test_adapter);
    try std.testing.expectEqualStrings(
        \\SELECT "name", "paws" FROM "cats" WHERE "name" = $1 AND "paws" = $2
    , sql);
}

test "limit" {
    const Schema = struct {
        pub const Cats = Table("cats", struct { name: []const u8, paws: usize }, .{});
    };
    const query = Query(Schema.Cats)
        .select(&.{ .name, .paws })
        .limit(100);

    var buf: [1024]u8 = undefined;
    const sql = try query.toSql(&buf, adapters.test_adapter);
    try std.testing.expectEqualStrings(
        \\SELECT "name", "paws" FROM "cats" LIMIT 100
    , sql);
}

test "insert" {
    const Schema = struct {
        pub const Cats = Table("cats", struct { name: []const u8, paws: usize }, .{});
    };
    const query = Query(Schema.Cats)
        .insert(.{ .name = "Hercules", .paws = 4 });

    var buf: [1024]u8 = undefined;
    try std.testing.expectEqualStrings(
        \\INSERT INTO "cats" ("name", "paws") VALUES ($1, $2)
    ,
        try query.toSql(&buf, adapters.test_adapter),
    );
}

test "update" {
    const Schema = struct {
        pub const Cats = Table("cats", struct { name: []const u8, paws: usize }, .{});
    };
    const query = Query(Schema.Cats)
        .update(.{ .name = "Heracles", .paws = 2 })
        .where(.{ .name = "Hercules" });

    var buf: [1024]u8 = undefined;
    try std.testing.expectEqualStrings(
        \\UPDATE "cats" SET "name" = $1, "paws" = $2 WHERE "name" = $3
    ,
        try query.toSql(&buf, adapters.test_adapter),
    );
}

test "delete" {
    const Schema = struct {
        pub const Cats = Table("cats", struct { name: []const u8, paws: usize }, .{});
    };
    const query = Query(Schema.Cats)
        .delete()
        .where(.{ .name = "Hercules" });

    var buf: [1024]u8 = undefined;
    try std.testing.expectEqualStrings(
        \\DELETE FROM "cats" WHERE "name" = $1
    ,
        try query.toSql(&buf, adapters.test_adapter),
    );
}

test "delete (without where clause)" {
    const Schema = struct {
        pub const Cats = Table("cats", struct { name: []const u8, paws: usize }, .{});
    };
    const query = Query(Schema.Cats).delete();

    var buf: [1024]u8 = undefined;
    try std.testing.expectError(
        error.JetQueryUnsafeDelete,
        query.toSql(&buf, adapters.test_adapter),
    );
}

test "deleteAll" {
    const Schema = struct {
        pub const Cats = Table("cats", struct { name: []const u8, paws: usize }, .{});
    };
    const query = Query(Schema.Cats)
        .deleteAll();

    var buf: [1024]u8 = undefined;
    try std.testing.expectEqualStrings(
        \\DELETE FROM "cats"
    ,
        try query.toSql(&buf, adapters.test_adapter),
    );
}

test "find" {
    const Schema = struct {
        pub const Cats = Table("cats", struct { id: usize, name: []const u8, paws: usize }, .{});
    };
    const query = Query(Schema.Cats).find(1000);

    var buf: [1024]u8 = undefined;
    try std.testing.expectEqualStrings(
        \\SELECT "id", "name", "paws" FROM "cats" WHERE "id" = $1 LIMIT 1
    ,
        try query.toSql(&buf, adapters.test_adapter),
    );
}

test "find (with coerced id)" {
    const Schema = struct {
        pub const Cats = Table("cats", struct { id: usize, name: []const u8, paws: usize }, .{});
    };
    const query = Query(Schema.Cats).find("1000");

    var buf: [1024]u8 = undefined;
    try std.testing.expectEqualStrings(
        \\SELECT "id", "name", "paws" FROM "cats" WHERE "id" = $1 LIMIT 1
    ,
        try query.toSql(&buf, adapters.test_adapter),
    );
}

test "find (without id column)" {
    const Schema = struct {
        pub const Cats = Table("cats", struct { name: []const u8, paws: usize }, .{});
    };
    const query = Query(Schema.Cats).find(1000);

    var buf: [1024]u8 = undefined;
    try std.testing.expectError(
        error.JetQueryMissingIdField,
        query.toSql(&buf, adapters.test_adapter),
    );
}

test "findBy" {
    const Schema = struct {
        pub const Cats = Table("cats", struct { id: usize, name: []const u8, paws: usize }, .{});
    };
    const query = Query(Schema.Cats).findBy(.{ .name = "Hercules", .paws = 4 });

    var buf: [1024]u8 = undefined;
    try std.testing.expectEqualStrings(
        \\SELECT "id", "name", "paws" FROM "cats" WHERE "name" = $1 AND "paws" = $2 LIMIT 1
    ,
        try query.toSql(&buf, adapters.test_adapter),
    );
}

test "runtime field values" {
    const Schema = struct {
        pub const Cats = Table("cats", struct { name: []const u8, paws: usize }, .{});
    };
    var hercules_buf: [8]u8 = undefined;
    const hercules = try std.fmt.bufPrint(&hercules_buf, "{s}", .{"Hercules"});
    var heracles_buf: [8]u8 = undefined;
    const heracles = try std.fmt.bufPrint(&heracles_buf, "{s}", .{"Heracles"});
    const query = Query(Schema.Cats)
        .update(.{ .name = heracles, .paws = 2 })
        .where(.{ .name = hercules });
    const values = query.values();
    try std.testing.expectEqualStrings("Heracles", values.@"0");
    try std.testing.expectEqual(2, values.@"1");
    try std.testing.expectEqualStrings("Hercules", values.@"2");
}

test "boolean coercion" {
    const Schema = struct {
        pub const Cats = Table("cats", struct { name: []const u8, intelligent: bool }, .{});
    };
    const query = Query(Schema.Cats)
        .select(&.{.name})
        .where(.{ .intelligent = "1" });

    try std.testing.expectEqual(query.values().@"0", true);
}

test "integer coercion" {
    const Schema = struct {
        pub const Cats = Table("cats", struct { name: []const u8, paws: usize }, .{});
    };
    const query = Query(Schema.Cats)
        .select(&.{.name})
        .where(.{ .paws = "4" });

    try std.testing.expectEqual(query.values().@"0", 4);
}

test "float coercion" {
    const Schema = struct {
        pub const Cats = Table("cats", struct { name: []const u8, intelligence: f64 }, .{});
    };
    const query = Query(Schema.Cats)
        .select(&.{.name})
        .where(.{ .intelligence = "10.2" });

    try std.testing.expectEqual(query.values().@"0", 10.2);
}

test "toJetQuery()" {
    const Schema = struct {
        pub const Cats = Table("cats", struct { name: []const u8, paws: usize }, .{});
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
                usize => 4,
                else => @compileError("Cannot coerce to " ++ @typeName(T)),
            };
        }
    };

    const name = Name{};
    const paws = Paws{};

    const query = Query(Schema.Cats)
        .insert(.{ .name = name, .paws = &paws });
    var buf: [1024]u8 = undefined;
    try std.testing.expectEqualStrings(
        \\INSERT INTO "cats" ("name", "paws") VALUES ($1, $2)
    , try query.toSql(&buf, adapters.test_adapter));
    const values = query.values();
    try std.testing.expectEqualStrings(values.@"0", "Hercules");
    try std.testing.expectEqual(values.@"1", 4);
}

test "failed coercion (bool)" {
    const Schema = struct {
        pub const Cats = Table("cats", struct { name: []const u8, intelligent: bool }, .{});
    };
    const query = Query(Schema.Cats)
        .select(&.{.name})
        .where(.{ .intelligent = "not a bool" });

    try std.testing.expectError(error.JetQueryInvalidBooleanString, query.validateValues());
}

test "failed coercion (int)" {
    const Schema = struct {
        pub const Cats = Table("cats", struct { name: []const u8, paws: usize }, .{});
    };
    const query = Query(Schema.Cats)
        .select(&.{.name})
        .where(.{ .paws = "not an int" });

    try std.testing.expectError(error.JetQueryInvalidIntegerString, query.validateValues());
}

test "failed coercion (float)" {
    const Schema = struct {
        pub const Cats = Table("cats", struct { name: []const u8, intelligence: f64 }, .{});
    };
    const query = Query(Schema.Cats)
        .select(&.{.name})
        .where(.{ .intelligence = "not a float" });

    try std.testing.expectError(error.JetQueryInvalidFloatString, query.validateValues());
}

test "timestamps (create)" {
    // TODO
}

test "timestamps (update)" {
    // TODO
}
