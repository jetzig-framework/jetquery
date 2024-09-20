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

test "incompatible query type" {
    // TODO
    // const Schema = struct {
    //     pub const Cats = Table("cats", struct { name: []const u8, paws: usize }, .{});
    // };
    // const query = Query(Schema.Cats)
    //     .update(.{ .name = "Heracles", .paws = 2 })
    //     .select(&.{.name})
    //     .where(.{ .name = "Hercules" });
    //
    // var buf: [1024]u8 = undefined;
    // try std.testing.expectError(
    //     error.JetQueryIncompatibleQueryType,
    //     query.toSql(&buf, adapters.test_adapter),
    // );
}

test "boolean coercion" {
    // TODO
    const Schema = struct {
        pub const Cats = Table("cats", struct { name: []const u8, intelligent: bool }, .{});
    };
    const query = Query(Schema.Cats)
        .select(&.{.name})
        .where(.{ .intelligent = "1" });

    var buf: [1024]u8 = undefined;
    try std.testing.expectEqualStrings(
        \\SELECT "name" FROM "cats" WHERE "intelligent" = $1
    ,
        try query.toSql(&buf, adapters.test_adapter),
    );
}

test "integer coercion" {
    // TODO
    const Schema = struct {
        pub const Cats = Table("cats", struct { name: []const u8, paws: usize }, .{});
    };
    const query = Query(Schema.Cats)
        .select(&.{.name})
        .where(.{ .paws = "4" });

    var buf: [1024]u8 = undefined;
    try std.testing.expectEqualStrings(
        \\SELECT "name" FROM "cats" WHERE "paws" = $1
    ,
        try query.toSql(&buf, adapters.test_adapter),
    );
}

test "float coercion" {
    // TODO
    const Schema = struct {
        pub const Cats = Table("cats", struct { name: []const u8, intelligence: f64 }, .{});
    };
    const query = Query(Schema.Cats)
        .select(&.{.name})
        .where(.{ .intelligence = "10.2" });

    var buf: [1024]u8 = undefined;
    try std.testing.expectEqualStrings(
        \\SELECT "name" FROM "cats" WHERE "intelligence" = $1
    ,
        try query.toSql(&buf, adapters.test_adapter),
    );
}

test "toJetQuery()" {
    // TODO
    // const Schema = struct {
    //     pub const Cats = Table("cats", struct { name: []const u8, paws: usize }, .{});
    // };
    //
    // const Name = struct {
    //     pub fn toJetQuery(self: @This(), T: type, allocator: std.mem.Allocator) T {
    //         _ = self;
    //         _ = allocator;
    //         return switch (T) {
    //             []const u8 => "Hercules",
    //             else => @compileError("Cannot coerce to " ++ @typeName(T)),
    //         };
    //     }
    // };
    //
    // const Paws = struct {
    //     pub fn toJetQuery(self: @This(), T: type, allocator: std.mem.Allocator) T {
    //         _ = self;
    //         _ = allocator;
    //         return switch (T) {
    //             usize => 4,
    //             else => @compileError("Cannot coerce to " ++ @typeName(T)),
    //         };
    //     }
    // };
    //
    // const name = Name{};
    // const paws = Paws{};
    //
    // const query = Query(Schema.Cats)
    //     .insert(.{ .name = name, .paws = paws });
    // var buf: [1024]u8 = undefined;
    // try std.testing.expectEqualStrings(
    //     \\insert into "cats" ("name", "paws") values ('Hercules', 4)
    // , try query.toSql(&buf, adapters.test_adapter));
}

test "timestamps (create)" {
    // TODO
}

test "timestamps (update)" {
    // TODO
}
