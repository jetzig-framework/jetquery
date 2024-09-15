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
    const query = Query(Schema.Cats).init(std.testing.allocator)
        .select(&.{ .name, .paws });
    defer query.deinit();

    var buf: [1024]u8 = undefined;
    const sql = try query.toSql(&buf, adapters.test_adapter);
    try std.testing.expectEqualStrings(
        \\select "name", "paws" from "cats"
    , sql);
}

test "where" {
    const Schema = struct {
        pub const Cats = Table("cats", struct { name: []const u8, paws: usize }, .{});
    };

    const paws = 4;
    const query = Query(Schema.Cats).init(std.testing.allocator)
        .select(&.{ .name, .paws })
        .where(.{ .name = "bar", .paws = paws });
    defer query.deinit();

    var buf: [1024]u8 = undefined;
    const sql = try query.toSql(&buf, adapters.test_adapter);
    try std.testing.expectEqualStrings(
        \\select "name", "paws" from "cats" where "name" = 'bar' and "paws" = 4
    , sql);
    try std.testing.expectEqualStrings(query.where_nodes[0].name, "name");
    try std.testing.expectEqualStrings(query.where_nodes[0].value.string, "bar");
    try std.testing.expectEqualStrings(query.where_nodes[1].name, "paws");
    try std.testing.expect(query.where_nodes[1].value == .integer);
}

test "limit" {
    const Schema = struct {
        pub const Cats = Table("cats", struct { name: []const u8, paws: usize }, .{});
    };
    const query = Query(Schema.Cats).init(std.testing.allocator)
        .select(&.{ .name, .paws })
        .limit(100);
    defer query.deinit();

    var buf: [1024]u8 = undefined;
    const sql = try query.toSql(&buf, adapters.test_adapter);
    try std.testing.expectEqualStrings(
        \\select "name", "paws" from "cats" limit 100
    , sql);
}

test "insert" {
    const Schema = struct {
        pub const Cats = Table("cats", struct { name: []const u8, paws: usize }, .{});
    };
    const query = Query(Schema.Cats).init(std.testing.allocator)
        .insert(.{ .name = "Hercules", .paws = 4 });
    defer query.deinit();

    var buf: [1024]u8 = undefined;
    try std.testing.expectEqualStrings(
        \\insert into "cats" ("name", "paws") values ('Hercules', 4)
    ,
        try query.toSql(&buf, adapters.test_adapter),
    );
}

test "update" {
    const Schema = struct {
        pub const Cats = Table("cats", struct { name: []const u8, paws: usize }, .{});
    };
    const query = Query(Schema.Cats).init(std.testing.allocator)
        .update(.{ .name = "Heracles", .paws = 2 })
        .where(.{ .name = "Hercules" });

    defer query.deinit();
    var buf: [1024]u8 = undefined;
    try std.testing.expectEqualStrings(
        \\update "cats" set "name" = 'Heracles', "paws" = 2 where "name" = 'Hercules'
    ,
        try query.toSql(&buf, adapters.test_adapter),
    );
}

test "delete" {
    const Schema = struct {
        pub const Cats = Table("cats", struct { name: []const u8, paws: usize }, .{});
    };
    const query = Query(Schema.Cats).init(std.testing.allocator)
        .delete()
        .where(.{ .name = "Hercules" });
    defer query.deinit();

    var buf: [1024]u8 = undefined;
    try std.testing.expectEqualStrings(
        \\delete from "cats" where "name" = 'Hercules'
    ,
        try query.toSql(&buf, adapters.test_adapter),
    );
}

test "incompatible query type" {
    const Schema = struct {
        pub const Cats = Table("cats", struct { name: []const u8, paws: usize }, .{});
    };
    const query = Query(Schema.Cats).init(std.testing.allocator)
        .update(.{ .name = "Heracles", .paws = 2 })
        .select(&.{.name})
        .where(.{ .name = "Hercules" });

    defer query.deinit();
    var buf: [1024]u8 = undefined;
    try std.testing.expectError(
        error.JetQueryIncompatibleQueryType,
        query.toSql(&buf, adapters.test_adapter),
    );
}

test "boolean coercion" {
    const Schema = struct {
        pub const Cats = Table("cats", struct { name: []const u8, intelligent: bool }, .{});
    };
    const query = Query(Schema.Cats).init(std.testing.allocator)
        .select(&.{.name})
        .where(.{ .intelligent = "1" });

    defer query.deinit();
    var buf: [1024]u8 = undefined;
    try std.testing.expectEqualStrings(
        \\select "name" from "cats" where "intelligent" = 1
    ,
        try query.toSql(&buf, adapters.test_adapter),
    );
}

test "integer coercion" {
    const Schema = struct {
        pub const Cats = Table("cats", struct { name: []const u8, paws: usize }, .{});
    };
    const query = Query(Schema.Cats).init(std.testing.allocator)
        .select(&.{.name})
        .where(.{ .paws = "4" });

    defer query.deinit();
    var buf: [1024]u8 = undefined;
    try std.testing.expectEqualStrings(
        \\select "name" from "cats" where "paws" = 4
    ,
        try query.toSql(&buf, adapters.test_adapter),
    );
}

test "float coercion" {
    const Schema = struct {
        pub const Cats = Table("cats", struct { name: []const u8, intelligence: f64 }, .{});
    };
    const query = Query(Schema.Cats).init(std.testing.allocator)
        .select(&.{.name})
        .where(.{ .intelligence = "10.2" });

    defer query.deinit();
    var buf: [1024]u8 = undefined;
    try std.testing.expectEqualStrings(
        \\select "name" from "cats" where "intelligence" = 10.2
    ,
        try query.toSql(&buf, adapters.test_adapter),
    );
}

test "failed coercion" {
    const Schema = struct {
        pub const Cats = Table("cats", struct { name: []const u8, paws: usize }, .{});
    };
    const query = Query(Schema.Cats).init(std.testing.allocator)
        .select(&.{.name})
        .where(.{ .paws = "notanumber" });

    defer query.deinit();
    var buf: [1024]u8 = undefined;
    try std.testing.expectError(
        error.InvalidCharacter,
        query.toSql(&buf, adapters.test_adapter),
    );
}

test "toJetQuery()" {
    const Schema = struct {
        pub const Cats = Table("cats", struct { name: []const u8, paws: usize }, .{});
    };

    const Name = struct {
        pub fn toJetQuery(self: @This(), T: type, allocator: std.mem.Allocator) T {
            _ = self;
            _ = allocator;
            return switch (T) {
                []const u8 => "Hercules",
                else => @compileError("Cannot coerce to " ++ @typeName(T)),
            };
        }
    };

    const Paws = struct {
        pub fn toJetQuery(self: @This(), T: type, allocator: std.mem.Allocator) T {
            _ = self;
            _ = allocator;
            return switch (T) {
                usize => 4,
                else => @compileError("Cannot coerce to " ++ @typeName(T)),
            };
        }
    };

    const name = Name{};
    const paws = Paws{};

    const query = Query(Schema.Cats).init(std.testing.allocator)
        .insert(.{ .name = name, .paws = paws });
    defer query.deinit();
    var buf: [1024]u8 = undefined;
    try std.testing.expectEqualStrings(
        \\insert into "cats" ("name", "paws") values ('Hercules', 4)
    , try query.toSql(&buf, adapters.test_adapter));
}

test "timestamps (create)" {
    // TODO
}

test "timestamps (update)" {
    // TODO
}
