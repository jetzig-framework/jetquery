const std = @import("std");
const jetquery = @import("jetquery");
const t = jetquery.schema.table;

pub fn up(repo: anytype) !void {
    try repo.createTable(
        "cats",
        &.{
            t.primaryKey("id", .{}),
            t.column("name", .string, .{ .not_null = true, .unique = true }),
            t.column("paws", .integer, .{ .index = true }),
            t.column("human_id", .integer, .{ .reference = .{ "humans", "id" } }),
            t.timestamps(.{}),
        },
        .{},
    );

    try repo.createIndex("cats", &.{ "name", "paws" }, .{});
}

pub fn down(repo: anytype) !void {
    try repo.dropTable("cats", .{});
}
