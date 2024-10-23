const std = @import("std");
const jetquery = @import("jetquery");
const t = jetquery.table;

pub fn up(repo: *jetquery.Repo) !void {
    try repo.createTable(
        "cats",
        &.{
            t.primaryKey("id", .{}),
            t.column("name", .string, .{ .not_null = true, .unique = true }),
            t.column("paws", .integer, .{ .index = true }),
            t.timestamps(.{}),
        },
        .{},
    );

    try repo.createIndex("cats", &.{ "name", "paws" }, .{});

    try repo.createTable(
        "humans",
        &.{
            t.primaryKey("id", .{}),
            t.column("name", .string, .{ .not_null = true, .unique = true }),
            t.column("cat_id", .integer, .{ .reference = .{ "cats", "id" } }),
            t.timestamps(.{}),
        },
        .{},
    );
}

pub fn down(repo: *jetquery.Repo) !void {
    _ = repo;
}
