const std = @import("std");
const jetquery = @import("jetquery");
const t = jetquery.table;

pub fn up(repo: *jetquery.Repo) !void {
    try repo.createTable(
        "cats",
        &.{
            t.primaryKey("id", .{}),
            t.column("name", .string, .{}),
            t.column("paws", .integer, .{}),
            t.timestamps(.{}),
        },
        .{},
    );
}

pub fn down(repo: *jetquery.Repo) !void {
    _ = repo;
}
