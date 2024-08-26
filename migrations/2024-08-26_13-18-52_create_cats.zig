const std = @import("std");
const jetquery = @import("jetquery");
const columns = jetquery.columns;

pub fn up(repo: *jetquery.Repo) !void {
    try repo.createTable(
        "cats",
        &.{
            columns.primaryKey("id", .{}),
            columns.column("name", .string, .{}),
            columns.column("paws", .integer, .{}),
            columns.timestamps(.{}),
        },
    );
}

pub fn down(repo: *jetquery.Repo) !void {
    _ = repo;
}
