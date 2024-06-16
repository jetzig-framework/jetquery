const std = @import("std");
const jetquery = @import("jetquery");
const t = jetquery.schema.table;

pub fn up(repo: anytype) !void {
    try repo.createTable(
        "humans",
        &.{
            t.primaryKey("id", .{}),
            t.column("name", .string, .{ .not_null = true, .unique = true }),
            t.timestamps(.{}),
        },
        .{},
    );
}

pub fn down(repo: anytype) !void {
    try repo.dropTable("humans", .{});
}
