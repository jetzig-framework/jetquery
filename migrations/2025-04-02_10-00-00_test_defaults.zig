const std = @import("std");
const jetquery = @import("jetquery");
const t = jetquery.schema.table;

// Testing of default column values of various types
pub fn up(repo: anytype) !void {
    try repo.createTable(
        "defaults_test",
        &.{
            t.primaryKey("id", .{}),
            t.column("name", .string, .{ .default = "'John Doe'" }),
            t.column("count", .integer, .{ .default = "42" }),
            t.column("active", .boolean, .{ .default = "true" }),
            t.column("description", .text, .{ .default = "'This is a default description with ''quotes'' and other special characters!'" }),
            t.column("score", .float, .{ .default = "3.14" }),
            t.column("no_default", .string, .{ .optional = true }), // Field WITHOUT default
            t.column("price", .decimal, .{ .default = "19.99" }),
            t.column("small_count", .smallint, .{ .default = "5" }),
            t.column("big_count", .bigint, .{ .default = "9223372036854775807" }),
            t.column("precise_value", .double_precision, .{ .default = "3.141592653589793" }),
            t.column("last_update", .datetime, .{ .default = "now()" }), // DateTime default using now()
            t.timestamps(.{}),
        },
        .{},
    );
}

pub fn down(repo: anytype) !void {
    try repo.dropTable("defaults_test", .{});
}
