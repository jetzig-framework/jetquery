const std = @import("std");
const jetquery = @import("jetquery");

// Testing of default column values of various types
pub fn run(repo: anytype) !void {
    try repo.insert(.DefaultsTest, .{});
}
