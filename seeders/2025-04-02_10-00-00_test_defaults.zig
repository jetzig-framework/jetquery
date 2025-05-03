const std = @import("std");

// Testing of default column values of various types
pub fn run(repo: anytype) !void {
    try repo.insert(.DefaultsTest, .{});
}
