const std = @import("std");

/// Very basic noun plural->singular conversion. This is used for translating database tables
/// into model names. If the user modifies the model name in the generated schema then any
/// subsequent schema re-generations will use the value defined by the user. For this reason we
/// do not attempt to do anything particularly clever (or deal with other languages, etc.).
pub fn singularize(allocator: std.mem.Allocator, input: []const u8) ![]const u8 {
    const es_endings = [_][]const u8{ "oes", "ses", "xes", "zes", "ches", "shes" };

    for (es_endings) |ending| {
        if (std.mem.endsWith(u8, input, ending)) {
            return try allocator.dupe(u8, input[0 .. input.len - 2]);
        }
    }

    if (std.mem.endsWith(u8, input, "ies")) {
        const base = input[0 .. input.len - 3];
        return try std.mem.concat(allocator, u8, &.{ base, "y" });
    }

    if (std.mem.endsWith(u8, input, "s")) {
        return try allocator.dupe(u8, input[0 .. input.len - 1]);
    }

    return try allocator.dupe(u8, input);
}

test "-s" {
    const singular = try singularize(std.testing.allocator, "cats");
    defer std.testing.allocator.free(singular);
    try std.testing.expectEqualStrings("cat", singular);
}
test "-ies" {
    const singular = try singularize(std.testing.allocator, "festivities");
    defer std.testing.allocator.free(singular);
    try std.testing.expectEqualStrings("festivity", singular);
}
test "-shes" {
    const singular = try singularize(std.testing.allocator, "bushes");
    defer std.testing.allocator.free(singular);
    try std.testing.expectEqualStrings("bush", singular);
}

pub fn zigEscape(
    allocator: std.mem.Allocator,
    comptime context: enum { id, string },
    input: []const u8,
) ![]const u8 {
    var buf = std.ArrayList(u8).init(allocator);
    const writer = buf.writer();
    const formatter = switch (context) {
        .id => std.zig.fmtId(input),
        .string => std.zig.fmtEscapes(input),
    };

    try writer.print("{}", .{formatter});
    return try buf.toOwnedSlice();
}
