const std = @import("std");

pub const Event = struct {
    const Error = struct {
        message: []const u8,
    };

    context: enum { query, migration } = .query,
    level: enum { DEBUG, INFO, WARN, ERROR } = .INFO,
    message: ?[]const u8 = null,
    sql: ?[]const u8 = null,
    status: enum { success, fail } = .success,
    err: ?Error = null,
};

pub fn defaultCallback(event: Event) !void {
    if (event.err) |err| {
        std.debug.print("Error:\n  {s}\nQuery:  {s}\n", .{ err.message, event.sql orelse "" });
    } else {
        std.debug.print("{s}{s}{s}{s}{s}", .{
            event.message orelse "",
            if (event.message) |_| "\n" else "",
            if (event.sql) |_| "Executing:\n  " else "",
            event.sql orelse "",
            if (event.sql) |_| "\n" else "",
        });
    }
}
