const std = @import("std");

const jetquery = @import("../jetquery.zig");

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
    caller_info: ?jetquery.debug.CallerInfo = null,
};

pub fn defaultCallback(event: Event) !void {
    if (event.caller_info) |info| {
        const allocator = info.debug_info.allocator;

        const cwd = try std.fs.cwd().realpathAlloc(allocator, ".");
        defer allocator.free(cwd);

        const relative = try std.fs.path.relative(allocator, cwd, info.file_name);
        defer allocator.free(relative);

        std.debug.print("[{s}:{}] ", .{ relative, info.line_number });
    }

    if (event.err) |err| {
        std.debug.print(
            \\/
            \\| Query:
            \\|   {s}
            \\| Error:
            \\|   {s}
            \\\
            \\
        , .{ event.sql orelse "", err.message });
    } else {
        std.debug.print("{s}{s}{s}{s}", .{
            event.message orelse "",
            if (event.message) |_| "\n" else "",
            event.sql orelse "",
            if (event.sql) |_| "\n" else "",
        });
    }
}
