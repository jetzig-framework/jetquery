const std = @import("std");

const jetquery = @import("../jetquery.zig");

/// An event triggered by executing a query. When creating a `Repo`, use the option
/// `eventCallback` to specify a function that will receive an `Event` for each query execution.
/// Otherwise, `defaultCallback` will be invoked instead.
pub const Event = struct {
    // TODO: Make this a union for failed/successful queries etc.
    const Error = struct {
        message: []const u8,
        err: anyerror,
    };

    context: jetquery.Context = .query,
    level: enum { DEBUG, INFO, WARN, ERROR } = .INFO,
    message: ?[]const u8 = null,
    sql: ?[]const u8 = null,
    status: enum { success, fail } = .success,
    err: ?Error = null,
    caller_info: ?jetquery.debug.CallerInfo = null,
    duration: ?i64 = null,
};

pub fn defaultCallback(event: Event) !void {
    if (event.caller_info) |info| {
        if (event.context == .query) {
            const allocator = info.debug_info.allocator;

            const cwd = try std.fs.cwd().realpathAlloc(allocator, ".");
            defer allocator.free(cwd);

            const relative = try std.fs.path.relative(allocator, cwd, info.file_name);
            defer allocator.free(relative);

            std.debug.print("[{s}:{}] ", .{ relative, info.line_number });
        }
    }

    if (event.err) |err| {
        std.debug.print(
            \\
            \\/
            \\| Query:
            \\|   {s}
            \\| Error:
            \\|   {s}: {s}
            \\\
            \\
        , .{ event.sql orelse "", @errorName(err.err), err.message });
    } else {
        var buf: [32]u8 = undefined;
        const formatted_duration = if (event.duration) |duration| {
            var duration_buf: [32]u8 = undefined;
            var writer: std.Io.Writer = .fixed(&duration_buf);
            try writer.printDurationSigned(duration);
            try std.fmt.bufPrint(&buf, " [{s}]", .{duration_buf});
        } else "";
        std.debug.print("{s}{s}{s}{s}{s}", .{
            event.message orelse "",
            if (event.message) |_| "\n" else "",
            event.sql orelse "",
            formatted_duration,
            if (event.sql) |_| "\n" else "",
        });
    }
}
