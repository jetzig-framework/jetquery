const std = @import("std");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer std.debug.assert(gpa.deinit() == .ok);
    const gpa_allocator = gpa.allocator();

    var arena = std.heap.ArenaAllocator.init(gpa_allocator);
    const allocator = arena.allocator();
    defer arena.deinit();

    const args = try std.process.argsAlloc(allocator);
    const migrations_module_path = args[1];
    const migrations_path = args[2];

    const migrations = try findMigrations(allocator, migrations_path);
    const migrations_file = try std.fs.createFileAbsolute(migrations_module_path, .{});
    const writer = migrations_file.writer();
    try writer.writeAll(
        \\const jetquery = @import("jetquery");
        \\pub const Migration = struct {
        \\  migrationFn: *const fn(jetquery.Repo) anyerror!void,
        \\};
        \\pub const migrations = []const Migration{
        \\
    );
    for (migrations) |migration| {
        try writer.print(
            \\.{{ .migrationFn = @import("{s}").up }},
            \\
        ,
            .{try zigEscape(allocator, migration)},
        );
    }
    try writer.writeAll(
        \\};
        \\
    );
    migrations_file.close();
}

fn findMigrations(allocator: std.mem.Allocator, path: []const u8) ![][]const u8 {
    const absolute_path = if (std.fs.path.isAbsolute(path))
        path
    else
        std.fs.cwd().realpathAlloc(allocator, path) catch |err| {
            switch (err) {
                error.FileNotFound => return &.{},
                else => return err,
            }
        };

    var dir = std.fs.openDirAbsolute(absolute_path, .{ .iterate = true }) catch |err| {
        switch (err) {
            error.FileNotFound => return &.{},
            else => return err,
        }
    };
    defer dir.close();

    var migrations = std.ArrayList([]const u8).init(allocator);

    var it = dir.iterate();
    while (try it.next()) |entry| {
        if (entry.kind != .file) continue;
        try migrations.append(try allocator.dupe(u8, entry.name));
    }

    return try migrations.toOwnedSlice();
}

fn zigEscape(allocator: std.mem.Allocator, input: []const u8) ![]const u8 {
    var buf = std.ArrayList(u8).init(allocator);
    const writer = buf.writer();
    try std.zig.stringEscape(input, "", .{}, writer);
    return try buf.toOwnedSlice();
}
