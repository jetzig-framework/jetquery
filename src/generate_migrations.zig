const std = @import("std");
const ArrayList = std.ArrayList;
const Allocator = std.mem.Allocator;
const ArenaAllocator = std.heap.ArenaAllocator;
const GeneralPurposeAllocator = std.heap.GeneralPurposeAllocator;

pub fn main() !void {
    var gpa: GeneralPurposeAllocator(.{}) = .init;
    defer std.debug.assert(gpa.deinit() == .ok);
    const gpa_allocator = gpa.allocator();

    var arena: ArenaAllocator = .init(gpa_allocator);
    const allocator = arena.allocator();
    defer arena.deinit();

    const args = try std.process.argsAlloc(allocator);
    const migrations_module_path = args[1];
    const migrations: []const []const u8 = if (args.len > 2) args[2..] else &.{};
    const migrations_file = try std.fs.cwd().createFile(migrations_module_path, .{});
    defer migrations_file.close();
    var migrations_module_dir = try std.fs.cwd().openDir(std.fs.path.dirname(migrations_module_path).?, .{});
    defer migrations_module_dir.close();

    var writer = migrations_file.writer(.{});
    try writer.interface.writeAll(
        \\const jetquery = @import("jetquery");
        \\pub const Migration = struct {
        \\    upFn: *const fn(repo: anytype) anyerror!void,
        \\    downFn: *const fn(repo: anytype) anyerror!void,
        \\    version: []const u8,
        \\    name: []const u8,
        \\};
        \\pub const migrations = [_]Migration{
        \\
    );
    for (migrations) |migration| {
        const basename = std.fs.path.basename(migration);
        if (basename[0] == '.') continue;
        const version = basename[0.."2000-01-01_12-00-00".len];
        try std.fs.cwd().copyFile(migration, migrations_module_dir, basename, .{});
        try writer.interface.print(
            \\    .{{
            \\        .upFn = @import("{0s}").up,
            \\        .downFn = @import("{0s}").down,
            \\        .version = "{1s}",
            \\        .name = "{0s}",
            \\    }},
            \\
        ,
            .{ try zigEscape(allocator, basename), try zigEscape(allocator, version) },
        );
    }
    try writer.interface.writeAll(
        \\};
        \\
    );
}

fn zigEscape(allocator: std.mem.Allocator, input: []const u8) ![]const u8 {
    var allocating: std.Io.Writer.Allocating = .init(allocator);
    defer allocating.deinit();

    try std.zig.stringEscape(input, &allocating.writer);
    return allocating.toOwnedSlice();
}
