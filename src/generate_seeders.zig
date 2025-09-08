const std = @import("std");
const ArrayList = std.ArrayList;
const GeneralPurposeAllocator = std.heap.GeneralPurposeAllocator;
const ArenaAllocator = std.heap.ArenaAllocator;
const Allocator = std.mem.Allocator;

pub fn main() !void {
    var gpa: GeneralPurposeAllocator(.{}) = .init;
    defer std.debug.assert(gpa.deinit() == .ok);
    const gpa_allocator = gpa.allocator();

    var arena: ArenaAllocator = .init(gpa_allocator);
    const allocator = arena.allocator();
    defer arena.deinit();

    const args = try std.process.argsAlloc(allocator);
    const seeders_module_path = args[1];
    const seeders: []const []const u8 = if (args.len > 2) args[2..] else &.{};
    const seeders_file = try std.fs.cwd().createFile(seeders_module_path, .{});
    var seeders_module_dir = try std.fs.cwd().openDir(std.fs.path.dirname(seeders_module_path).?, .{});
    defer seeders_module_dir.close();

    var writer = seeders_file.writer(.{});
    try writer.interface.writeAll(
        \\const jetquery = @import("jetquery");
        \\pub const Seeder = struct {
        \\    runFn: *const fn(repo: anytype) anyerror!void,
        \\    name: []const u8,
        \\};
        \\pub const seeders = [_]Seeder{
        \\
    );
    for (seeders) |seed| {
        const basename = std.fs.path.basename(seed);
        if (basename[0] == '.') continue;
        try std.fs.cwd().copyFile(seed, seeders_module_dir, basename, .{});
        try writer.interface.print(
            \\    .{{
            \\        .runFn = @import("{0s}").run,
            \\        .name = "{0s}",
            \\    }},
            \\
        ,
            .{try zigEscape(allocator, basename)},
        );
    }
    try seeders_file.writeAll(
        \\};
        \\
    );
    seeders_file.close();
}

fn zigEscape(allocator: Allocator, input: []const u8) ![]const u8 {
    var allocating: std.Io.Writer.Allocating = .init(allocator);
    defer allocating.deinit();

    try std.zig.stringEscape(input, &allocating.writer);
    return allocating.toOwnedSlice();
}
