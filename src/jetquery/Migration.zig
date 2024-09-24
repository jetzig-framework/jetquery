const std = @import("std");

const jetcommon = @import("jetcommon");

allocator: std.mem.Allocator,
name: []const u8,
options: MigrationOptions,

const Migration = @This();

const MigrationOptions = struct {
    migrations_path: ?[]const u8 = null,
};

pub fn init(allocator: std.mem.Allocator, name: []const u8, options: MigrationOptions) Migration {
    return .{ .allocator = allocator, .name = name, .options = options };
}

pub fn save(self: Migration) !void {
    var dir = if (self.options.migrations_path) |path|
        try std.fs.openDirAbsolute(path, .{})
    else
        try std.fs.cwd().openDir("migrations", .{});
    defer dir.close();

    var buf: [19]u8 = undefined;
    const prefix = try timestamp(&buf);
    const filename = try std.mem.concat(self.allocator, u8, &.{ prefix, "_", self.name, ".zig" });
    const migration_file = try dir.createFile(filename, .{ .exclusive = true });
    defer migration_file.close();
    const writer = migration_file.writer();
    try writer.writeAll(
        \\const std = @import("std");
        \\const jetquery = @import("jetquery");
        \\const t = jetquery.table;
        \\
        \\pub fn up(repo: *jetquery.Repo) !void {
        \\    try repo.createTable(
        \\        "my_table",
        \\        &.{
        \\            t.primaryKey("id", .{}),
        \\            t.column("my_string", .string, .{}),
        \\            t.column("my_integer", .integer, .{}),
        \\            t.timestamps(.{}),
        \\        },
        \\        .{},
        \\    );
        \\}
        \\
        \\pub fn down(repo: *jetquery.Repo) !void {
        \\    try repo.dropTable("my_table", .{});
        \\}
        \\
    );
}

fn timestamp(buf: []u8) ![]const u8 {
    const datetime = jetcommon.types.DateTime.now();
    const date = datetime.date();
    const time = datetime.time();
    var stream = std.io.fixedBufferStream(buf);
    const writer = stream.writer();
    try writer.print(
        "{d:04}-{d:02}-{d:02}_{d:02}-{d:02}-{d:02}",
        .{ @as(u16, @intCast(date.year)), date.month, date.day, time.hour, time.min, time.sec },
    );
    return stream.getWritten();
}
