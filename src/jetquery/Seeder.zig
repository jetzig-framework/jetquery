const std = @import("std");
const ArrayListManaged = std.array_list.Managed;

const jetcommon = @import("jetcommon");

allocator: std.mem.Allocator,
name: []const u8,
options: SeederOptions,

const Seeder = @This();

const SeederOptions = struct {
    seeders_path: ?[]const u8 = null,
};

pub fn init(allocator: std.mem.Allocator, name: []const u8, options: SeederOptions) Seeder {
    return .{ .allocator = allocator, .name = name, .options = options };
}

const seeder_template =
    \\const std = @import("std");
    \\
    \\pub fn run(repo: anytype) !void {
    \\    // The `run` function runs when a seed is executed.
    \\    //
    \\    // This example seeder populates a table named `my_table` with the following columns:
    \\    //
    \\    // See https://www.jetzig.dev/documentation/sections/database/seeders for more details.
    \\    //
    \\    // Run `jetzig database migrate` to apply migrations and create the Schema.
    \\    //
    \\    // Then run `jetzig database seed` to execute all seeds in `src/app/database/seeders/`
    \\    //
    \\    try repo.insert(
    \\        .MyTable,
    \\        .{
    \\            .my_string = "value",
    \\            .my_integer = 100,
    \\        },
    \\    );
    \\}
    \\
;

pub fn save(self: Seeder) ![]const u8 {
    const content = try self.render();

    var dir = if (self.options.seeders_path) |path|
        try std.fs.openDirAbsolute(path, .{})
    else
        try std.fs.cwd().openDir("seeders", .{});
    defer dir.close();

    var timestamp_buf: [19]u8 = undefined;
    const prefix = try timestamp(&timestamp_buf);
    const filename = try std.mem.concat(self.allocator, u8, &.{ prefix, "_", self.name, ".zig" });
    const seeder_file = try dir.createFile(filename, .{ .exclusive = true });
    defer seeder_file.close();

    var write_buffer: [1024]u8 = undefined;
    var writer = seeder_file.writer(&write_buffer);
    try writer.interface.writeAll(content);
    const realpath = try dir.realpathAlloc(self.allocator, filename);

    return realpath;
}

pub fn render(self: Seeder) ![]const u8 {
    var arena = std.heap.ArenaAllocator.init(self.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    var buf = ArrayListManaged(u8).init(alloc);
    const writer = buf.writer();
    try writer.writeAll(seeder_template);

    return try jetcommon.fmt.zig(
        self.allocator,
        buf.items,
        "Found errors in generated migration.",
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
        // TODO: Fix jetcommon types to expose these directly
        .{ @as(u16, @intCast(date.zul_date.year)), date.zul_date.month, date.zul_date.day, time.zul_time.hour, time.zul_time.min, time.zul_time.sec },
    );
    return stream.getWritten();
}

inline fn FieldType(T: type, comptime name: []const u8) type {
    const tag = std.enums.nameCast(std.meta.FieldEnum(T), name);
    const F = std.meta.fieldInfo(T, tag);
    return switch (@typeInfo(F.type)) {
        .optional => |info| info.child,
        else => F,
    };
}

inline fn hasField(T: type, comptime name: []const u8, VT: type) bool {
    return @hasField(T, name) and FieldType(T, name) == VT;
}

inline fn hasEnum(T: type, comptime name: []const u8, VT: type) bool {
    if (VT != []const u8) return false;
    if (!@hasField(T, name)) return false;

    const FT = FieldType(T, name);

    return @typeInfo(FT) == .@"enum";
}

test "default seeder" {
    const seeder = Seeder.init(std.testing.allocator, "test_seeder", .{});

    const rendered = try seeder.render();
    defer std.testing.allocator.free(rendered);

    try std.testing.expectEqualStrings(seeder_template, rendered);
}

test "seeder from command line: generate seeder" {
    const seeder = Seeder.init(
        std.testing.allocator,
        "test_seeder",
        .{},
    );
    const rendered = try seeder.render();
    defer std.testing.allocator.free(rendered);

    try std.testing.expectEqualStrings(
        \\const std = @import("std");
        \\
        \\pub fn run(repo: anytype) !void {
        \\    // The `run` function runs when a seed is executed.
        \\    //
        \\    // This example seeder populates a table named `my_table` with the following columns:
        \\    //
        \\    // See https://www.jetzig.dev/documentation/sections/database/seeders for more details.
        \\    //
        \\    // Run `jetzig database migrate` to apply migrations and create the Schema.
        \\    //
        \\    // Then run `jetzig database seed` to execute all seeds in `src/app/database/seeders/`
        \\    //
        \\    try repo.insert(
        \\        .MyTable,
        \\        .{
        \\            .my_string = "value",
        \\            .my_integer = 100,
        \\        },
        \\    );
        \\}
        \\
    , rendered);
}
