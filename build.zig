const std = @import("std");

pub fn build(b: *std.Build) !void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const lib = b.addStaticLibrary(.{
        .name = "jetquery",
        .root_source_file = b.path("src/jetquery.zig"),
        .target = target,
        .optimize = optimize,
    });

    b.installArtifact(lib);

    const pg_module = b.dependency("pg", .{ .target = target, .optimize = optimize });
    const zul_module = b.dependency("zul", .{ .target = target, .optimize = optimize });

    lib.root_module.addImport("pg", pg_module.module("pg"));
    lib.root_module.addImport("zul", zul_module.module("zul"));

    const jetquery_module = b.addModule("jetquery", .{ .root_source_file = b.path("src/jetquery.zig") });
    jetquery_module.addImport("pg", pg_module.module("pg"));
    jetquery_module.addImport("zul", zul_module.module("zul"));

    const migrations_path = b.option([]const u8, "jetquery_migrations_path", "Migrations path") orelse
        "migrations";
    const lib_unit_tests = b.addTest(.{
        .root_source_file = b.path("src/jetquery.zig"),
        .target = target,
        .optimize = optimize,
    });
    lib_unit_tests.root_module.addImport("pg", pg_module.module("pg"));
    lib_unit_tests.root_module.addImport("zul", zul_module.module("zul"));

    const run_lib_unit_tests = b.addRunArtifact(lib_unit_tests);

    const test_step = b.step("test", "Run unit tests");
    test_step.dependOn(&run_lib_unit_tests.step);

    const exe_generate_migrations = b.addExecutable(.{
        .name = "migrations",
        .root_source_file = b.path("src/generate_migrations.zig"),
        .target = target,
        .optimize = optimize,
    });

    const migration_unit_tests = b.addTest(.{
        .root_source_file = b.path("src/jetquery/Migrate.zig"),
        .target = target,
        .optimize = optimize,
    });
    const run_migration_unit_tests = b.addRunArtifact(migration_unit_tests);
    migration_unit_tests.step.dependOn(&exe_generate_migrations.step);
    test_step.dependOn(&run_migration_unit_tests.step);

    const run_generate_migrations_cmd = b.addRunArtifact(exe_generate_migrations);
    const generated_migrations_path = run_generate_migrations_cmd.addOutputFileArg("migrations.zig");

    for (try findMigrations(b.allocator, migrations_path)) |path| {
        run_generate_migrations_cmd.addFileArg(.{ .cwd_relative = path });
    }

    const migrations_module = b.createModule(.{ .root_source_file = generated_migrations_path });
    migrations_module.addImport("jetquery", jetquery_module);
    migration_unit_tests.root_module.addImport("migrations", migrations_module);
    migration_unit_tests.root_module.addImport("jetquery", jetquery_module);

    const jetquery_migrate_module = b.addModule(
        "jetquery_migrate",
        .{ .root_source_file = b.path("src/jetquery/Migrate.zig") },
    );
    jetquery_migrate_module.addImport("jetquery", jetquery_module);
    jetquery_migrate_module.addImport("migrations", migrations_module);
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
        try migrations.append(try std.fs.path.join(allocator, &.{ absolute_path, entry.name }));
    }

    return try migrations.toOwnedSlice();
}
